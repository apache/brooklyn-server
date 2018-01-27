/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.testng.Assert.assertFalse;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;

import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;

/**
 * Testing utility that registers an appender to watch a given logback logger, and records events 
 * that match a given predicate.
 * 
 * Callers should first call {@link #start()}, and must call {@link #close()} to de-register the
 * appender (doing this in a finally block).
 */
@Beta
public class LogWatcher implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LogWatcher.class);

    public static class EventPredicates {
        public static Predicate<ILoggingEvent> containsMessage(final String expected) {
            return containsMessages(expected);
        }

        protected static Predicate<ILoggingEvent> matchPredicate(BiPredicate<String, String> pred,
                                                              final String... expecteds) {
            return event -> {
                if (event == null) return false;
                String msg = event.getFormattedMessage();
                if (msg == null) return false;
                for (String expected : expecteds) {
                    if (!pred.test(msg.trim(), expected)) return false;
                }
                return true;
            };
        }

        public static Predicate<ILoggingEvent> matchingRegexes(final String... patterns) {
            return matchPredicate(String::matches, patterns);
        }

        public static Predicate<ILoggingEvent> containsMessages(final String... expecteds) {
            return matchPredicate(String::contains, expecteds);
        }

        public static Predicate<ILoggingEvent> containsExceptionStackLine(final Class<?> clazz, final String methodName) {
            return event -> {
                IThrowableProxy throwable = (event != null) ? event.getThrowableProxy() : null;
                if (throwable != null) {
                    for (StackTraceElementProxy line : throwable.getStackTraceElementProxyArray()) {
                        if (line.getStackTraceElement().getClassName().contains(clazz.getSimpleName())
                                && line.getStackTraceElement().getMethodName().contains(methodName)) {
                            return true;
                        }
                    }
                }
                return false;
            };
        }

        public static Predicate<ILoggingEvent> containsException() {
            return new Predicate<ILoggingEvent>() {
                @Override public boolean apply(ILoggingEvent input) {
                    return (input != null) && (input.getThrowableProxy() != null);
                }
            };
        }

        public static Predicate<ILoggingEvent> containsExceptionMessage(final String expected) {
            return containsExceptionMessages(expected);
        }

        public static Predicate<ILoggingEvent> containsExceptionMessages(final String... expecteds) {
            return new Predicate<ILoggingEvent>() {
                @Override public boolean apply(ILoggingEvent input) {
                    IThrowableProxy throwable = (input != null) ? input.getThrowableProxy() : null;
                    String msg = (throwable != null) ? throwable.getMessage() : null;
                    if (msg == null) return false;
                    for (String expected : expecteds) {
                        if (!msg.contains(expected)) return false;
                    }
                    return true;
                }
            };
        }
        public static Predicate<ILoggingEvent> containsExceptionClassname(final String expected) {
            return new Predicate<ILoggingEvent>() {
                @Override public boolean apply(ILoggingEvent input) {
                    IThrowableProxy throwable = (input != null) ? input.getThrowableProxy() : null;
                    String classname = (throwable != null) ? throwable.getClassName() : null;
                    if (classname == null) return false;
                    return classname.contains(expected);
                }
            };
        }
        public static Predicate<ILoggingEvent> levelGeaterOrEqual(final Level expectedLevel) {
            return new Predicate<ILoggingEvent>() {
                @Override public boolean apply(ILoggingEvent input) {
                    if (input == null) return false;
                    Level level = input.getLevel();
                    return level.isGreaterOrEqual(expectedLevel);
                }
            };
        }
    }
    
    private final List<ILoggingEvent> events = Collections.synchronizedList(Lists.<ILoggingEvent>newLinkedList());
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ch.qos.logback.classic.Level loggerLevel;
    private final ConsoleAppender<ILoggingEvent> appender;
    private final List<ch.qos.logback.classic.Logger> watchedLoggers = Lists.newArrayList();
    private volatile Map<ch.qos.logback.classic.Logger, Level> origLevels = Maps.newLinkedHashMap();

    public LogWatcher(String loggerName, ch.qos.logback.classic.Level loggerLevel, final Predicate<? super ILoggingEvent> filter) {
        this(ImmutableList.of(checkNotNull(loggerName, "loggerName")), loggerLevel, filter);
    }
    
    @SuppressWarnings("unchecked")
    public LogWatcher(Iterable<String> loggerNames, ch.qos.logback.classic.Level loggerLevel, final Predicate<? super ILoggingEvent> filter) {

        this.loggerLevel = checkNotNull(loggerLevel, "loggerLevel");
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        this.appender = new ConsoleAppender<>();

        PatternLayoutEncoder ple = new PatternLayoutEncoder() {
            @Override
            public void doEncode(ILoggingEvent event) throws IOException {
                final String txt = layout.doLayout(event);

                // Jump through hoops to turn the input event (without any layout)
                // into one processed by the pattern layout, prior to applying the filter.
                // Wrap the input event in a dynamic proxy to lay out the message/toString methods
                // but delegate all other methods to the real input event
                ILoggingEvent formatted = (ILoggingEvent) Proxy.newProxyInstance(
                    ILoggingEvent.class.getClassLoader(),
                    new Class<?>[]{ILoggingEvent.class},
                    (proxy, method, args) -> {
                        if (method.getName().endsWith("Message") || method.getName().equals("toString")) {
                            return txt;
                        } else {
                            return method.invoke(event, args);
                        }
                    });

                // now we can do the filter on the text that will be written to the log output
                if (event != null && filter.apply(formatted)) {
                    events.add(formatted);
                }
                LOG.trace("level="+event.getLevel()+"; event="+event+"; msg="+event.getFormattedMessage());

                super.doEncode(event);
            }
        };

        // The code below makes the assumption that the (test) logger configuration has a console appender
        // for root, with a pattern layout encoder, and re-uses its encoder pattern.
        // This is (at time of writing) as defined in logback-appender-stdout.xml.
        final Appender<ILoggingEvent> appender = lc.getLogger("ROOT").getAppender("STDOUT");
        final ConsoleAppender stdout = ConsoleAppender.class.cast(appender);
        final PatternLayoutEncoder stdoutEncoder = PatternLayoutEncoder.class.cast(stdout.getEncoder());
        ple.setPattern(stdoutEncoder.getPattern());
        ple.setContext(lc);
        ple.start();

        this.appender.setContext(lc);
        this.appender.setEncoder(ple);
        this.appender.start();

        for (String loggerName : loggerNames) {
            final ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(checkNotNull(loggerName, "loggerName"));
            logger.addAppender(this.appender);
            logger.setLevel(this.loggerLevel);
            logger.setAdditive(false);
            watchedLoggers.add(logger);
        }
    }
    
    public void start() {

        checkState(!closed.get(), "Cannot start LogWatcher after closed");
        for (ch.qos.logback.classic.Logger watchedLogger : watchedLoggers) {
            origLevels.put(watchedLogger, watchedLogger.getLevel());
            watchedLogger.setLevel(loggerLevel);
            watchedLogger.addAppender(appender);
        }
    }
    
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (watchedLoggers != null) {
                for (ch.qos.logback.classic.Logger watchedLogger : watchedLoggers) {
                    Level origLevel = origLevels.get(watchedLogger);
                    if (origLevel != null) watchedLogger.setLevel(origLevel);
                    watchedLogger.detachAppender(appender);
                }
            }
            watchedLoggers.clear();
            origLevels.clear();
        }
    }
    
    public void assertHasEvent() {
        assertFalse(events.isEmpty());
    }

    public List<ILoggingEvent> assertHasEvent(final Predicate<? super ILoggingEvent> filter) {
        synchronized (events) {
            Iterable<ILoggingEvent> filtered = Iterables.filter(events, filter);
            assertFalse(Iterables.isEmpty(filtered), "events="+events);
            return ImmutableList.copyOf(filtered);
        }
    }

    public List<ILoggingEvent> assertHasEventEventually() {
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertFalse(events.isEmpty());
            }});
        return getEvents();
    }

    public List<ILoggingEvent> assertHasEventEventually(final Predicate<? super ILoggingEvent> filter) {
        final AtomicReference<List<ILoggingEvent>> result = new AtomicReference<>();
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                result.set(assertHasEvent(filter));
            }});
        return result.get();
    }

    public List<ILoggingEvent> getEvents() {
        synchronized (events) {
            return ImmutableList.copyOf(events);
        }
    }

    public List<ILoggingEvent> getEvents(Predicate<? super ILoggingEvent> filter) {
        synchronized (events) {
            return ImmutableList.copyOf(Iterables.filter(events, filter));
        }
    }

    public void printEvents() {
        printEvents(System.out, getEvents());
    }
    public String printEventsToString() {
        return printEventsToString(getEvents());
    }

    public String printEventsToString(Iterable<? extends ILoggingEvent> events) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        printEvents(new PrintStream(baos), events);
        return new String(baos.toByteArray());
    }
    public void printEvents(PrintStream stream, Iterable<? extends ILoggingEvent> events) {
        for (ILoggingEvent event : events) {
            stream.println(Time.makeDateString(event.getTimeStamp()) + ": " + event.getThreadName()
                    + ": " + event.getLevel() + ": " + event.getMessage());
            IThrowableProxy throwable = event.getThrowableProxy();
            if (throwable != null) {
                stream.println("\t" + throwable.getMessage());
                if (throwable.getStackTraceElementProxyArray() != null) {
                    for (StackTraceElementProxy element : throwable.getStackTraceElementProxyArray()) {
                        stream.println("\t\t" + "at " + element);
                    }
                }
            }
        }
    }

    public void clearEvents() {
        synchronized (events) {
            events.clear();
        }
    }
}
