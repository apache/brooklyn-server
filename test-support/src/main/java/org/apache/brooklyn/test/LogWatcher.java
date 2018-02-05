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

import java.io.Closeable;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.util.time.Time;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import ch.qos.logback.core.Appender;

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
    
        public static Predicate<ILoggingEvent> containsMessages(final String... expecteds) {
            return new Predicate<ILoggingEvent>() {
                @Override public boolean apply(ILoggingEvent input) {
                    if (input == null) return false;
                    String msg = input.getFormattedMessage();
                    if (msg == null) return false;
                    for (String expected : expecteds) {
                        if (!msg.contains(expected)) return false;
                    }
                    return true;
                }
            };
        }
        
        public static Predicate<ILoggingEvent> containsExceptionStackLine(final Class<?> clazz, final String methodName) {
            return new Predicate<ILoggingEvent>() {
                @Override public boolean apply(ILoggingEvent input) {
                    IThrowableProxy throwable = (input != null) ? input.getThrowableProxy() : null;
                    if (throwable != null) {
                        for (StackTraceElementProxy line : throwable.getStackTraceElementProxyArray()) {
                            if (line.getStackTraceElement().getClassName().contains(clazz.getSimpleName())
                                    && line.getStackTraceElement().getMethodName().contains(methodName)) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
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
    private final Appender<ILoggingEvent> appender;
    private final List<ch.qos.logback.classic.Logger> watchedLoggers = Lists.newArrayList();
    private volatile Map<ch.qos.logback.classic.Logger, Level> origLevels = Maps.newLinkedHashMap();

    public LogWatcher(String loggerName, ch.qos.logback.classic.Level loggerLevel, final Predicate<? super ILoggingEvent> filter) {
        this(ImmutableList.of(checkNotNull(loggerName, "loggerName")), loggerLevel, filter);
    }
    
    @SuppressWarnings("unchecked")
    public LogWatcher(Iterable<String> loggerNames, ch.qos.logback.classic.Level loggerLevel, final Predicate<? super ILoggingEvent> filter) {
        for (String loggerName : loggerNames) {
            Logger logger = LoggerFactory.getLogger(checkNotNull(loggerName, "loggerName"));
            watchedLoggers.add((ch.qos.logback.classic.Logger) logger);
        }
        this.loggerLevel = checkNotNull(loggerLevel, "loggerLevel");
        this.appender = Mockito.mock(Appender.class);
        
        Mockito.when(appender.getName()).thenReturn("MOCK");
        Answer<Void> answer = new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                ILoggingEvent event = invocation.getArgument(0);
                if (event != null && filter.apply(event)) {
                    events.add(event);
                }
                LOG.trace("level="+event.getLevel()+"; event="+event+"; msg="+event.getFormattedMessage());
                return null;
            }
        };
        Mockito.doAnswer(answer).when(appender).doAppend(Mockito.<ILoggingEvent>any());
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
