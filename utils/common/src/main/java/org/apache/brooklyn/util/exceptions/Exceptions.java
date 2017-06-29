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
package org.apache.brooklyn.util.exceptions;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.instanceOf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class Exceptions {

    /** {@link Throwable} types whose existence is unhelpful in a <b>message</b>. */
    private static final List<Class<? extends Throwable>> ALWAYS_BORING_MESSAGE_THROWABLE_SUPERTYPES = ImmutableList.<Class<? extends Throwable>>of(
        ExecutionException.class, InvocationTargetException.class, UndeclaredThrowableException.class);
    /** As {@link #ALWAYS_BORING_MESSAGE_THROWABLE_SUPERTYPES} but might carry an interesting message. */
    private static final List<Class<? extends Throwable>> BORING_IF_NO_MESSAGE_THROWABLE_SUPERTYPES = ImmutableList.<Class<? extends Throwable>>of(
        PropagatedRuntimeException.class);

    public static final int MAX_COLLAPSE_RECURSIVE_DEPTH = 100;
    
    /** NB: might be useful for stack trace, e.g. {@link ExecutionException} */
    private static boolean isBoringForMessage(Throwable t) {
        for (Class<? extends Throwable> type: ALWAYS_BORING_MESSAGE_THROWABLE_SUPERTYPES)
            if (type.isInstance(t)) return true;
        if (Strings.isBlank(t.getMessage())) {
            for (Class<? extends Throwable> type: BORING_IF_NO_MESSAGE_THROWABLE_SUPERTYPES)
                if (type.isInstance(t)) return true;
        }
        return false;
    }

    private static final Predicate<Throwable> IS_THROWABLE_BORING_FOR_MESSAGE = new Predicate<Throwable>() {
        @Override
        public boolean apply(Throwable input) {
            return isBoringForMessage(input);
        }
    };

    private static List<Class<? extends Throwable>> BORING_PREFIX_THROWABLE_EXACT_TYPES = ImmutableList.<Class<? extends Throwable>>of(
        RuntimeException.class, Exception.class, Throwable.class,
        IllegalStateException.class, IllegalArgumentException.class);
    
    private static List<Class<? extends Throwable>> BORING_PREFIX_THROWABLE_SUPERTYPES = ImmutableList.<Class<? extends Throwable>>of(
        ClassCastException.class, CompoundRuntimeException.class, PropagatedRuntimeException.class);

    /** Returns whether the prefix is throwable either known to be boring or to have an unhelpful type name (prefix)
     * which should be suppressed in <b>messages</b>. (They may be important in stack traces.)
     * <p>
     * null is accepted but treated as not boring. */
    public static boolean isPrefixBoring(Throwable t) {
        if (t==null) return false;
        if (isBoringForMessage(t))
            return true;
        if (t instanceof UserFacingException) return true;
        for (Class<? extends Throwable> type: BORING_PREFIX_THROWABLE_EXACT_TYPES)
            if (t.getClass().equals(type)) return true;
        for (Class<? extends Throwable> type: BORING_PREFIX_THROWABLE_SUPERTYPES)
            if (type.isInstance(t)) return true;
        return false;
    }

    /** @deprecated never used, but kept as it might be useful */
    @Deprecated
    static String stripBoringPrefixes(String s) {
        ArrayList<String> prefixes = Lists.newArrayListWithCapacity(2 + BORING_PREFIX_THROWABLE_EXACT_TYPES.size() * 3);
        for (Class<? extends Throwable> type : BORING_PREFIX_THROWABLE_EXACT_TYPES) {
            prefixes.add(type.getCanonicalName());
            prefixes.add(type.getName());
            prefixes.add(type.getSimpleName());
        }
        prefixes.add(":");
        prefixes.add(" ");
        String[] ps = prefixes.toArray(new String[prefixes.size()]);
        return Strings.removeAllFromStart(s, ps);
    }

    /**
     * Propagate a {@link Throwable} as a {@link RuntimeException}.
     * <p>
     * Like Guava {@link Throwables#propagate(Throwable)} but:
     * <li> throws {@link RuntimeInterruptedException} to handle {@link InterruptedException}s; and
     * <li> wraps as PropagatedRuntimeException for easier filtering
     */
    public static RuntimeException propagate(Throwable throwable) {
        if (throwable instanceof InterruptedException) {
            throw new RuntimeInterruptedException((InterruptedException) throwable);
        } else if (throwable instanceof RuntimeInterruptedException) {
            Thread.currentThread().interrupt();
            throw (RuntimeInterruptedException) throwable;
        }
        Throwables.propagateIfPossible(checkNotNull(throwable));
        throw new PropagatedRuntimeException(throwable);
    }

    /**
     * See {@link #propagate(Throwable)}.
     * <p>
     * The given message is included <b>only</b> if the given {@link Throwable}
     * needs to be wrapped; otherwise the message is not used.
     * To always include the message, use {@link #propagateAnnotated(String, Throwable)}.
     */
    public static RuntimeException propagate(String msg, Throwable throwable) {
        return propagate(msg, throwable, false);
    }

    /** As {@link #propagate(String, Throwable)} but always re-wraps including the given message. */
    public static RuntimeException propagateAnnotated(String msg, Throwable throwable) {
        return propagate(msg, throwable, true);
    }

    private static RuntimeException propagate(String msg, Throwable throwable, boolean alwaysAnnotate) {
        if (throwable instanceof InterruptedException) {
            throw new RuntimeInterruptedException(msg, (InterruptedException) throwable);
        } else if (throwable instanceof RuntimeInterruptedException) {
            Thread.currentThread().interrupt();
            if (alwaysAnnotate) {
                throw new RuntimeInterruptedException(msg, (RuntimeInterruptedException) throwable);
            } else {
                throw (RuntimeInterruptedException) throwable;
            }
        }
        if (throwable==null) {
            throw new PropagatedRuntimeException(msg, new NullPointerException("No throwable supplied."));
        }
        if (!alwaysAnnotate) {
            Throwables.propagateIfPossible(checkNotNull(throwable));
        }
        throw new PropagatedRuntimeException(msg, throwable);
    }

    /** 
     * Propagate exceptions which are interrupts (be it {@link InterruptedException}
     * or {@link RuntimeInterruptedException}.
     */
    public static void propagateIfInterrupt(Throwable throwable) {
        if (throwable instanceof InterruptedException) {
            throw new RuntimeInterruptedException((InterruptedException) throwable);
        } else if (throwable instanceof RuntimeInterruptedException) {
            Thread.currentThread().interrupt();
            throw (RuntimeInterruptedException) throwable;
        }
    }

    /** 
     * Propagate exceptions which are fatal.
     * <p>
     * Propagates only those exceptions which one rarely (if ever) wants to capture,
     * such as {@link InterruptedException} and {@link Error}s.
     */
    public static void propagateIfFatal(Throwable throwable) {
        propagateIfInterrupt(throwable);
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
    }

    /** 
     * Indicates whether this exception is "fatal" - i.e. in normal programming, should not be 
     * caught but should instead be propagating so the call-stack fails. For example, an interrupt
     * should cause the task to abort rather than catching and ignoring (or "handling" incorrectly).
     */
    public static boolean isFatal(Throwable throwable) {
        return (throwable instanceof InterruptedException)
                || (throwable instanceof RuntimeInterruptedException) 
                || (throwable instanceof Error);
    }

    public static Predicate<Throwable> isFatalPredicate() {
        return IsFatalPredicate.INSTANCE;
    }

    private static class IsFatalPredicate implements Predicate<Throwable> {
        private static final IsFatalPredicate INSTANCE = new IsFatalPredicate();
        
        @Override
        public boolean apply(Throwable input) {
            return input != null && isFatal(input);
        }
    }
    
    /** returns the first exception of the given type, or null */
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> T getFirstThrowableOfType(Throwable from, Class<T> clazz) {
        return (T) Iterables.tryFind(getCausalChain(from), instanceOf(clazz)).orNull();
    }

    /** returns the first exception that matches the filter, or null */
    public static Throwable getFirstThrowableMatching(Throwable from, Predicate<? super Throwable> filter) {
        return Iterables.tryFind(getCausalChain(from), filter).orNull();
    }

    /** returns the first exception in the call chain which whose message is potentially interesting,
     * in the sense that it is has some chance of giving helpful information as the cause.
     * <p>
     * more specifically this drops those which typically wrap such causes giving chain / thread info,
     * reporting rather than causal explanation or important context -- 
     * ie excluding {@link ExecutionException} always,
     * and {@link PropagatedRuntimeException} if it has no message,
     * and similar such.
     * <p>
     * if all are "uninteresting" in this sense (which should not normally be the case) 
     * this method just returns the original. 
     * <p>
     * often looking for a {@link UserFacingException} eg using {@link #getFirstThrowableOfType(Throwable, Class)}
     * is a better way to give a user-facing message.
     */
    public static Throwable getFirstInteresting(Throwable throwable) {
        return Iterables.tryFind(getCausalChain(throwable), Predicates.not(IS_THROWABLE_BORING_FOR_MESSAGE)).or(throwable);
    }

    /** creates (but does not throw) a new {@link PropagatedRuntimeException} whose 
     * message and cause are taken from the first _interesting_ element in the source */
    public static Throwable collapse(Throwable source) {
        return collapse(source, true);
    }
    
    /** as {@link #collapse(Throwable)} but includes causal messages in the message as per {@link #collapseTextIncludingAllCausalMessages(Throwable)};
     * use with care (limit once) as repeated usage can result in multiple copies of the same message */ 
    public static Throwable collapseIncludingAllCausalMessages(Throwable source) {
        return collapse(source, true, true, ImmutableSet.<Throwable>of(), new Object[0]);
    }
    
    /** creates (but does not throw) a new {@link PropagatedRuntimeException} whose 
     * message is taken from the first _interesting_ element in the source,
     * and optionally also the causal chain */
    public static Throwable collapse(Throwable source, boolean collapseCausalChain) {
        return collapse(source, collapseCausalChain, false, ImmutableSet.<Throwable>of(), new Object[0]);
    }

    /** As {@link Throwables#getCausalChain(Throwable)} but safe in the face of perverse classes which return themselves as their cause or otherwise have a recursive causal chain. */
    public static List<Throwable> getCausalChain(Throwable t) {
        Set<Throwable> result = MutableSet.of();
        while (t!=null) {
            if (!result.add(t)) break;
            t = t.getCause();
        }
        return ImmutableList.copyOf(result);
    }
    
    private static boolean isCausalChainDepthExceeding(Throwable t, int size) {
        if (size<0) return true;
        if (t==null) return false;
        return isCausalChainDepthExceeding(t.getCause(), size-1);
    }
    
    private static Throwable collapse(Throwable source, boolean collapseCausalChain, boolean includeAllCausalMessages, Set<Throwable> visited, Object contexts[]) {
        if (visited.isEmpty()) {
            if (isCausalChainDepthExceeding(source, MAX_COLLAPSE_RECURSIVE_DEPTH)) {
                // do fast check above, then do deeper check which survives recursive causes
                List<Throwable> chain = getCausalChain(source);
                if (chain.size() > MAX_COLLAPSE_RECURSIVE_DEPTH) {
                    // if it's an OOME or other huge stack, shrink it so we don't spin huge cycles processing the trace and printing it
                    // (sometimes generating subsequent OOME's in logback that mask the first!)
                    // coarse heuristic for how to reduce it, but that's better than killing cpu, causing further errors, and suppressing the root cause altogether!
                    String msg = chain.get(0).getMessage();
                    if (msg.length() > 512) msg = msg.substring(0, 500)+"...";
                    return new PropagatedRuntimeException("Huge stack trace (size "+chain.size()+", removing all but last few), "
                            + "starting: "+chain.get(0).getClass().getName()+": "+msg+"; ultimately caused by: ", 
                            chain.get(chain.size() - 10));
                }
            }
        }

        visited = MutableSet.copyOf(visited);
        String message = "";
        Throwable collapsed = source;
        int collapseCount = 0;
        boolean messageIsFinal = false;
        // remove boring exceptions at the head; if message is interesting append it
        while ((isBoringForMessage(collapsed) || isSkippableInContext(collapsed, contexts)) && !messageIsFinal) {
            collapseCount++;
            Throwable cause = collapsed.getCause();
            if (cause==null) {
                // everything in the tree is boring...
                return source;
            }
            if (!visited.add(collapsed)) {
                // there is a recursive loop
                break;
            }
            String collapsedS = collapsed.getMessage();
            if (collapsed instanceof PropagatedRuntimeException && ((PropagatedRuntimeException)collapsed).isCauseEmbeddedInMessage()) {
                message = collapsedS;
                messageIsFinal = true;
            }
            collapsed = cause;
        }
        // if no messages so far (ie we will be the toString) then remove boring prefixes from the message
        Throwable messagesCause = collapsed;
        while (messagesCause!=null && isPrefixBoring(messagesCause) && Strings.isBlank(message)) {
            collapseCount++;
            if (Strings.isNonBlank(messagesCause.getMessage())) {
                message = messagesCause.getMessage();
                messagesCause = messagesCause.getCause();
                break;
            }
            visited.add(messagesCause); messagesCause = messagesCause.getCause();
        }
        
        if (collapseCount==0 && !includeAllCausalMessages)
            return source;
        
        if (collapseCount==0 && messagesCause!=null) {
            message = getMessageWithAppropriatePrefix(messagesCause);
            messagesCause = messagesCause.getCause();
        }
        
        if (messagesCause!=null && !messageIsFinal) {
            String extraMessage = collapseText(messagesCause, includeAllCausalMessages, ImmutableSet.copyOf(visited), contexts);
            message = appendSeparator(message, extraMessage);
        }
        if (message==null) message = "";
        return new PropagatedRuntimeException(message, collapseCausalChain ? collapsed : source, Strings.isNonBlank(message));
    }
    
    /** True if the given exception is skippable in any of the supplied contexts. */
    public static boolean isSkippableInContext(Throwable e, Object... contexts) {
        if (!(e instanceof CanSkipInContext)) return false;
        for (Object c: contexts) { 
            if (((CanSkipInContext)e).canSkipInContext(c)) {
                return true;
            }
        }
        return false;
    }

    static String appendSeparator(String message, String next) {
        if (Strings.isBlank(message))
            return next;
        if (Strings.isBlank(next))
            return message;
        if (message.endsWith(next))
            return message;
        if (message.trim().endsWith(":") || message.trim().endsWith(";"))
            return message.trim()+" "+next;
        return message + ": " + next;
    }

    /** removes uninteresting items from the top of the call stack (but keeps interesting messages), and throws 
     * @deprecated since 0.7.0 same as {@link #propagate(Throwable)} */
    @Deprecated
    public static RuntimeException propagateCollapsed(Throwable source) {
        throw propagate(source);
    }

    /** like {@link #collapse(Throwable)} but returning a one-line message suitable for logging without traces */
    public static String collapseText(Throwable t) {
        return collapseText(t, false);
    }
    
    /** as {@link #collapseText(Throwable)} but skipping any throwables which implement {@link CanSkipInContext}
     * and indicate they should be skipped any of the given contexts */
    public static String collapseTextInContext(Throwable t, Object ...contexts) {
        return collapseText(t, false, ImmutableSet.<Throwable>of(), contexts);
    }

    /** normally {@link #collapseText(Throwable)} will stop following causal chains when encountering an interesting exception
     * with a message; this variant will continue to follow such causal chains, showing all messages. 
     * for use e.g. when verbose is desired in the single-line message. */
    public static String collapseTextIncludingAllCausalMessages(Throwable t) {
        return collapseText(t, true);
    }
    
    private static String collapseText(Throwable t, boolean includeAllCausalMessages) {
        return collapseText(t, includeAllCausalMessages, ImmutableSet.<Throwable>of(), new Object[0]);
    }
    private static String collapseText(Throwable t, boolean includeAllCausalMessages, Set<Throwable> visited, Object contexts[]) {
        if (t == null) return null;
        if (visited.contains(t)) {
            // If a boring-prefix class has no message it will render as multiply-visited.
            // Additionally IllegalStateException sometimes refers to itself as its cause.
            // In both cases, don't stack overflow!
            if (Strings.isNonBlank(t.getMessage())) return t.getMessage();
            if (t.getCause()!=null) return t.getCause().getClass().getName();
            return t.getClass().getName();
        }
        Throwable t2 = collapse(t, true, includeAllCausalMessages, visited, contexts);
        visited = MutableSet.copyOf(visited);
        visited.add(t);
        visited.add(t2);
        if (t2 instanceof PropagatedRuntimeException) {
            if (((PropagatedRuntimeException)t2).isCauseEmbeddedInMessage())
                // normally
                return t2.getMessage();
            else if (t2.getCause()!=null)
                return collapseText(t2.getCause(), includeAllCausalMessages, ImmutableSet.copyOf(visited), contexts);
            return ""+t2.getClass();
        }
        String result = getMessageWithAppropriatePrefix(t2);
        if (!includeAllCausalMessages) {
            return result;
        }
        Throwable cause = t2.getCause();
        if (cause != null) {
            String causeResult = collapseTextInContext(new PropagatedRuntimeException(cause), contexts);
            if (result.indexOf(causeResult)>=0)
                return result;
            return result + "; caused by "+causeResult;
        }
        return result;
    }

    public static class CollapseTextSupplier implements Supplier<String> {
        final Throwable cause;
        public CollapseTextSupplier(Throwable cause) { this.cause = cause; }
        @Override
        public String get() {
            return Exceptions.collapseText(cause);
        }
        public Throwable getOriginal() { return cause; }
    }

    public static RuntimeException propagate(Iterable<? extends Throwable> exceptions) {
        throw propagate(create(exceptions));
    }
    public static RuntimeException propagate(String prefix, Iterable<? extends Throwable> exceptions) {
        throw propagate(create(prefix, exceptions));
    }

    /** creates the given exception, but without propagating it, for use when caller will be wrapping */
    public static Throwable create(Iterable<? extends Throwable> exceptions) {
        return create(null, exceptions);
    }
    /** creates the given exception, but without propagating it, for use when caller will be wrapping */
    public static RuntimeException create(@Nullable String prefix, Iterable<? extends Throwable> exceptions) {
        if (Iterables.size(exceptions)==1) {
            Throwable e = exceptions.iterator().next();
            if (Strings.isBlank(prefix)) return new PropagatedRuntimeException(e);
            return new PropagatedRuntimeException(prefix + ": " + Exceptions.collapseText(e), e);
        }
        if (Iterables.isEmpty(exceptions)) {
            if (Strings.isBlank(prefix)) return new CompoundRuntimeException("(empty compound exception)", exceptions);
            return new CompoundRuntimeException(prefix, exceptions);
        }
        if (Strings.isBlank(prefix)) return new CompoundRuntimeException(Iterables.size(exceptions)+" errors, including: " + Exceptions.collapseText(exceptions.iterator().next()), exceptions);
        return new CompoundRuntimeException(prefix+"; "+Iterables.size(exceptions)+" errors including: " + Exceptions.collapseText(exceptions.iterator().next()), exceptions);
    }

    /** Some throwables require a prefix for the message to make sense,
     * for instance NoClassDefFoundError's message is often just the type.
     */
    @Beta
    public static boolean isPrefixRequiredForMessageToMakeSense(Throwable t) {
        if (t instanceof NoClassDefFoundError) return true;
        return false;
    }

    /** For {@link Throwable} instances where know {@link #isPrefixRequiredForMessageToMakeSense(Throwable)},
     * this returns a nice message for use as a prefix;
     * returns empty string if {@link #isPrefixBoring(Throwable)} is true;
     * otherwise this returns the simplified class name. */
    private static String getPrefixText(Throwable t) {
        if (t instanceof NoClassDefFoundError) return "Invalid java type";
        if (isPrefixBoring(t)) return "";
        return JavaClassNames.cleanSimpleClassName(t);
    }

    /** Like {@link Throwable#toString()} except suppresses boring prefixes and replaces prefixes with sensible messages where required */
    public static String getMessageWithAppropriatePrefix(Throwable t) {
        return appendSeparator(getPrefixText(t), t.getMessage());
    }

    /** Returns true if the root cause is a "boring" CNF, ie a straightforward declaration that the clazz is not found;
     * this permits callers to include the cause only when it is interesting, ie caused by a dependent class not loadable.
     */
    public static boolean isRootBoringClassNotFound(Exception e, String clazz) {
        Throwable root = Throwables.getRootCause(e);
        if (!(root instanceof ClassNotFoundException)) return false;
        if (clazz.equals(root.getMessage())) return true;
        return false;
    }

}
