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
package org.apache.brooklyn.util.guava;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.javalang.MemoryUsageTracker.SoftUsageTracker;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Like Guava Optional but permitting null and permitting errors to be thrown. */
public abstract class Maybe<T> implements Serializable, Supplier<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Maybe.class);

    private static final long serialVersionUID = -6372099069863179019L;

    /** Returns an absent indicator. No message is available and access does not include any reference to this creation.
     * Therefore it is fast and simple, but hard to work with if someone might {@link #get()} it and want a useful exception.
     * See also {@link #absentNoTrace(String)} to include a message with very low overhead,
     * or {@link #absentWithTrace(String)} or {@link #absent(Throwable)} for more control over the exception thrown. 
     */
    public static <T> Maybe<T> absent() {
        return new Maybe.Absent<T>();
    }

    /** Convenience for {@link #absentWithTrace(String)}. */
    public static <T> Maybe<T> absent(final String message) {
        return absent(new IllegalStateException(message));
    }

    /** Creates an absent whose {@link #get()} throws an {@link IllegalStateException} with the indicated message.
     * Both stack traces (the cause and the callers) are provided, which can be quite handy,
     * but comparatively expensive as the cause stack trace has to generated when this method is invoked,
     * even if it is never accessed. See also {@link #absentNoTrace(String)} and {@link #absent(Throwable)}. */
    public static <T> Maybe<T> absentWithTrace(final String message) {
        return absent(new IllegalStateException(message));
    }

    /** Creates an absent whose get throws an {@link IllegalStateException} with the indicated message,
     * but not a stack trace for the calling location. As stack traces can be comparatively expensive
     * this is useful for efficiency, but it can make debugging harder as the origin of the absence is not kept,
     * in contrast to {@link #absentWithTrace(String)} and {@link #absent(Throwable)}. */
    public static <T> Maybe<T> absentNoTrace(final String message) {
        return absent(new IllegalStateExceptionSupplier(message));
    }

    /** As {@link #absentWithTrace(String)} but using the provided exception instead of this location
     * as the cause, and a string based on this cause as the message on the {@link IllegalStateException}
     * thrown if a user does a {@link #get()}. 
     * Useful if an {@link Exception} has already been generated (and no overhead)
     * or if you want to supply a specific cause as in <code>absent(new MyException(...))</code>
     * (but there is the Exception creation overhead there). */
    public static <T> Maybe<T> absent(final Throwable cause) {
        return absent(new IllegalStateExceptionSupplier(cause));
    }
    
    /** As {@link #absent(Throwable)} but using the given message as the message on the {@link IllegalStateException}
     * thrown if a user does a {@link #get()}. */
    public static <T> Maybe<T> absent(final String message, final Throwable cause) {
        return absent(new IllegalStateExceptionSupplier(message, cause));
    }
    
    /** Creates an absent whose {@link #get()} throws a {@link RuntimeException} 
     * generated on demand from the given supplier */
    public static <T> Maybe<T> absent(final Supplier<? extends RuntimeException> exceptionSupplier) {
        return new Absent<T>(Preconditions.checkNotNull(exceptionSupplier));
    }

    /** as {@link #absentNull(String)} but with a generic message */
    public static <T> Maybe<T> absentNull() {
        return absentNull("disallowed null value");
    }
    
    /** like {@link #absent(String)} but {@link #isNull()} will return true on the result. */
    public static <T> Maybe<T> absentNull(String message) {
        return new AbsentNull<T>(message);
    }
    
    /** Creates a new Maybe object which is present. 
     * The argument may be null and the object still present, 
     * which may be confusing in some contexts
     * (traditional {@link Optional} usages) but
     * may be natural in others (where null is a valid value, distinguished from no value set). 
     * See also {@link #ofDisallowingNull(Object)}. */
    public static <T> Maybe<T> ofAllowingNull(@Nullable T value) {
        return new Present<T>(value);
    }

    /** Creates a new Maybe object which is present if and only if the argument is not null.
     * If the argument is null, then an {@link #absentNull()} is returned,
     * on which {@link #isNull()} will be true. */
    public static <T> Maybe<T> ofDisallowingNull(@Nullable T value) {
        if (value==null) return absentNull();
        return new Present<T>(value);
    }

    /** Creates a new Maybe object.
     * Currently this uses {@link #ofAllowingNull(Object)} semantics,
     * but it is recommended to use that method for clarity 
     * if the argument might be null. */
    // note: Optional throws if null is supplied; we might want to do the same here
    public static <T> Maybe<T> of(@Nullable T value) {
        return ofAllowingNull(value);
    }
    
    /**
     * Casts the given value to the desired super-type. This is valid because {@link Maybe} is immutable,
     * so things like {@code Maybe<Object>} is a super-type of {@code Maybe<String>}.
     */
    @SuppressWarnings("unchecked")
    public static <T> Maybe<T> cast(Maybe<? extends T> value) {
        return (Maybe<T>) value;
    }

    /** Like {@link #cast(Maybe)} but allows any casting because that is valid for absents.
     * Enforces that the argument really is absent (or null). */
    @SuppressWarnings("unchecked")
    public static <T> Maybe<T> castAbsent(Maybe<?> absent) {
        if (absent!=null && absent.isPresent() && !absent.isNull()) {
            throw new IllegalArgumentException("Expected an absent, but instead got: "+absent);
        }
        return (Maybe<T>)absent;
    }

    /** Converts the given {@link Maybe} to {@link Optional} (guava), failing if this {@link Maybe} contains null.
     * Recommend use {@link #toGuavaOptional()} instead to avoid confusion. */
    public Optional<T> toOptional() {
        return toGuavaOptional();
    }

    /** Converts the given {@link Maybe} to {@link Optional}, failing if this {@link Maybe} contains null. */
    public Optional<T> toGuavaOptional() {
        if (isPresent()) return Optional.of(get());
        return Optional.absent();
    }

    /** Converts the given {@link Maybe} to {@link java.util.Optional}, failing if this {@link Maybe} contains null. */
    public java.util.Optional<T> toJavaOptional() {
        if (isPresent()) return java.util.Optional.of(get());
        return java.util.Optional.empty();
    }

    /** Creates a new Maybe object using {@link #ofDisallowingNull(Object)} semantics.
     * It is recommended to use that method for clarity. 
     * This method is provided for consistency with {@link Optional#fromNullable(Object)}. */
    public static <T> Maybe<T> fromNullable(@Nullable T value) {
        return ofDisallowingNull(value);
    }
    
    /** Creates a new Maybe object out of the {@link Optional} argument */
    public static <T> Maybe<T> fromOptional(Optional<T> value) {
        return new MaybeGuavaOptional<>(value);
    }

    /** Creates a new Maybe object out of the {@link Optional} argument */
    public static <T> Maybe<T> fromOptional(java.util.Optional<T> value) {
        return new MaybeJavaOptional<>(value);
    }

    /** creates an instance wrapping a {@link SoftReference}, so it might go absent later on.
     * if null is supplied the result is a present null. */
    public static <T> Maybe<T> soft(@Nonnull T value) {
        return softThen(value, null);
    }
    /** creates an instance wrapping a {@link SoftReference}, using the second item given 
     * if the first argument is dereferenced.
     * however if the first argument is null, this is a permanent present null,
     * as {@link #of(Object)} with null. */
    public static <T> Maybe<T> softThen(T value, Maybe<T> ifEmpty) {
        if (value==null) return of((T)null);
        return new SoftlyPresent<T>(value).usingAfterExpiry(ifEmpty);
    }

    public static <T> Maybe<T> of(final Optional<T> value) {
        return fromOptional(value);
    }

    private static <T> Maybe<T> ofOldKeptForDeserializationOfAnonymousInnerClass(final Optional<T> value) {
        if (value.isPresent()) return new AbstractPresent<T>() {
            private static final long serialVersionUID = -5735268814211401356L;
            @Override
            public T get() {
                return value.get();
            }
        };
        return absent();
    }

    public static <T> Maybe<T> of(final java.util.Optional<T> value) {
        return fromOptional(value);
    }
    
    @SuppressWarnings("unused")
    private static <T> Maybe<T> ofOldKeptForDeserializationOfAnonymousInnerClass(final Supplier<T> value) {
        return new AbstractPresent<T>() {
            private static final long serialVersionUID = -5735268814211401356L;
            @Override
            public T get() {
                return value.get();
            }
        };
    }

    public static class MaybeGuavaOptional<T> extends Maybe<T> {
        private static final long serialVersionUID = -823731500051341455L;
        private final Optional<T> value;
        public MaybeGuavaOptional(Optional<T> value) {
            this.value = value;
        }
        @Override
        public T get() {
            return value.get();
        }
        @Override
        public boolean isNull() { return false; }
        public Optional<T> getOptional() {
            return value;
        }
        @Override
        public boolean isPresent() { return value.isPresent(); }
    }

    public static class MaybeJavaOptional<T> extends Maybe<T> {
        private static final long serialVersionUID = -823731500051341455L;
        private final java.util.Optional<T> value;
        public MaybeJavaOptional(java.util.Optional<T> value) {
            this.value = value;
        }
        @Override
        public T get() {
            return value.get();
        }
        @Override
        public boolean isNull() { return false; }
        public java.util.Optional<T> getOptional() {
            return value;
        }
        @Override
        public boolean isPresent() { return value.isPresent(); }
    }
    
    public static <T> Maybe<T> of(final Supplier<T> value) {
        return new MaybeSupplier<T>(value);
    }
    
    public static class MaybeSupplier<T> extends AbstractPresent<T> {
        private static final long serialVersionUID = -823731500051341455L;
        private final Supplier<T> supplier;
        public MaybeSupplier(Supplier<T> value) {
            this.supplier = value;
        }
        @Override
        public T get() {
            return supplier.get();
        }
        public Supplier<T> getSupplier() {
            return supplier;
        }
    }

    /** returns a Maybe containing the next element in the iterator, or absent if none */
    public static <T> Maybe<T> next(Iterator<T> iterator) {
        return iterator.hasNext() ? Maybe.of(iterator.next()) : Maybe.<T>absent();
    }

    public abstract boolean isPresent();
    @Override
    public abstract T get();
    
    public boolean isAbsent() {
        return !isPresent(); 
    }
    public boolean isAbsentOrNull() {
        return isAbsent() || isNull();
    }
    public boolean isPresentAndNonNull() {
        return isPresent() && !isNull();
    }
    /** Whether the value is null, if present, or
     * if it was specified as absent because it was null,
     * e.g. using {@link #fromNullable(Object)}.
     */
    public abstract boolean isNull();
    
    public T or(T nextValue) {
        if (isPresent()) return get();
        return nextValue;
    }

    public Maybe<T> or(Maybe<T> nextValue) {
        if (isPresent()) return this;
        return nextValue;
    }

    public T or(Supplier<T> nextValue) {
        if (isPresent()) return get();
        return nextValue.get();
    }

    public Maybe<T> orMaybe(Supplier<Maybe<T>> nextValue) {
        if (isPresent()) return this;
        return nextValue.get();
    }

    public T orNull() {
        if (isPresent()) return get();
        return null;
    }

    /** As {@link #get()} but if the Maybe wraps an exception 
     * (and where {@link #get()} throws a {@link RuntimeException} indicating the caller,
     * caused by the exception in original processing)
     * this throws the original unwrapped exception
     * (masking the caller's location)
     * <p>
     * As this masks the caller's exception, use with care, typically in a location
     * near the original execution, otherwise it can get confusing.
     * For instance <code>someCallReturningMaybe().orThrowUnwrapped()</code> is a nice idiom,
     * but <code>someMaybeVarReturnedEarlier.orThrowUnwrapped()</code> is usually a bad idea.
     * <p>
     * The benefit of this is simpler stack traces and preservation of original exception type.
     */
    public T orThrowUnwrapped() {
        // note: the difference in behaviour to get() is in the Absent subclass
        return get();
    }

    public Maybe<T> orThrowing(String message) {
        return or(Maybe.absent(message));
    }

    public Maybe<T> orThrowing(Throwable t) {
        return or(Maybe.absent(t));
    }

    public Maybe<T> orThrowing(Supplier<RuntimeException> t) {
        return or(Maybe.absent(t));
    }


    public T orThrow(String message) {
        return orThrowing(message).get();
    }

    public T orThrow(Throwable t) {
        return orThrowing(t).get();
    }

    public T orThrow(Supplier<RuntimeException> t) {
        return orThrowing(t).get();
    }

    public Set<T> asSet() {
        if (isPresent()) return ImmutableSet.of(get());
        return Collections.emptySet();
    }

    /** alias for {@link #map(Function)} - does lazy conversion */
    public <V> Maybe<V> transform(final Function<? super T, V> f) {
        return map(f);
    }

    /** lazy conversion if a value is present, otherwise preserves the absence;
     * see {@link #transformNow(Function)} for immediate conversion */
    public <V> Maybe<V> map(final Function<? super T, V> f) {
        return new MaybeTransforming(this, f);
    }

    private <V> Maybe<V> mapKeptForDeserializingOld(final Function<? super T, V> f) {
        if (isPresent()) return new AbstractPresent<V>() {
            private static final long serialVersionUID = 325089324325L;
            @Override
            public V get() {
                return f.apply(Maybe.this.get());
            }
        };
        return (Maybe<V>)this;
    }

    public static class MaybeTransforming<T,V> extends Maybe<V> {
        private static final long serialVersionUID = 325089324325L;
        private final Maybe<T> input;
        private final Function<? super T, V> f;

        public MaybeTransforming(Maybe<T> input, Function<? super T,V> f) {
            this.input = input;
            this.f = f;
        }

        @Override
        public boolean isPresent() {
            return input.isPresent();
        }

        @Override
        public V get() {
            return f.apply(input.get());
        }

        @Override
        public boolean isNull() {
            return isPresent() ? get()==null : input.isNull();
        }
    }

    /** applies a function immediately if a value is present and returns the transformed object,
     * or returns the original absence */
    public <V> Maybe<V> transformNow(final Function<? super T, V> f) {
        if (isPresent()) return Maybe.of(f.apply(Maybe.this.get()));
        return (Maybe<V>)this;
    }

    /** applies a consumer immediately if a value is present;
     * returns the same object for convenience (as obviously there is no result from the consumer) */
    public Maybe<T> apply(final Consumer<? super T> f) {
        if (isPresent()) {
            f.accept(Maybe.this.get());
        }
        return this;
    }

    /**
     * Returns the value of each present instance from the supplied {@code maybes}, in order,
     * skipping over occurrences of {@link Maybe#absent()}. Iterators are unmodifiable and are
     * evaluated lazily.
     *
     * @see Optional#presentInstances(Iterable)
     */
    @Beta
    public static <T> Iterable<T> presentInstances(final Iterable<? extends Maybe<? extends T>> maybes) {
        checkNotNull(maybes);
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new AbstractIterator<T>() {
                    private final Iterator<? extends Maybe<? extends T>> iterator = checkNotNull(maybes.iterator());

                    @Override
                    protected T computeNext() {
                        while (iterator.hasNext()) {
                            Maybe<? extends T> maybe = iterator.next();
                            if (maybe.isPresent()) { return maybe.get(); }
                        }
                        return endOfData();
                    }
                };
            }
        };
    }
    
    public static class Absent<T> extends Maybe<T> {
        private static final long serialVersionUID = -757170462010887057L;
        private final Supplier<? extends RuntimeException> exception;
        public Absent() {
            this(IllegalStateExceptionSupplier.EMPTY_EXCEPTION);
        }
        public Absent(Supplier<? extends RuntimeException> exception) {
            this.exception = exception;
        }
        @Override
        public boolean isPresent() {
            return false;
        }
        @Override
        public boolean isNull() {
            return false;
        }
        @Override
        public T get() {
            throw getException();
        }
        @Override
        public T orThrowUnwrapped() {
            throw getException();
        }
        public RuntimeException getException() {
            return exception.get();
        }
        public Supplier<? extends RuntimeException> getExceptionSupplier() {
            return exception;
        }
        public static <T> Maybe<T> changeExceptionSupplier(Maybe<T> original, final Class<? extends RuntimeException> type) {
            return changeExceptionSupplier(original, new Function<AnyExceptionSupplier<?>,Supplier<? extends RuntimeException>>() {
                @SuppressWarnings("unchecked")
                @Override
                public Supplier<? extends RuntimeException> apply(AnyExceptionSupplier<?> input) {
                    if (type.isInstance(input)) return (Supplier<? extends RuntimeException>) input;
                    return new AnyExceptionSupplier<RuntimeException>(type, input.getMessageSupplier(), input.getCause());
                }
            });
        }
        public static <T> Maybe<T> changeExceptionSupplier(Maybe<T> original, Function<AnyExceptionSupplier<?>,Supplier<? extends RuntimeException>> transform) {
            if (original==null || original.isPresent()) return original;

            while (original instanceof MaybeTransforming) original = ((MaybeTransforming)original).input;
            if (!(original instanceof Maybe.Absent)) {
                LOG.debug("Cannot replace exception supplier for "+original.getClass()+" "+original+"; ignoring");
                LOG.trace("Cannot replace exception supplier for "+original.getClass()+" "+original+"; trace supplied", new Throwable("trace for irreplaceable exception supplier"));
                return original;
            }
            
            final Supplier<? extends RuntimeException> supplier = ((Maybe.Absent<?>)original).getExceptionSupplier();
            if (!(supplier instanceof AnyExceptionSupplier)) return original;
            
            return Maybe.absent(transform.apply((AnyExceptionSupplier<?>)supplier));
        }
    }

    public static class AbsentNull<T> extends Absent<T> {
        private static final long serialVersionUID = 2422627709567857268L;
        public AbsentNull(String message) {
            super(new IllegalStateExceptionSupplier(message));
        }
        @Override
        public boolean isNull() {
            return true;
        }
    }
    
    public static abstract class AbstractPresent<T> extends Maybe<T> {
        private static final long serialVersionUID = -2266743425340870492L;
        protected AbstractPresent() {
        }
        @Override
        public boolean isNull() {
            return get()==null;
        }
        @Override
        public boolean isPresent() {
            return true;
        }
    }

    public static class Present<T> extends AbstractPresent<T> {
        private static final long serialVersionUID = 436799990500336015L;
        private final T value;
        protected Present(T value) {
            this.value = value;
        }
        @Override
        public T get() {
            return value;
        }
    }

    public static class SoftlyPresent<T> extends Maybe<T> {
        private static final SoftUsageTracker TRACKER = new SoftUsageTracker();
        private static final long serialVersionUID = 436799990500336015L;
        private final SoftReference<T> value;
        private Maybe<T> defaultValue;
        protected SoftlyPresent(@Nonnull T value) {
            this.value = new SoftReference<T>(value);
            TRACKER.track(this, this.value);
        }
        @Override
        public T get() {
            T result = value.get();
            if (result!=null) return result;
            if (defaultValue==null) throw new IllegalStateException("Softly present item has been GC'd");
            return defaultValue.get();
        }
        @Override
        public T orNull() {
            T result = value.get();
            if (result!=null) return result;
            if (defaultValue==null) return null;
            return defaultValue.orNull();
        }
        @Override
        public boolean isPresent() {
            return value.get()!=null || (defaultValue!=null && defaultValue.isPresent()); 
        }
        @Override
        public boolean isNull() {
            // null not allowed here
            return false;
        }
        public Maybe<T> solidify() {
            return Maybe.fromNullable(value.get());
        }
        SoftlyPresent<T> usingAfterExpiry(Maybe<T> defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        /** Returns the global usage tracker to determine how many references are soft */
        public static SoftUsageTracker getUsageTracker() { return TRACKER; }
    }

    @Override
    public String toString() {
        return JavaClassNames.simpleClassName(this)+"["+(isPresent()?"value="+get():"")+"]";
    }

    @Override
    public int hashCode() {
        if (!isPresent()) return Objects.hashCode(31, isPresent());
        return Objects.hashCode(31, get());
    }
    
    /** Two {@link Maybe} instances are equal if both present wrapping the same value,
     * or if both are absent for any reason.
     * <p>
     * Specifically, in cases of absences, the reasons for absence are not compared.
     * This could be revisited if there is compelling reason to do so, but in the main
     * the cause of an absence is interesting for giving information to the user.
     * Note this is different to the behaviour of {@link Optional} which says absences
     * are only equal if they are the same instance.
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Maybe)) return false;
        Maybe<?> other = (Maybe<?>)obj;
        if (!isPresent()) { 
            if (other.isPresent()) return false;
            // could compare exceptions; see javadoc
            return true;
        }
        if (!other.isPresent()) {
            return false;
        }
        return Objects.equal(get(), other.get());
    }

    /** Finds the {@link Absent#getException()} if {@link #isAbsent()}, or else null */
    public static RuntimeException getException(Maybe<?> t) {
        if (t instanceof Maybe.Absent) return ((Maybe.Absent<?>)t).getException();
        if (t.isPresent()) return null;
        if (t instanceof MaybeTransforming) return getException( ((MaybeTransforming)t).input );

        // fall back to attempting access
        try {
            t.get();
        } catch (RuntimeException e) {
            return e;
        }
        return new RuntimeException("Unsupported exception access on maybe type "+t.getClass()+", not present, but not in error: "+t);
    }
}
