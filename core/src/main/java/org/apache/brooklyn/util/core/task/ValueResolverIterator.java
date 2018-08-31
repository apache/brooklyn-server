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
package org.apache.brooklyn.util.core.task;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.reflect.TypeToken;

/**
 * Resolve one step at a time, returning intermediate results or absent if there's a resolve error.
 * Each step is a single one-level evaluation of the previous value.
 * Use this if you are interested in intermediate suppliers wrapping the end result. Use ValueResolver if you need the deeper-most value.
 */
public class ValueResolverIterator<T> implements Iterator<Maybe<Object>> {
    private static final Maybe<Object> NEXT_VALUE = Maybe.absent();
    private ValueResolver<T> resolver;
    private Maybe<Object> prev;

    // * Present when resolving is successful
    // * Absent when resolving failed
    // * null when last value reached; can't resolve further either due to error or value is non-resolvable
    private Maybe<Object> next;

    public ValueResolverIterator(ValueResolver<T> resolver) {
        this.resolver = resolver;
        this.prev = Maybe.of(resolver.getOriginalValue());
        this.next = prev;
    }

    /**
     * @return the current element in the iterator. If {@link Maybe#absent()} there was a resolve error.
     */
    public Maybe<Object> peek() {
        return prev;
    }

    /**
     * Returns {@code true} if the current value is resolvable.
     */
    @Override
    public boolean hasNext() {
        fetchNext();
        return next != null;
    }

    /**
     * Resolves the value recursively, returning the immediately resolved value.
     * If there's a resolve failure a {@link Maybe#absent()} is returned containing
     * the failure description as the last element.
     */
    @Override
    public Maybe<Object> next() {
        if (!hasNext()) {
            if (prev.isPresent()) {
                throw new NoSuchElementException("The value " + prev.get() + " is non-resolvable");
            } else {
                throw new NoSuchElementException("Last resolve failed: " + prev);
            }
        }
        prev = next;
        next = NEXT_VALUE;
        return prev;
    }

    private void fetchNext() {
        if (next == NEXT_VALUE) {
            if (prev.isPresent()) {
                Object prevValue = prev.get();
                if (prevValue != null) {
                    ValueResolver<Object> nextResolver = createIterativeResolver(prevValue);
                    try {
                        next = nextResolver.getMaybe();
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        next = Maybe.absent("Failed resolving " + prev + " with resolver " + resolver, e);
                    }
                    if (next.isPresent() && next.get() == prev.get()) {
                        // Resolved value same as previous value, last element reached
                        next = null;
                    }
                } else {
                    // Can't resolve null further
                    // Same as previous case, just cuts on on the resolver calls
                    next = null;
                }
            } else {
                // Resolve error, can't continue
                next = null;
            }
        }
    }

    @Override
    public void remove() {
        throw new IllegalStateException("Operation not supported");
    }

    private ValueResolver<Object> createIterativeResolver(Object value) {
        return resolver.cloneReplacingValueAndType(value, TypeToken.of(Object.class))
            .recursive(false);
    }

    /**
     * @return the first resolved value satisfying the {@code stopCondition}.
     * If none is found return the last element.
     */
    public Maybe<Object> nextOrLast(Predicate<Object> stopCondition) {
        if (!hasNext()) {
            return prev;
        }
        Maybe<Object> item;
        do {
            item = next();
            if (item.isAbsent() || stopCondition.apply(item.get())) {
                return item;
            }
        } while (hasNext());
        return item;
    }

    /**
     * @return the first resolved value instance of {@code type}.
     * If none is found return the last element.
     */
    public Maybe<Object> nextOrLast(Class<?> type) {
        return nextOrLast(Predicates.instanceOf(type));
    }

    /**
     * @return the first resolved value instance of {@code type}.
     * If not found returns {@link Maybe#absent()} either due to
     * the end reached or a resolve error. To check if there was a
     * resolve error call {@code peek().isAbsent()}.
     */
    public <S> Maybe<S> next(Class<S> type) {
        while (hasNext()) {
            Maybe<Object> item = next();
            if (item.isAbsent() || type.isInstance(item.get())) {
                @SuppressWarnings("unchecked")
                Maybe<S> typedItem = (Maybe<S>) item;
                return typedItem;
            }
        }
        return Maybe.absent("Did not find items of type " + type + " in " + resolver);
    }

    /**
     * @return the last element of the iterator reached either because
     * resolving no longer possible or there was a resolve error.
     */
    @SuppressWarnings("unchecked")
    public Maybe<T> last() {
        Maybe<Object> last = peek();
        while (hasNext()) {
            last = next();
            if (last.isAbsent()) {
                return (Maybe<T>) last;
            }
        }
        return coerceValue(last, resolver.getTypeToken());
    }

    private Maybe<T> coerceValue(Maybe<Object> valueMaybe, TypeToken<T> type) {
        if (valueMaybe.isPresent()) {
            T coercedValue = TypeCoercions.coerce(valueMaybe.get(), type);
            return Maybe.of(coercedValue);
        } else {
            @SuppressWarnings("unchecked")
            Maybe<T> uncheckedValue = (Maybe<T>) valueMaybe;
            return uncheckedValue;
        }
    }

}
