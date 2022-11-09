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
package org.apache.brooklyn.util.collections;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.util.guava.Maybe;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;

public class ThreadLocalStack<T> implements Iterable<T> {

    private final boolean allowDuplicates;

    public ThreadLocalStack(boolean allowsDuplicates) {
        this.allowDuplicates = allowsDuplicates;
    }

    final ThreadLocal<Collection<T>> set = new ThreadLocal<>();

    public Collection<T> getAll(boolean forceInitialized) {
        Collection<T> result = set.get();
        if (forceInitialized && result==null) {
            result = allowDuplicates ? MutableList.of() : MutableSet.of();
            set.set(result);
        }
        return result;
    }

    public T pop() {
        Collection<T> resultS = getAll(true);
        T last = Iterables.getLast(resultS);
        resultS.remove(last);
        if (resultS.isEmpty()) set.remove();
        return last;
    }

    public boolean push(T object) {
        return getAll(true).add(object);
    }

    @Override
    public Iterator<T> iterator() {
        return null;
    }

    public Maybe<T> peek() {
        Collection<T> resultS = getAll(false);
        if (resultS==null || resultS.isEmpty()) return Maybe.absent("Nothing in local stack");
        return Maybe.of( Iterables.getLast(resultS) );
    }

    public Maybe<T> peekPenultimate() {
        Collection<T> resultS = getAll(false);
        if (resultS==null) return Maybe.absent();
        int size = resultS.size();
        if (size<=1) return Maybe.absent();
        return Maybe.of( Iterables.get(resultS, size-2) );
    }

    public void pop(T entry) {
        Maybe<T> popped = peek();
        if (popped.isAbsent()) throw new IllegalStateException("Nothing to pop; cannot pop "+entry);
        if (!Objects.equals(entry, popped.get())) throw new IllegalStateException("Stack mismatch, expected to pop "+entry+" but instead would have popped "+popped.get());
        pop();
    }

    public int size() {
        Collection<T> v = getAll(false);
        if (v==null) return 0;
        return v.size();
    }
}
