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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class ThreadLocalStack<T> implements Iterable<T> {

    private final boolean acceptDuplicates;

    /** if duplicates not accepted, the call to push will return false */
    public ThreadLocalStack(boolean acceptDuplicates) {
        this.acceptDuplicates = acceptDuplicates;
    }
    public ThreadLocalStack() { this.acceptDuplicates = true; }

    protected final ThreadLocal<Collection<T>> backing = new ThreadLocal<>();

    protected Collection<T> get() {
        return backing.get();
    }
    protected void set(Collection<T> value) {
        backing.set(value);
    }
    protected void remove() {
        backing.remove();
    }

    protected Collection<T> upsert() {
        Collection<T> result = get();
        if (result==null) {
            result = acceptDuplicates ? MutableList.of() : MutableSet.of();
            set(result);
        }
        return result;
    }

    public T pop() {
        Collection<T> resultS = upsert();
        T last = Iterables.getLast(resultS);
        resultS.remove(last);
        if (resultS.isEmpty()) remove();
        return last;
    }

    /** returns true unless duplicates are not accepted, in which case it returns false iff the object supplied is equal to one already present */
    public boolean push(T object) {
        return upsert().add(object);
    }

    /** top of stack first */
    @Override
    public Iterator<T> iterator() {
        return stream().iterator();
    }

    /** top of stack first */
    public Stream<T> stream() {
        return getCopyReversed().stream();
    }
    protected Collection<T> getCopyReversed() {
        return copyReversed(get());
    }
    protected Collection<T> copyReversed(Collection<T> c1) {
        List<T> l = MutableList.copyOf(c1);
        Collections.reverse(l);
        return l;
    }

    public Maybe<T> peek() {
        Iterator<T> si = stream().iterator();
        if (!si.hasNext()) return Maybe.absent("Nothing in local stack");
        return Maybe.of( si.next() );
    }

    public Maybe<T> peekPenultimate() {
        Iterator<T> si = stream().iterator();
        if (si.hasNext()) si.next();
        if (!si.hasNext()) return Maybe.absent();
        return Maybe.of( si.next() );
    }

    public void pop(T entry) {
        Maybe<T> popped = peek();
        if (popped.isAbsent()) throw new IllegalStateException("Nothing to pop; cannot pop "+entry);
        if (!Objects.equals(entry, popped.get())) throw new IllegalStateException("Stack mismatch, expected to pop "+entry+" but instead would have popped "+popped.get());
        pop();
    }

    public int size() {
        return (int) stream().count();
    }
}
