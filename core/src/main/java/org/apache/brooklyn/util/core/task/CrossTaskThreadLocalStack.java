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

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.WeakHashMap;
import java.util.stream.Stream;

import com.google.common.collect.Streams;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.util.collections.ThreadLocalStack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrossTaskThreadLocalStack<T> extends ThreadLocalStack<T> {

    private static final Logger log = LoggerFactory.getLogger(CrossTaskThreadLocalStack.class);

    public CrossTaskThreadLocalStack(boolean acceptDuplicates) {
        super(acceptDuplicates);
    }
    public CrossTaskThreadLocalStack() { super(); }

    // override since we cannot access another thread's thread local
    final WeakHashMap<Thread,Collection<T>> backingOverride = new WeakHashMap<>();

    @Override protected Collection<T> get() {
        return get(Thread.currentThread());
    }
    protected Collection<T> get(Thread t) {
        synchronized (backingOverride) { return backingOverride.get(t); }
    }
    @Override protected void set(Collection<T> value) {
        synchronized (backingOverride) { backingOverride.put(Thread.currentThread(), value); }
    }
    @Override protected void remove() {
        synchronized (backingOverride) { backingOverride.remove(Thread.currentThread()); }
    }
    @Override protected Collection<T> getCopyReversed() {
        return getCopyReversed(Thread.currentThread());
    }
    protected Collection<T> getCopyReversed(Thread t) {
        int retries = 0;
        while (true) {
            try {
                synchronized (backingOverride) {return copyReversed(get(t));}
            } catch (ConcurrentModificationException cme) {
                // can happen as the collections within the map are not synchronized. simply retry.
                // unusual if it loops
                if (retries++ >= 10) throw cme;
                log.debug("CME checking cross-thread local stack; retrying (#" + retries + "): " + cme);
            }
        }
    }

    public Stream<T> stream() {
        return concatSubmitterTaskThreadStacks(getCopyReversed().stream(), Tasks.current());
    }

    protected Stream<T> concatSubmitterTaskThreadStacks(Stream<T> stream, Task current) {
        if (current==null) return stream;
        Task submitter = current.getSubmittedByTask();
        if (submitter==null) return stream;
        Collection<T> ss = getCopyReversed(submitter.getThread());
        if (ss!=null && !ss.isEmpty()) stream = Streams.concat(stream, ss.stream());
        return concatSubmitterTaskThreadStacks(stream, submitter);
    }

}
