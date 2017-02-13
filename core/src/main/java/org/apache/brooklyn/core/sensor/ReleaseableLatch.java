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
package org.apache.brooklyn.core.sensor;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

public interface ReleaseableLatch {
    /**
     * Increment usage count for the {@code caller} entity. Implementations may ignore {@code caller}.
     * 
     * @param caller The entity latching on the object
     */
    void acquire(Entity caller);

    /**
     * Decrement usage count for the {@code caller} entity. Implementations may ignore {@code caller}.
     *
     * @param caller The entity latching on the object
     */
    void release(Entity caller);

    ReleaseableLatch NOP = new Factory.NopLatch();

    static class Factory {
        static {
            TypeCoercions.registerAdapter(ReleaseableLatch.class, Boolean.class, new Function<ReleaseableLatch, Boolean>() {
                @Override public Boolean apply(ReleaseableLatch input) { return Boolean.TRUE; }
            });
        }

        private static class NopLatch implements ReleaseableLatch {
            @Override public void acquire(Entity caller) {}
            @Override public void release(Entity caller) {}
        }

        private static class MaxConcurrencyLatch implements ReleaseableLatch {
            private static final Logger LOG = LoggerFactory.getLogger(MaxConcurrencyLatch.class);

            private int permits;
            private transient final Semaphore sem;

            // Not initialized on rebind, but #readResolve() will make sure a
            // properly initialized object is used instead.
            private transient final Set<Entity> ownerEntities = Collections.newSetFromMap(new ConcurrentHashMap<Entity, Boolean>());

            public MaxConcurrencyLatch(int permits) {
                this.permits = permits;
                this.sem = new Semaphore(permits);
            }

            /**
             * Decreases the available permits by one regardless of whether a previous unreleased call
             * to the method was done by {@code caller} in this or another thread.
             */
            @Override
            public void acquire(Entity caller) {
                if (!ownerEntities.add(caller)) {
                    LOG.warn("Entity {} acquiring permit multiple times with ~{} permits available and ~{} threads waiting in queue",
                            new Object[] {caller, sem.availablePermits(), sem.getQueueLength()});
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Entity " + caller + " double-acquire call stack:", new RuntimeException("Call stack for permit double-acquire"));
                    }
                }
                try {
                    sem.acquire();
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            }

            /**
             * Increments the available permits by one. No check is done whether a previous call
             * to {@code acquire} has been made.
             */
            @Override
            public void release(Entity caller) {
                try {
                    sem.release();
                } finally {
                    ownerEntities.remove(caller);
                }
            }

            // On rebind reset thread count
            private Object readResolve() {
                return newMaxConcurrencyLatch(permits);
            }

            @Override
            public String toString() {
                return getClass().getSimpleName() + "[permits=" + sem.availablePermits() + "/" + permits + "]";
            }
        }

        public static ReleaseableLatch newMaxConcurrencyLatch(int maxThreadsNum) {
            return new MaxConcurrencyLatch(maxThreadsNum);
        }

    }

}
