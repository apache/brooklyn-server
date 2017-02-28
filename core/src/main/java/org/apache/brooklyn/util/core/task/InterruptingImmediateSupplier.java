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

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.annotations.Beta;
import com.google.common.base.Supplier;

/**
 * Wraps a {@link Supplier} as an {@link ImmediateSupplier} by interrupting the thread before calling {@link Supplier#get()}.
 * If the call succeeds, the result is returned.
 * If the call throws any trace including an {@link InterruptedException} or {@link RuntimeInterruptedException} 
 * (ie the call failed due to the interruption, typically because it tried to wait) 
 * then this class concludes that there is no value available immediately and returns {@link Maybe#absent()}.
 * If the call throws any other error, that is returned.
 * The interruption is cleared afterwards (unless the thread was interrupted when the method was entered).
 * <p>
 * Note that some "immediate" methods, such as {@link Semaphore#acquire()} when a semaphore is available,
 * will throw if the thread is interrupted.  Typically there are workarounds, for instance:
 * <code>if (semaphore.tryAcquire()) semaphore.acquire();</code>. 
 */
@Beta
public class InterruptingImmediateSupplier<T> implements ImmediateSupplier<T>, DeferredSupplier<T> {

    private final Supplier<T> nestedSupplier;
    
    public InterruptingImmediateSupplier(Supplier<T> nestedSupplier) {
        this.nestedSupplier = nestedSupplier;
    }
    
    @Override
    public Maybe<T> getImmediately() {
        boolean interrupted = Thread.currentThread().isInterrupted();
        try {
            if (!interrupted) Thread.currentThread().interrupt();
            return Maybe.ofAllowingNull(get());
        } catch (Throwable t) {
            if (Exceptions.getFirstThrowableOfType(t, InterruptedException.class)!=null || 
                    Exceptions.getFirstThrowableOfType(t, RuntimeInterruptedException.class)!=null || 
                    Exceptions.getFirstThrowableOfType(t, CancellationException.class)!=null) {
                return Maybe.absent(new UnsupportedOperationException("Immediate value not available", t));
            }
            throw Exceptions.propagate(t);
        } finally {
            if (!interrupted) Thread.interrupted();
        }
    }

    @Override
    public T get() {
        return nestedSupplier.get();
    }

    public static <T> InterruptingImmediateSupplier<T> of(final Object o) {
        return InterruptingImmediateSupplier.<T>ofSafe(o).get();
    }

    @SuppressWarnings("unchecked")
    public static <T> ReferenceWithError<InterruptingImmediateSupplier<T>> ofSafe(final Object o) {
        if (o instanceof Supplier) {
            return ReferenceWithError.newInstanceWithoutError(new InterruptingImmediateSupplier<T>((Supplier<T>)o));
        } else if (o instanceof Callable) {
            return ReferenceWithError.newInstanceWithoutError(new InterruptingImmediateSupplier<T>(new Supplier<T>() {
                @Override
                public T get() {
                    try {
                        return ((Callable<T>)o).call();
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    }
                }
            }));
        } else if (o instanceof Runnable) {
            return ReferenceWithError.newInstanceWithoutError(new InterruptingImmediateSupplier<T>(new Supplier<T>() {
                @Override
                public T get() {
                    ((Runnable)o).run();
                    return null;
                }
            }));
        } else {
            return ReferenceWithError.newInstanceThrowingError(null, new InterruptingImmediateSupplierNotSupportedForObject(o)); 
        }
    }

    public static class InterruptingImmediateSupplierNotSupportedForObject extends UnsupportedOperationException {
        private static final long serialVersionUID = 307517409005386500L;

        public InterruptingImmediateSupplierNotSupportedForObject(Object o) {
            super("Type "+o.getClass()+" not supported as InterruptingImmediateSupplier (instance "+o+")");
        }
    }
}
