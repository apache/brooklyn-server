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

import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Supplier;

/**
 * A {@link Supplier} that has an extra method capable of supplying a value immediately or an absent if definitely not available,
 * or throwing an {@link ImmediateUnsupportedException} if it cannot determine whether a value is immediately available.
 */
public interface ImmediateSupplier<T> extends Supplier<T> {
    
    /**
     * Indicates that a supplier does not support immediate evaluation,
     * i.e. it may need to block to evaluate even if there is a value available
     * (e.g. because the supplier is composed of sub-tasks that do not support {@link ImmediateSupplier}.  
     */
    public static class ImmediateUnsupportedException extends UnsupportedOperationException {
        private static final long serialVersionUID = -7942339715007942797L;
        
        public ImmediateUnsupportedException(String message) {
            super(message);
        }
        public ImmediateUnsupportedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    /**
     * Indicates that an attempt was made to forcibly get a requested immediate value 
     * where blocking is required. See {@link ImmediateSupplier#getImmediately()}, which if
     * it returns an absent result, that absent will throw this.
     * <p>
     * This is useful for passing between contexts that support immediate evaluation,
     * through contexts that do not, to outer contexts which do, as the outer context
     * will be able to use this exception to return a {@link Maybe#absent()} rather than throwing.  
     */
    public static class ImmediateValueNotAvailableException extends RuntimeException {
        private static final long serialVersionUID = -5860437285154375232L;
        
        public ImmediateValueNotAvailableException() { }
        public ImmediateValueNotAvailableException(String message) {
            super(message);
        }
        public ImmediateValueNotAvailableException(String message, Throwable cause) {
            super(message, cause);
        }
        public static <T> Maybe<T> newAbsentWithExceptionSupplier() {
            return Maybe.Absent.changeExceptionSupplier(Maybe.<T>absent(), ImmediateValueNotAvailableException.class);
        }
        public static <T> Maybe<T> newAbsentWrapping(String message, Maybe<?> inner) {
            return Maybe.absent(new ImmediateValueNotAvailableException(message, Maybe.getException(inner)));
        }
    }
    
    /**
     * Gets the value promptly, or returns {@link Maybe#absent()} if the value requires blocking,
     * or throws {@link ImmediateUnsupportedException} if it cannot be determined whether the value requires blocking or not.
     * <p>
     * The {@link Maybe#absent()} returned here indicates that a value definitively <i>is</i> pending, just it is not yet available,
     * and an attempt to {@link Maybe#get()} it should throw an {@link ImmediateValueNotAvailableException};
     * it can be created with {@link ImmediateValueNotAvailableException#newAbsentWithExceptionSupplier()} to
     * avoid creating traces (or simply with <code>Maybe.absent(new ImmediateValueNotAvailableException(...))</code>). 
     * This is in contrast with this method throwing a {@link ImmediateUnsupportedException} which should be done
     * if the presence of an eventual value cannot even be determined in a non-blocking way.
     * <p>
     * Implementations of this method should typically catch the former exception if encountered and return a
     * {@link Maybe#absent()} wrapping it, whereas {@link ImmediateUnsupportedException} instances should be propagated.
     * 
     * @throws ImmediateUnsupportedException as above, if cannot be determined whether a value is or might eventually be available
     */
    Maybe<T> getImmediately();

}
