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
     * Gets the value promptly, or returns {@link Maybe#absent()} if the value is not yet available.
     * 
     * @throws ImmediateUnsupportedException if cannot determine whether a value is immediately available
     */
    Maybe<T> getImmediately();
}
