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

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Supplier;

public class Suppliers {

    private Suppliers() {}

    /**
     * @return A supplier that atomically increments the value returned on each call to
     * {@link Supplier#get get}, starting from zero.
     */
    public static Supplier<Integer> incrementing() {
        return incrementing(0);
    }

    /**
     * @return A supplier that atomically increments the value returned on each call to
     * {@link Supplier#get get}, starting from <code>initialValue</code>.
     */
    public static Supplier<Integer> incrementing(int initialValue) {
        return new IncrementingSupplier(initialValue);
    }

    private static class IncrementingSupplier implements Supplier<Integer> {
        private final AtomicInteger value;

        private IncrementingSupplier(int initialValue) {
            this.value = new AtomicInteger(initialValue);
        }

        @Override
        public Integer get() {
            return value.getAndIncrement();
        }
    }


}
