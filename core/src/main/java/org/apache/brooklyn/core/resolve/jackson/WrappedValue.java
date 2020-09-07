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
package org.apache.brooklyn.core.resolve.jackson;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Wraps a value which might be constant or might come from a supplier.
 * The pattern where fields are of this type is used to assist with (de)serialization
 * where values might come from a DSL.
 */
//@JsonSerialize(using = WrappedValueSerializer.class)
//@JsonDeserialize(using = WrappedValueDeserializer.class)
public class WrappedValue<T> implements Supplier<T> {
    final static WrappedValue<?> NULL_WRAPPED_VALUE = new WrappedValue<>(null, false);
    final T value;
    final Supplier<T> supplier;

    private WrappedValue(Object x, boolean isSupplier) {
        if (isSupplier) {
            supplier = (Supplier<T>) x;
            value = null;
        } else {
            value = (T) x;
            supplier = null;
        }
    }

    public static <T> WrappedValue<T> of(Object x) {
        return new WrappedValue<>(x, x instanceof Supplier);
    }
    public static <T> WrappedValue<T> ofConstant(T x) {
        return new WrappedValue<>(x, false);
    }
    public static <T> WrappedValue<T> ofSupplier(Supplier<T> x) { return new WrappedValue<>(x, true); }
    public static <T> WrappedValue<T> ofNull() { return (WrappedValue<T>)NULL_WRAPPED_VALUE; }

    public T get() {
        return supplier != null ? supplier.get() : value;
    }

    public Supplier<T> getSupplier() {
        return supplier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WrappedValue<?> that = (WrappedValue<?>) o;
        return Objects.equals(value, that.value) && Objects.equals(supplier, that.supplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, supplier);
    }

    /** Convenience superclass to ensure all WrappedValue fields are initialized at creation time */
    public static class WrappedValuesInitialized {
        public WrappedValuesInitialized() {
            WrappedValuesSerialization.ensureWrappedValuesInitialized(this);
        }
    }

}
