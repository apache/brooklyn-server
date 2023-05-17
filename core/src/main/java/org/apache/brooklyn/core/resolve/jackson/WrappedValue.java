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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.brooklyn.core.validation.BrooklynValidation;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.guava.Maybe;

/**
 * Wraps a value which might be constant or might come from a supplier.
 * The pattern where fields are of this type is used to assist with (de)serialization
 * where values might come from a DSL.
 *
 * This will be unwrapped using {@link org.apache.brooklyn.util.core.flags.TypeCoercions}
 * if it contains a value (unless a WrappedValue is requested), but not if it uses a supplier.
 * Any object can be coerced to a {@link WrappedValue} using {@link org.apache.brooklyn.util.core.flags.TypeCoercions}.
 *
 * It will always be unwrapped (attempted) using {@link org.apache.brooklyn.util.core.task.Tasks#resolving(Object)}.
 *
 * When deserialized, this will parse Brooklyn DSL expressions.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WrappedValue<T> implements Supplier<T>, com.google.common.base.Supplier<T>, DeferredSupplier<T> {
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

    public static class GuavaSupplierAsJavaSupplier<T> implements Supplier<T> {
        final com.google.common.base.Supplier<T> guavaSupplier;
        private GuavaSupplierAsJavaSupplier() {
            this.guavaSupplier = null;
        }
        public GuavaSupplierAsJavaSupplier(com.google.common.base.Supplier<T> guavaSupplier) {
            this.guavaSupplier = Preconditions.checkNotNull(guavaSupplier);
        }
        @Override
        public T get() {
            return guavaSupplier.get();
        }

        public com.google.common.base.Supplier<T> getGuavaSupplier() {
            return guavaSupplier;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GuavaSupplierAsJavaSupplier<?> that = (GuavaSupplierAsJavaSupplier<?>) o;
            return Objects.equals(guavaSupplier, that.guavaSupplier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(guavaSupplier);
        }
    }

    public static <T> WrappedValue<T> of(Object x) {
        if (x instanceof Supplier) return ofSupplier((Supplier<T>)x);
        if (x instanceof com.google.common.base.Supplier) return ofGuavaSupplier((com.google.common.base.Supplier<T>)x);
        return new WrappedValue<>(x, false);
    }
    public static <T> WrappedValue<T> ofConstant(T x) {
        return new WrappedValue<>(x, false);
    }
    public static <T> WrappedValue<T> ofSupplier(Supplier<T> x) { return new WrappedValue<>(x, true); }
    public static <T> WrappedValue<T> ofGuavaSupplier(com.google.common.base.Supplier<T> x) { return new WrappedValue<>(new GuavaSupplierAsJavaSupplier<>( (com.google.common.base.Supplier<T>)x ), true); }
    public static <T> WrappedValue<T> ofNull() { return (WrappedValue<T>)NULL_WRAPPED_VALUE; }

    public T get() {
        return BrooklynValidation.getInstance().ensureValid(supplier != null ? supplier.get() : value);
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
    public String toString() {
        return "WrappedValue{" +
                (value!=null ? "value=" + value : supplier!=null ? "supplier=" + supplier : "null") +
                '}';
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

    /** Convenience to access the value of a wrapper where the wrapper itself may be null. */
    public static <T> T get(WrappedValue<T> wrapper) {
        if (wrapper==null) return null;
        return wrapper.get();
    }

    /** Convenience to wrap the value of a wrapper in a `Maybe<T>` instance. . */
    public static <T> Maybe<T> getMaybe(WrappedValue<T> wrapper) {
        if (wrapper==null) return Maybe.absent();
        return Maybe.of(wrapper.get());
    }

}
