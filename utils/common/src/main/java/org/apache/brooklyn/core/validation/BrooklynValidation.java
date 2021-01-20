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
package org.apache.brooklyn.core.validation;

import com.google.common.annotations.VisibleForTesting;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import org.apache.brooklyn.util.exceptions.Exceptions;

/** Stores validators for objects in a {@link java.util.WeakHashMap} and allows validation to be performed on objects upon request. */
public class BrooklynValidation {

    protected static final BrooklynValidation INSTANCE = new BrooklynValidation();
    public static BrooklynValidation getInstance() { return INSTANCE; }

    protected WeakHashMap<Object,Consumer<Object>> record = new WeakHashMap();
    protected ThreadLocal<Boolean> validationSuppressed = new ThreadLocal<>();

    static class SameInstanceEqualsHashWrapper {
        final Object target;

        SameInstanceEqualsHashWrapper(Object target) { this.target = target; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (target == o) return true;
            if (o instanceof SameInstanceEqualsHashWrapper) return target == ((SameInstanceEqualsHashWrapper)o).target;
            return false;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(target);
        }
    }

    /** Sets a validator for the given object instance. Returns any previous validator set (which will now be replaced) or null.
     * The validator will be invoked when consumers of this API invoke {@link #validateIfPresent(Object)} and
     * should simply throw an exception if the object at that time is invalid.
     * <p>
     * Lookup to the validator is expected on this exact instance, irrespective of whether the instance changes.
     * The validator will not run on other instances which are equal to the target and/or have the same hash code. */
    public <T> Consumer<? super T> setValidatorForInstance(T object, Consumer<? super T> validator) {
        return record.put(object, (Consumer)validator);
    }

    /** Validates the object if present, returning whether there was a validator present. Validator should throw exception if there is any validation issue.
     * Exceptions thrown by the validator are thrown without interception by this method.
     *
     * @param target
     * @return whether validation was performed
     */
    public boolean validateIfPresent(Object target) {
        if (Boolean.TRUE.equals(validationSuppressed.get())) return false;

        Consumer<Object> v = null;
        //record.get(target);  // could do this to allow equal objects to be looked up
        if (v==null) {
            v = record.get(new SameInstanceEqualsHashWrapper(target));
        }
        if (v==null) {
            return false;
        }
        v.accept(target);
        return true;
    }

    /** Performs validation if present then returns the input for convenience. */
    public <T> T ensureValid(T t) {
        validateIfPresent(t);
        return t;
    }

    @VisibleForTesting
    public int size() {
        return record.size();
    }

    public <T> T withValidationSuppressed(Callable<T> target) {
        Boolean old = validationSuppressed.get();
        try {
            validationSuppressed.set(true);

            return target.call();

        } catch (Exception e) {
            throw Exceptions.propagate(e);

        } finally {
            if (old!=null) {
                validationSuppressed.set(old);
            } else {
                validationSuppressed.remove();;
            }
        }
    }

}
