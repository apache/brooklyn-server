/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.camp.brooklyn.spi.dsl.methods;

import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslCallable;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.guava.Maybe;

public class DslTestObjects {

    public static class DslTestSupplierWrapper {
        private Object supplier;

        public DslTestSupplierWrapper(Object supplier) {
            this.supplier = supplier;
        }

        public Object getSupplier() {
            return supplier;
        }
    }

    public static class TestDslSupplierValue {
        public boolean isSupplierEvaluated() {
            return true;
        }
    }

    public static class TestDslSupplier implements DeferredSupplier<Object>, ImmediateSupplier<Object> {
        private Object value;

        public TestDslSupplier(Object value) {
            this.value = value;
        }

        @Override
        public Object get() {
            return getImmediately().get();
        }

        @Override
        public Maybe<Object> getImmediately() {
            return Maybe.of(value);
        }
    }

    public static class DslTestCallable implements DslCallable, DeferredSupplier<TestDslSupplier>, ImmediateSupplier<TestDslSupplier> {

        @Override
        public Maybe<TestDslSupplier> getImmediately() {
            throw new IllegalStateException("Not to be called");
        }

        @Override
        public TestDslSupplier get() {
            throw new IllegalStateException("Not to be called");
        }

        public boolean isSupplierCallable() {
            return true;
        }
    }

}
