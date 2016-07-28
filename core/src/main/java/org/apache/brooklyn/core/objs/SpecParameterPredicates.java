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
package org.apache.brooklyn.core.objs;

import com.google.common.base.Predicate;

import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.text.Strings;

public class SpecParameterPredicates {

    /**
     * Returns true if the {@link SpecParameter parameter name} is the same as
     * that on the specified paramater.
     */
    public static Predicate<SpecParameter<?>> sameName(final SpecParameter<?> param) {
        return new SameName(param);
    }

    /** @see #sameName(SpecParameter) */
    protected static class SameName implements Predicate<SpecParameter<?>> {
        private final SpecParameter<?> param;

        public SameName(SpecParameter<?> param) {
            this.param = param;
        }

        @Override
        public boolean apply(SpecParameter<?> input) {
            return input.getConfigKey().getName().equals(param.getConfigKey().getName());
        }

        @Override
        public String toString() {
            return String.format("sameName(%s)",Strings.toString(param));
        }
    }

    /**
     * Returns true if the {@link SpecParameter#getLabel() label} is the same as
     * the specified string.
     */
    public static Predicate<SpecParameter<?>> labelEqualTo(final String label) {
        return new LabelEqualTo(label);
    }

    /** @see #labelEqualTo(String) */
    protected static class LabelEqualTo implements Predicate<SpecParameter<?>> {
        private final String label;

        public LabelEqualTo(String label) {
            this.label = label;
        }

        @Override
        public boolean apply(SpecParameter<?> input) {
            return input.getLabel().equals(label);
        }

        @Override
        public String toString() {
            return String.format("labelEqualTo(%s)",Strings.toString(label));
        }
    }

    /**
     * Returns true if the {@link ConfigKey#getName() config key name} is the same
     * as the specified string.
     */
    public static Predicate<SpecParameter<?>> nameEqualTo(final String name) {
        return new NameEqualTo(name);
    }

    /** @see #nameEqualTo(String) */
    protected static class NameEqualTo implements Predicate<SpecParameter<?>> {
        private final String name;

        public NameEqualTo(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(SpecParameter<?> input) {
            return input.getConfigKey().getName().equals(name);
        }

        @Override
        public String toString() {
            return String.format("nameEqualTo(%s)",Strings.toString(name));
        }
    }

}
