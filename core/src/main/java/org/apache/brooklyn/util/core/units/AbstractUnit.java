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
package org.apache.brooklyn.util.core.units;

import org.apache.brooklyn.util.guava.Maybe;

import java.util.Objects;
import java.util.function.Function;

/** a unit of size, eg "1 gb" */
public abstract class AbstractUnit {

    protected final long value;
    private static final String[] PREFIXES = { "", "K", "M", "G", "T" };

    /** json constructor */
    protected AbstractUnit() { this(0); }

    public AbstractUnit(long v) {
        value = v;
    }

    protected String[] prefixes() { return PREFIXES; }
    protected abstract String unit();
    protected abstract String binaryUnit();

    public String toString() {
        if (value==0) return "0 "+unit();
        return toString(1000, s -> s+unit())
                .orMaybe(() -> binaryUnit()==null ? Maybe.absent() : toString(1024, s -> s+binaryUnit()))
                .or(() -> value + " " + unit());
    }

    private Maybe<String> toString(int modulus, Function<String,String> unitFn) {
        if (value % modulus == 0) {
            long v = value;
            int unit = 0;
            while (v % modulus == 0 && unit+1 < PREFIXES.length) {
                v /= modulus;
                unit++;
            }
            return Maybe.of(v + " " + unitFn.apply(PREFIXES[unit]));
        }
        return Maybe.absent();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractUnit other = (AbstractUnit) o;
        return value == other.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    public int compareTo(AbstractUnit o) {
        return Long.compare(value, o.value);
    }

}
