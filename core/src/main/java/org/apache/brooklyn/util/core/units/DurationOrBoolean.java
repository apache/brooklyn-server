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

import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.time.Duration;

public class DurationOrBoolean {

    static {
        TypeCoercions.registerAdapter(DurationOrBoolean.class, Duration.class, DurationOrBoolean::asDuration);
        TypeCoercions.registerAdapter(Boolean.class, DurationOrBoolean.class, DurationOrBoolean::new);
        TypeCoercions.registerAdapter(Duration.class, DurationOrBoolean.class, DurationOrBoolean::new);
        TypeCoercions.registerAdapter(String.class, DurationOrBoolean.class, DurationOrBoolean::fromString);
    }

    private DurationOrBoolean() {
        this(null);
    }

    public DurationOrBoolean(Duration d) {
        this.durationSupplied = d;
        this.booleanSupplied = null;
    }

    public DurationOrBoolean(boolean b) {
        this.booleanSupplied = b;
        this.durationSupplied = null;
    }

    public static DurationOrBoolean fromString(String s) {
        if (s==null) return null;
        if (s.equalsIgnoreCase("true")) return new DurationOrBoolean(true);
        if (s.equalsIgnoreCase("false")) return new DurationOrBoolean(false);
        return new DurationOrBoolean(Duration.parse(s));
    }

    /** Takes true as forver, false as zero, otherwise returns the duration (or null) */
    public Duration asDuration() {
        return durationSupplied != null ? durationSupplied :
                booleanSupplied == null ? null :
                        booleanSupplied ? Duration.PRACTICALLY_FOREVER : Duration.ZERO;
    }

    final Duration durationSupplied;
    final Boolean booleanSupplied;

    public Duration getDurationSupplied() {
        return durationSupplied;
    }

    public Boolean getBooleanSupplied() {
        return booleanSupplied;
    }

}
