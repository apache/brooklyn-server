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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.brooklyn.util.core.task.DeferredSupplier;

public class DslUtils {

    /** true iff none of the args are deferred / tasks */
    public static boolean resolved(final Object... args) {
        if (args == null) return true;
        return resolved(Arrays.asList(args));
    }
    
    /** true iff none of the args are deferred / tasks */
    public static boolean resolved(Iterable<?> args) {
        if (args == null) return true;
        boolean allResolved = true;
        for (Object arg: args) {
            if (arg instanceof DeferredSupplier<?>) {
                allResolved = false;
                break;
            } else if (arg instanceof Collection) {
                if (!resolved((Collection<?>)arg)) {
                    allResolved = false;
                    break;
                }
            } else if (arg instanceof Map) {
                if (!(resolved(((Map<?,?>)arg).keySet()) && resolved(((Map<?,?>)arg).values()))) {
                    allResolved = false;
                    break;
                }
            }
        }
        return allResolved;
    }
}
