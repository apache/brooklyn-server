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
package org.apache.brooklyn.util.javalang.coerce;

import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;

/**
 * A coercer that can be registered, which will try to coerce the given input to the given type.
 * 
 * This can be used for "generic" coercers, such as those that look for a {@code fromValue()} 
 * method on the target type.
 */
@Beta
public interface TryCoercer {

    /**
     * The meaning of the return value is:
     * <ul>
     *   <li>null - no errors, recommend continue with fallbacks (i.e. not found).
     *   <li>absent - had some kind of exception, recommend continue with fallbacks (but can report this error if
     *       other fallbacks fail).
     *   <li>present - coercion successful.
     * </ul>
     */
    <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type);
}
