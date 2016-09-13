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
package org.apache.brooklyn.util.yoml.annotations;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.apache.brooklyn.util.yoml.serializers.ConvertSingletonMap;

/** 
 * Indicates that a class can be yoml-serialized as a map with a single key,
 * where the single-key map is converted to a bigger map such that 
 * the value of that single key is set under as one key ({@link #keyForKey()}), 
 * and the value either merged (if a map and {@link #keyForMapValue()} is blank) 
 * or placed under a different key ({@link #keyForAnyValue()} and other value keys).
 * <p>
 * Default values (<code>.key</code> and <code>.value</code>) are intended for
 * use by other serializers.
 * <p>
 * See {@link ConvertSingletonMap}.
 */
@Retention(RUNTIME)
@Target({ TYPE })
public @interface YomlSingletonMap {
    /** The single key is taken as a value against the key name given here. */
    String keyForKey() default ConvertSingletonMap.DEFAULT_KEY_FOR_KEY;
    
    /** If value is a primitive or string, place under a key,
     * the name of which is given here. */
    String keyForPrimitiveValue() default "";
    /** If value is a list, place under a key,
     * the name of which is given here. */
    String keyForListValue() default "";
    /** If value is a map, place under a key,
     * the name of which is given here. */
    String keyForMapValue() default "";
    /** Any value (including a map) is placed in a different key,
     * the name of which is given here, 
     * but after more specific types are applied. */
    String keyForAnyValue() default ConvertSingletonMap.DEFAULT_KEY_FOR_VALUE;

    DefaultKeyValue[] defaults() default {};
    
}
