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

import org.apache.brooklyn.util.yoml.serializers.ConvertFromPrimitive;

/** 
 * Indicates that a class can be yoml-serialized as a primitive
 * if there is just a single key to set.
 * <p>
 * Default value <code>.value</code> is intended for use by other serializers.
 * <p>
 * See {@link ConvertFromPrimitive}.
 */
@Retention(RUNTIME)
@Target({ TYPE })
public @interface YomlFromPrimitive {
    
    /** The key to insert for the given value */
    String keyToInsert() default ConvertFromPrimitive.DEFAULT_DEFAULT_KEY;
    
    DefaultKeyValue[] defaults() default {};
    
}
