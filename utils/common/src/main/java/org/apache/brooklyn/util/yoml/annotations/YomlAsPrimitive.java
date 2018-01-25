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
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypePrimitive;

/** 
 * Indicates that a class is potentially coercible to and from a primitive.
 * YOML will always try to coerce from a primitive if appropriate,
 * but this indicates that it should try various strategies to coerce to a primitive.
 * <p>
 * See {@link InstantiateTypePrimitive}.
 */
@Retention(RUNTIME)
@Target({ TYPE })
public @interface YomlAsPrimitive {
    
    /** The key to insert for the given value */
    String keyToInsert() default ConvertFromPrimitive.DEFAULT_DEFAULT_KEY;
    
    DefaultKeyValue[] defaults() default {};
    
}
