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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/** 
 * Indicates that the type of a field should be taken at runtime from another field.
 * This is useful to allow a user to specify e.g. <code>{ objType: int, obj: 3 }</code>
 * rather than requiring <code>{ obj: { type: int, value: e } }</code>.
 * <p>
 * By default the other field is assumed to exist but that need not be the case. 
 */
@Retention(RUNTIME)
@Target(FIELD)
public @interface YomlTypeFromOtherField {
    
    /** The other field which will supply the type. This must point at the field name or the config key name,
     * not any alias, although aliases can be used in the YAML when specifying the type. */
    String value();
    
    /** Whether the other field is real in the java object,
     * ie present as a field or config key, and so should be serialized/deserialized normally;
     * if false it will be created on writing to yaml and deleted while reading,
     * ie not reflected in the java object */
    boolean real() default true;
    
}
