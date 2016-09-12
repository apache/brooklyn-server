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
package org.apache.brooklyn.camp.yoml;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.apache.brooklyn.util.yoml.annotations.YomlConstructorConfigMap;

/** 
 * Indicates that a class should be yoml-serialized using a one-arg constructor taking a map or bag of config.
 * Similar to {@link YomlConstructorConfigMap} but accepting config-bag constructors
 * and defaulting to `brooklyn.config` as the key for unknown config.
 */
@Retention(RUNTIME)
@Target({ TYPE })
public @interface YomlConstructorConfigBag {
    /** YOML needs to know which field contains the config at serialization time. */
    String value();
    /** By default here reads/writes unrecognised key values against `brooklyn.config`. */
    String writeAsKey() default "brooklyn.config";
    
    /** Validate that a suitable field and constructor exist, failing fast if not */
    boolean validateAheadOfTime() default true;
    
    /** Skip if there are no declared config keys (default false) */
    boolean requireStaticKeys() default false;
}
