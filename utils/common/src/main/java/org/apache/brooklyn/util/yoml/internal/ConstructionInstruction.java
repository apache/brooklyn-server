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
package org.apache.brooklyn.util.yoml.internal;

import java.util.List;

import org.apache.brooklyn.util.guava.Maybe;

/** Capture instructions on how an object should be created.
 * <p>
 * This is used when we need information from an outer instance definition in order to construct the object.
 * (The default pathway is to use a no-arg constructor and to apply the outer definitions to the instance.
 * But that doesn't necessarily work if a specific constructor or static method is being expected.
 * It gets more complicated if the outer instance is overwriting information from an inner instance,
 * but it is the inner instance which actually defines the java type to instantiate ... and that isn't so uncommon!)  
 */
public interface ConstructionInstruction {
    
    public Class<?> getType();
    public List<?> getArgs();
    public Maybe<Object> create();
    
    public ConstructionInstruction getOuterInstruction();
    
}
