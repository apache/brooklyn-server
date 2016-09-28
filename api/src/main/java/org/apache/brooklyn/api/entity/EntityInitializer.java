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
package org.apache.brooklyn.api.entity;

import java.util.Map;

import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlSingletonMap;

/** 
 * Instances of this class supply logic which can be used to initialize entities. 
 * These can be added to an {@link EntitySpec} programmatically, or declared as part
 * of YAML recipes in a <code>brooklyn.initializers</code> section.
 * <p>
 * When intended for use from YAML, implementing classes should define 
 * a single-argument constructor taking either a {@link Map} or a ConfigBag 
 * so that YAML parameters can be supplied
 * (or a no-arg constructor if parameters are not supported).
 * <p>
 * Note that initializers are only invoked on first creation; they are not called 
 * during a rebind. Instead, the typical pattern is that initializers will create
 * {@link EntityAdjunct} instances such as {@link Policy} and {@link Feed}
 * which will be attached during rebind.
 **/ 
@YomlSingletonMap(keyForPrimitiveValue="type")
@Alias("entity-initializer")
public interface EntityInitializer {
    
    /** Applies initialization logic to a just-built entity.
     * Invoked immediately after the "init" call on the AbstractEntity constructed.
     * 
     * @param entity guaranteed to be the actual implementation instance, 
     * thus guaranteed to be castable to EntityInternal which is often desired,
     * or to the type at hand (it is not even a proxy)
     */
    public void apply(EntityLocal entity);
    
}
