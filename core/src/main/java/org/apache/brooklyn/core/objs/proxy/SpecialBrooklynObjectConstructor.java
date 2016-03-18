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
package org.apache.brooklyn.core.objs.proxy;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;

/** Marker interface for use when a spec needs something special to create the object under management. 
 * Set the {@link Config#SPECIAL_CONSTRUCTOR} config key in the spec pointing to the class;
 * it will be instantiated and {@link #create(Class, AbstractBrooklynObjectSpec)} invoked to create the class.
 * The type of course must be consistent with the spec.
 * <p>
 * This entire mechanism is beta; it is introduced for a few special cases where some type of dynamicism is currently required.
 * In most cases the {@link AbstractBrooklynObjectSpec} should correspond directly to the object created.
 * Some use cases are:
 * <li>PortForwarderManager, shared global record
 * <li>Choosing a different entity type smartly based on the location (e.g. deploying to PaaS vs IaaS) 
 * <li>Referencing an existing location (optionally, and there are MAJOR issues with this in general; see below)
 * <li>Clocker (?) - newly defined docker locations (although these may all point at entities)
 * <p>
 * See implementations of this interface.
 * <p>
 * Note that the caller may typically do additional configuration on the resulting object,
 * such as setting a catalog item ID etc. That behaviour may merit adjustment and is subject to change.
 * <p>
 * Where this returns an existing item we must be VERY careful around *unmanaging*,
 * both when this reference is unmanaged (but the original is still in use) 
 * and when the target is unmanaged (but this reference is still in use).
 * <p>
 * Some possible improvements to the simple approach here are:
 * <li>Return a delegate impl of the {@link Location} or {@link Entity} interface pointing at the target.
 *     (This would work fine for sensors/effectors but it would fail any java type checks or casts.
 *     We could switch to a brooklyn-managed type system, which would also allow for explicit yaml inheritance,
 *     but that's not a small task.)
 * <li>Return a java reflections proxy impl (we have this for entity already;
 *     but for many Location types interfaces are not currently defined;
 *     adding them would be nice to make Locations more like Entities)
 * <li>Require the appropriate {@link Entity} or {@link Location} type, on a case-by-case basis,
 *     where the lifecycle is as normal, and the logic for initializing and delegating is done 
 *     in the init.  (Possibly this is better than the current implementation!?  It is certainly
 *     simpler than allowing multiple links to existing items, and would be feasible in many
 *     cases, e.g. a `same-machine-as` type (assuming we made SshMachineEntity an interface);
 *     but it would not allow choosing an entity type smartly based on location.)
 */
@Beta
public interface SpecialBrooklynObjectConstructor {
    
    /** Method to implement to create the given type from the given spec. */
    public <T> T create(ManagementContext mgmt, Class<T> type, AbstractBrooklynObjectSpec<?,?> spec);
    
    public static class Config {
        @Beta @SuppressWarnings("serial")
        public static ConfigKey<Class<? extends SpecialBrooklynObjectConstructor>> SPECIAL_CONSTRUCTOR =
            ConfigKeys.newConfigKey(new TypeToken<Class<? extends SpecialBrooklynObjectConstructor>>() {}, "brooklyn.object.constructor.special");
    }
    
}
