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
package org.apache.brooklyn.entity.group;

import org.apache.brooklyn.api.catalog.CatalogConfig;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

/**
 * When a dynamic fabric is started, it starts an entity in each of its locations. 
 * This entity will be the parent of each of the started entities. 
 */
@ImplementedBy(DynamicFabricImpl.class)
@SuppressWarnings("serial")
public interface DynamicFabric extends AbstractGroup, Startable, Fabric {

    ConfigKey<Boolean> INCLUDE_INITIAL_CHILDREN = ConfigKeys.newBooleanConfigKey(
            "includeInitialChildren",
            "Whether to include the initial children when handing out the locations to new members "
                    + "(i.e. if true, these children will each be given a location (round-robin), "
                    + "rather than creating new members; if false then the children will be given "
                    + "just the first location (if any), and new members will be created dynamically "
                    + "for each given location).",
            true);
    
    @CatalogConfig(label = "Member spec")
    @SetFromFlag("memberSpec")
    ConfigKey<EntitySpec<?>> MEMBER_SPEC = ConfigKeys.newConfigKey(
            new TypeToken<EntitySpec<?>>() {}, 
            "dynamiccfabric.memberspec", 
            "Entity spec for creating new members (one per location)", 
            null);

    @SetFromFlag("firstMemberSpec")
    ConfigKey<EntitySpec<?>> FIRST_MEMBER_SPEC = ConfigKeys.newConfigKey(
            new TypeToken<EntitySpec<?>>() {}, 
            "dynamiccfabric.firstmemberspec", 
            "Entity spec for the first member", 
            null);

    @SetFromFlag("displayNamePrefix")
    ConfigKey<String> DISPLAY_NAME_PREFIX = ConfigKeys.newStringConfigKey(
            "dynamicfabric.displayNamePrefix", 
            "Display name prefix, for created children");

    @SetFromFlag("displayNameSuffix")
    ConfigKey<String> DISPLAY_NAME_SUFFIX = ConfigKeys.newStringConfigKey(
            "dynamicfabric.displayNameSuffix", 
            "Display name suffix, for created children");

    @SetFromFlag("customChildFlags")
    MapConfigKey<Object> CUSTOM_CHILD_FLAGS = new MapConfigKey<Object>(
            Object.class, 
            "dynamicfabric.customChildFlags", 
            "Additional flags to be passed to children when they are being created", 
            ImmutableMap.<String,Object>of());

    AttributeSensor<Integer> FABRIC_SIZE = Sensors.newIntegerSensor("fabric.size", "Fabric size");
    
    AttributeSensor<Lifecycle> SERVICE_STATE_ACTUAL = Attributes.SERVICE_STATE_ACTUAL;

    public void setMemberSpec(EntitySpec<?> memberSpec);
    
    public Integer getFabricSize();

}
