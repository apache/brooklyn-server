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
package org.apache.brooklyn.location.jclouds;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

import org.jclouds.compute.ComputeService;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.entity.stock.ConditionalEntity;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.time.Duration;

/**
 * Location customizer that optionally creates and starts a child entity.
 * <p>
 * Use this class when the child entity should be created and started
 * <em>before</em> the parent entity has begun executing its lifecycle
 * methods. This is useful for supporting agent software installation
 * or machine configuration tasks that are too complex to be defined
 * in a singe {@code pre.install.command} script.
 * <p>
 * The {@link #CREATE_CHILD_ENTITY} configuration key can be set to
 * false to disable creation of the child entity.
 * 
 * @see {@link ConditionalEntity} for an alternative mechanism
 */
public class ChildEntityCustomizer extends BasicJcloudsLocationCustomizer {

    private static final Logger LOG = LoggerFactory.getLogger(ChildEntityCustomizer.class);

    @SetFromFlag("childSpec")
    public static final ConfigKey<EntitySpec<?>> CHILD_ENTITY_SPEC = ConfigKeys.newConfigKey(new TypeToken<EntitySpec<?>>() { },
            "child.entitySpec", "The specification for the child entity to be created");

    @SetFromFlag("create")
    public static final ConfigKey<Boolean> CREATE_CHILD_ENTITY = ConfigKeys.newBooleanConfigKey(
            "child.create", "Whether the child entity should be created",
            Boolean.TRUE);

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
        EntitySpec<?> spec = config().get(CHILD_ENTITY_SPEC);
        Boolean create = config().get(CREATE_CHILD_ENTITY);
        Duration timeout = config().get(BrooklynConfigKeys.START_TIMEOUT);
        Entity parent = getCallerContext(machine);

        if (Boolean.TRUE.equals(create) && spec != null) {
            LOG.info("Creating child entity for {} in {}", parent, machine);
            Entity child = getBrooklynManagementContext().getEntityManager().createEntity(spec);
            child.setParent(parent);
            Task<Void> start = Entities.invokeEffectorWithArgs(parent, child, Startable.START, ImmutableList.of(machine));
            if (!start.blockUntilEnded(timeout)) {
                throw new IllegalStateException(String.format("Timed out while starting child entity for %s", parent));
            }
        }
    }
}