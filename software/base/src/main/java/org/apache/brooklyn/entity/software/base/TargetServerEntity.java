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
package org.apache.brooklyn.entity.software.base;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

/**
 * An entity that implements the same behaviour as {@link VanillaSoftwareProcess}
 * using the same target location as another entity. This allows more complex
 * sequencing of events, where parent-child relationships and latching do not
 * allow the desired order.
 * <p>
 * The following blueprint starts entity {@code initial} with the {@code first.sh}
 * script, followed by the load balancer entity. Then, the {@code target} entity
 * will start, due to the {@code launch.latch} setting, using the same location as
 * {@code initial}, causing the {@code second.sh} script to now be executed on that
 * server.
 * <pre>
 * services:
 *   - type: vanilla-software-process
 *     id: initial
 *     brooklyn.config:
 *       launch.command: "first.sh"
 *   - type: load-balancer
 *     id: load-balancer
 *     brooklyn.config:
 *       launch.latch:
 *         $brooklyn:entity("initial").attributeWhenRead("service.isUp")
 *   - type: target-server-entity
 *     id: target
 *     brooklyn.config:
 *       target.entity:
 *         $brooklyn:sibling("initial")
 *       launch.command: "second.sh"
 *       launch.latch:
 *         $brooklyn:entity("nginx").attributeWhenRead("service.isUp")
 * </pre>
 */
@ImplementedBy(TargetServerEntityImpl.class)
@SuppressWarnings("serial")
public interface TargetServerEntity extends VanillaSoftwareProcess {

    @SetFromFlag("target")
    AttributeSensorAndConfigKey<Entity, Entity> TARGET_ENTITY = ConfigKeys.newSensorAndConfigKey(Entity.class,
            "target.entity", "The target entity whose location is to be used");

}