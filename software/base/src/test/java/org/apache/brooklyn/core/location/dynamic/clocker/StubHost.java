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
package org.apache.brooklyn.core.location.dynamic.clocker;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.location.dynamic.LocationOwner;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;

@ImplementedBy(StubHostImpl.class)
public interface StubHost extends EmptySoftwareProcess, LocationOwner<StubHostLocation, StubHost> {
    AttributeSensorAndConfigKey<StubInfrastructure, StubInfrastructure> DOCKER_INFRASTRUCTURE = StubAttributes.DOCKER_INFRASTRUCTURE;
    
    AttributeSensor<DynamicCluster> DOCKER_CONTAINER_CLUSTER = Sensors.newSensor(DynamicCluster.class,
            "docker.container.cluster", "The cluster of Docker containers");

    StubInfrastructure getInfrastructure();
    DynamicCluster getDockerContainerCluster();
}