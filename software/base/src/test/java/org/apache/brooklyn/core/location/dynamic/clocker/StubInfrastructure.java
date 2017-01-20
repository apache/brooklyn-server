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

import java.util.List;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.dynamic.LocationOwner;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.group.DynamicGroup;
import org.apache.brooklyn.entity.group.DynamicMultiGroup;

/**
 * These entities and locations mirror the structure used in Clocker v1 (prepend everything 
 * with "Docker" instead of "Stub"). The purpose of repeating it here is to ensure that the 
 * functionality Clocker is relying on will get regularly tested and kept working - or if it 
 * does change, then Clocker can be updated as appropriate.
 * 
 * The Clocker hierarchy has the following structure:
 * 
 * <pre>
 * Infrastructure -----------> InfrastructureLocation
 *       |                               |
 *      Host      ----------->      HostLocation
 *       |                               |
 *    Container   ----------->   ContainerLocation
 * <pre>
 * 
 * The Infrastructure, Host and Container are all entities that implement {@link LocationOwner}. 
 * Whenever one of these is started, it creates the corresponding Location object (shown on the
 * right).
 * 
 * The Infrastructure has a cluster of Host instances. A host has a cluster of Container 
 * instances.
 * 
 * When the {@code Infrastructure} is initially provisioned, it registers the newly create 
 * location in the {@link ManagementContext#getLocationRegistry()}. This makes it a named 
 * location, which can be used by other apps.
 * 
 * When {@code InfrastructureLocation.obtain()) is called, it will create a container somewhere in 
 * the cluster. The following chain of events happens:
 * <ol>
 *   <li>From the {@code Infrastructure} (i.e. the location's "owner"), get the set of 
 *       {@code Host}s and thus the set of {@code HostLocation}s.
 *   <li>Choose a {@code HostLocation}
 *       (if there's not one, consider scaling up the {@code Infrastructure}'s cluster, if permitted)
 *   <li>Delegate to {@code DockerHostLocation.obtain()}, and return the result.
 *   <li>The {@code DockerHostLocation} first does some fiddly Docker stuff around images
 *       (not represented in this stub structure).
 *   <li>Get the {@code dockerHost.getDockerContainerCluster()}, and use that to create a new 
 *       {@code Container} entity.
 *   <li>Invoke {@code start} effector on the {@code Container} entity
 *   <li>The {code Container.start} calls {@code Container.createLocation}, which instantiates
 *       the actual Docker container (by using the jclouds provider).
 *   <li>Return the {@code container.getDynamicLocation()} (i.e. the {@code ContainerLocation}).
 * <ul>
 */
@ImplementedBy(StubInfrastructureImpl.class)
public interface StubInfrastructure extends Application, Startable, LocationOwner<StubInfrastructureLocation, StubInfrastructure> {
    AttributeSensorAndConfigKey<String, String> LOCATION_NAME = ConfigKeys.newSensorAndConfigKeyWithDefault(LocationOwner.LOCATION_NAME, "my-stub-cloud");
    ConfigKey<Integer> DOCKER_HOST_CLUSTER_MIN_SIZE = ConfigKeys.newConfigKeyWithPrefix("docker.host.", DynamicCluster.INITIAL_SIZE);

    AttributeSensor<DynamicCluster> DOCKER_HOST_CLUSTER = Sensors.newSensor(DynamicCluster.class, "docker.hosts", "Docker host cluster");
    AttributeSensor<DynamicGroup> DOCKER_CONTAINER_FABRIC = Sensors.newSensor(DynamicGroup.class, "docker.fabric", "Docker container fabric");
    AttributeSensor<DynamicMultiGroup> DOCKER_APPLICATIONS = Sensors.newSensor(DynamicMultiGroup.class, "docker.buckets", "Docker applications");
    
    List<Entity> getStubHostList();
    DynamicCluster getStubHostCluster();
}