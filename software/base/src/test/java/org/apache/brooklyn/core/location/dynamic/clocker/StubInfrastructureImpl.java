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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.AbstractApplication;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.feed.ConfigToAttributes;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.location.dynamic.LocationOwner;
import org.apache.brooklyn.entity.group.BasicGroup;
import org.apache.brooklyn.entity.group.Cluster;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.group.DynamicGroup;
import org.apache.brooklyn.entity.group.DynamicMultiGroup;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.QuorumCheck.QuorumChecks;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class StubInfrastructureImpl extends AbstractApplication implements StubInfrastructure {

    private static final Logger LOG = LoggerFactory.getLogger(StubInfrastructureImpl.class);

    @Override
    public void init() {
        super.init();
        
        ConfigToAttributes.apply(this);

        EntitySpec<?> dockerHostSpec = EntitySpec.create(StubHost.class)
                .configure(StubHost.DOCKER_INFRASTRUCTURE, this)
                .configure(SoftwareProcess.CHILDREN_STARTABLE_MODE, SoftwareProcess.ChildStartableMode.BACKGROUND_LATE);

        DynamicCluster hosts = addChild(EntitySpec.create(DynamicCluster.class)
                .configure(Cluster.INITIAL_SIZE, config().get(DOCKER_HOST_CLUSTER_MIN_SIZE))
                .configure(DynamicCluster.QUARANTINE_FAILED_ENTITIES, true)
                .configure(DynamicCluster.MEMBER_SPEC, dockerHostSpec)
                .configure(DynamicCluster.RUNNING_QUORUM_CHECK, QuorumChecks.atLeastOneUnlessEmpty())
                .configure(DynamicCluster.UP_QUORUM_CHECK, QuorumChecks.atLeastOneUnlessEmpty())
                .displayName("Docker Hosts"));

        DynamicGroup fabric = addChild(EntitySpec.create(DynamicGroup.class)
                .configure(DynamicGroup.ENTITY_FILTER, Predicates.and(Predicates.instanceOf(StubContainer.class), EntityPredicates.attributeEqualTo(StubContainer.DOCKER_INFRASTRUCTURE, this)))
                .configure(DynamicGroup.MEMBER_DELEGATE_CHILDREN, true)
                .displayName("All Docker Containers"));

        DynamicMultiGroup buckets = addChild(EntitySpec.create(DynamicMultiGroup.class)
                .configure(DynamicMultiGroup.ENTITY_FILTER, StubUtils.sameInfrastructure(this))
                .configure(DynamicMultiGroup.RESCAN_INTERVAL, 15L)
                .configure(DynamicMultiGroup.BUCKET_FUNCTION, new Function<Entity, String>() {
                    @Override
                    public String apply(@Nullable Entity input) {
                        return input.getApplication().getDisplayName() + ":" + input.getApplicationId();
                    }
                })
                .configure(DynamicMultiGroup.BUCKET_SPEC, EntitySpec.create(BasicGroup.class)
                        .configure(BasicGroup.MEMBER_DELEGATE_CHILDREN, true))
                .displayName("Docker Applications"));

        sensors().set(DOCKER_HOST_CLUSTER, hosts);
        sensors().set(DOCKER_CONTAINER_FABRIC, fabric);
        sensors().set(DOCKER_APPLICATIONS, buckets);
    }
    
    @Override
    public StubInfrastructureLocation getDynamicLocation() {
        return (StubInfrastructureLocation) sensors().get(DYNAMIC_LOCATION);
    }

    @Override
    public void doStart(Collection<? extends Location> locations) {
        // TODO support multiple locations
        sensors().set(SERVICE_UP, Boolean.FALSE);

        Location provisioner = Iterables.getOnlyElement(locations);
        LOG.info("Creating new DockerLocation wrapping {}", provisioner);

        Map<String, ?> flags = MutableMap.<String, Object>builder()
                .putAll(config().get(LOCATION_FLAGS))
                .put("provisioner", provisioner)
                .build();
        createLocation(flags);

        super.doStart(locations);

        sensors().set(SERVICE_UP, Boolean.TRUE);
    }

    /**
     * De-register our {@link DockerLocation} and its children.
     */
    @Override
    public void stop() {
        setExpectedStateAndRecordLifecycleEvent(Lifecycle.STOPPING);
        sensors().set(SERVICE_UP, Boolean.FALSE);

        // Find all applications and stop, blocking for up to five minutes until ended
        try {
            Iterable<Entity> entities = Iterables.filter(getManagementContext().getEntityManager().getEntities(), StubUtils.sameInfrastructure(this));
            Set<Application> applications = ImmutableSet.copyOf(Iterables.transform(entities, new Function<Entity, Application>() {
                @Override
                public Application apply(Entity input) {
                    return input.getApplication();
                }
            }));
            LOG.debug("Stopping applications: {}", Iterables.toString(applications));
            Entities.invokeEffectorList(this, applications, Startable.STOP).get(Duration.THIRTY_SECONDS);
        } catch (Exception e) {
            LOG.warn("Error stopping applications", e);
        }

        // Stop all Docker hosts in parallel
        try {
            DynamicCluster hosts = sensors().get(DOCKER_HOST_CLUSTER);
            LOG.debug("Stopping hosts: {}", Iterables.toString(hosts.getMembers()));
            Entities.invokeEffectorList(this, hosts.getMembers(), Startable.STOP).get(Duration.THIRTY_SECONDS);
        } catch (Exception e) {
            LOG.warn("Error stopping hosts", e);
        }

        deleteLocation();

        // Stop anything else left over
        super.stop();
    }

    @Override
    public StubInfrastructureLocation createLocation(Map<String, ?> flags) {
        String locationName = config().get(LOCATION_NAME);
        if (Strings.isBlank(locationName)) {
            String prefix = config().get(LOCATION_NAME_PREFIX);
            String suffix = config().get(LOCATION_NAME_SUFFIX);
            locationName = Joiner.on("-").skipNulls().join(prefix, getId(), suffix);
        }

        StubInfrastructureLocation location = getManagementContext().getLocationManager().createLocation(LocationSpec.create(StubInfrastructureLocation.class)
                .displayName("Docker Infrastructure("+getId()+")")
                .configure(flags)
                .configure("owner", getProxy())
                .configure("locationName", locationName));
        
        LocationDefinition definition = location.register();

        sensors().set(LocationOwner.LOCATION_SPEC, definition.getSpec());
        sensors().set(LocationOwner.DYNAMIC_LOCATION, location);
        sensors().set(LocationOwner.LOCATION_NAME, locationName);
        
        return location;
    }

    @Override
    public boolean isLocationAvailable() {
        return getDynamicLocation() != null;
    }

    @Override
    public void deleteLocation() {
        StubInfrastructureLocation location = getDynamicLocation();

        if (location != null) {
            location.deregister();
            Locations.unmanage(location);
        }

        sensors().set(LocationOwner.DYNAMIC_LOCATION, null);
        sensors().set(LocationOwner.LOCATION_NAME, null);
    }

    @Override
    public List<Entity> getStubHostList() {
        if (getStubHostCluster() == null) {
            return ImmutableList.of();
        } else {
            return ImmutableList.copyOf(getStubHostCluster().getMembers());
        }
    }

    @Override
    public DynamicCluster getStubHostCluster() {
        return sensors().get(DOCKER_HOST_CLUSTER);
    }
}