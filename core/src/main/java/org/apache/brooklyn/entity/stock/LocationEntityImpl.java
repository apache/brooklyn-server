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
package org.apache.brooklyn.entity.stock;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.enricher.stock.Enrichers;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationEntityImpl extends BasicStartableImpl implements LocationEntity {

    private static final Logger LOG = LoggerFactory.getLogger(BasicStartableImpl.class);

    @Override
    public void start(Collection<? extends Location> locations) {
        try {
            ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
            sensors().set(Attributes.SERVICE_UP, false);

            // Block startup until other dependent components are available
            Object val = config().get(START_LATCH);
            if (val != null) {
                LOG.debug("Finished waiting for start-latch in {}", this);
            }

            Entity child = sensors().get(LOCATION_ENTITY);
            Map<String,EntitySpec<?>> specMap = MutableMap.copyOf(config().get(LOCATION_ENTITY_SPEC_MAP));

            Boolean propagate = config().get(PROPAGATE_LOCATION_ENTITY_SENSORS);
            Set<AttributeSensor<?>> sensors = MutableSet.copyOf(config().get(LOCATION_ENTITY_SENSOR_LIST));
            Duration timeout = config().get(BrooklynConfigKeys.START_TIMEOUT);

            addLocations(locations);
            locations = Locations.getLocationsCheckingAncestors(locations, this);
            LOG.info("Starting entity {}: {}", this, locations);

            // Child not yet created and Entity spec is available
            if (child == null && specMap.size() > 0) {
                // Determine provisioning location
                MachineProvisioningLocation<?> provisioner = null;
                Maybe<? extends Location> location = Machines.findUniqueElement(locations, MachineLocation.class);
                if (location.isPresent()) {
                    Location parent = location.get().getParent();
                    while (parent != null && ! (parent instanceof MachineProvisioningLocation)) {
                        parent = parent.getParent();
                    }
                    provisioner = (MachineProvisioningLocation<?>) parent;
                } else {
                    location = Machines.findUniqueElement(locations, MachineProvisioningLocation.class);
                    if (location.isPresent()) {
                        provisioner = (MachineProvisioningLocation<?>) location.get();
                    }
                }
                if (provisioner == null) {
                    throw new IllegalStateException("Cannot determine provisioning location for entity: " + this);
                }

                // Determine which entity spec to use
                String locationType = provisioner.getClass().getSimpleName();
                String provider = provisioner.config().get(LocationConfigKeys.CLOUD_PROVIDER);
                Set<String> countryCodes = MutableSet.copyOf(provisioner.config().get(LocationConfigKeys.ISO_3166));
                EntitySpec<?> spec = specMap.get(DEFAULT);
                if (specMap.containsKey(locationType)) {
                    LOG.debug("Matched location type {} for entity: {}", locationType, this);
                    spec = specMap.get(locationType);
                } else if (specMap.containsKey(provider)) {
                    LOG.debug("Matched provider {} for entity: {}", provider, this);
                    spec = specMap.get(provider);
                } else for (String country : countryCodes) {
                    if (specMap.containsKey(country)) {
                        LOG.debug("Matched country code {} for entity: {}", country, this);
                        spec = specMap.get(country);
                    }
                }

                // Instantiate spec as child entity if set
                if (spec != null) {
                    child = addChild(EntitySpec.create(spec));

                    sensors().set(LOCATION_ENTITY, child);

                    // Add enrichers for sensor propagation
                    enrichers().add(Enrichers.builder().propagating(Startable.SERVICE_UP).from(child).build());
                    enrichers().add(ServiceStateLogic.newEnricherFromChildrenState()
                            .suppressDuplicates(true)
                            .configure(ComputeServiceIndicatorsFromChildrenAndMembers.RUNNING_QUORUM_CHECK, QuorumCheck.QuorumChecks.all()));
                    if (Boolean.TRUE.equals(propagate)) {
                        if (sensors.isEmpty()) {
                            enrichers().add(Enrichers.builder().propagatingAllButUsualAnd().from(child).build());
                        } else {
                            enrichers().add(Enrichers.builder().propagating(sensors).from(child).build());
                        }
                    }
                }
            }

            // Start child if present; otherwise just set service.isUp
            if (child != null) {
                LOG.info("Starting child {}: {}", child, locations);
                if (Entities.invokeEffectorWithArgs(this, child, Startable.START, locations).blockUntilEnded(timeout)) {
                    LOG.debug("Successfully started {} by {}", child, this);
                } else {
                    throw new IllegalStateException(String.format("Timed out while %s was starting %s", this, child));
                }
            } else {
                LOG.debug("No child created, setting SERVICE_UP to true");
                sensors().set(Attributes.SERVICE_UP, true);
            }

            ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
        } catch (Throwable t) {
            ServiceStateLogic.setExpectedState(this, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }
}

