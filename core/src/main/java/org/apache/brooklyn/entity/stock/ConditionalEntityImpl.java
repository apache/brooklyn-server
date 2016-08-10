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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.enricher.stock.Enrichers;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;

public class ConditionalEntityImpl extends BasicStartableImpl implements ConditionalEntity {

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

            Entity child = sensors().get(CONDITIONAL_ENTITY);
            EntitySpec<?> spec = config().get(CONDITIONAL_ENTITY_SPEC);
            Boolean create = config().get(CREATE_CONDITIONAL_ENTITY);
            Boolean propagate = config().get(PROPAGATE_CONDITIONAL_ENTITY_SENSORS);
            Set<AttributeSensor<?>> sensors = MutableSet.copyOf(config().get(CONDITIONAL_ENTITY_SENSOR_LIST));
            Duration timeout = config().get(BrooklynConfigKeys.START_TIMEOUT);

            addLocations(locations);
            locations = Locations.getLocationsCheckingAncestors(locations, this);
            LOG.info("Starting entity {}: {}", this, locations);

            // Child not yet created; Entity spec is present; Create flag is true
            if (child == null && spec != null && Boolean.TRUE.equals(create)) {
                child = addChild(EntitySpec.create(spec));
                sensors().set(CONDITIONAL_ENTITY, child);

                // Add enrichers for sensor propagateion
                enrichers().add(Enrichers.builder().propagating(Startable.SERVICE_UP).from(child).build());
                if (Boolean.TRUE.equals(propagate)) {
                    if (sensors.isEmpty()) {
                        enrichers().add(Enrichers.builder().propagatingAllButUsualAnd().from(child).build());
                    } else {
                        enrichers().add(Enrichers.builder().propagating(sensors).from(child).build());
                    }
                }
            }

            // Start child if create flag is set; otherwise just set service.isUp
            if (Boolean.TRUE.equals(create)) {
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

