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

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

public class StubUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StubUtils.class);

    /** Do not instantiate. */
    private StubUtils() { }

    /*
     * Configuration and constants.
     */

    public static final Predicate<Entity> sameInfrastructure(Entity entity) {
        Preconditions.checkNotNull(entity, "entity");
        return new SameInfrastructurePredicate(entity.getId());
    }

    public static class SameInfrastructurePredicate implements Predicate<Entity> {

        private final String id;

        public SameInfrastructurePredicate(String id) {
            this.id = Preconditions.checkNotNull(id, "id");
        }

        @Override
        public boolean apply(@Nullable Entity input) {
            // Check if entity is deployed to a DockerContainerLocation
            Optional<Location> lookup = Iterables.tryFind(input.getLocations(), Predicates.instanceOf(StubContainerLocation.class));
            if (lookup.isPresent()) {
                StubContainerLocation container = (StubContainerLocation) lookup.get();
                // Only containers that are part of this infrastructure
                return id.equals(container.getOwner().getDockerHost().getInfrastructure().getId());
            } else {
                return false;
            }
        }
    };

    public static final ManagementContext.PropertiesReloadListener reloadLocationListener(ManagementContext context, LocationDefinition definition) {
        return new ReloadDockerLocation(context, definition);
    }

    public static class ReloadDockerLocation implements ManagementContext.PropertiesReloadListener {

        private final ManagementContext context;
        private final LocationDefinition definition;

        public ReloadDockerLocation(ManagementContext context, LocationDefinition definition) {
            this.context = Preconditions.checkNotNull(context, "context");
            this.definition = Preconditions.checkNotNull(definition, "definition");
        }

        @Override
        public void reloaded() {
            Location resolved = context.getLocationRegistry().resolve(definition);
            context.getLocationRegistry().updateDefinedLocation(definition);
            context.getLocationManager().manage(resolved);
        }
    };
}
