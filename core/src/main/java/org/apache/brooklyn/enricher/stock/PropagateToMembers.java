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
package org.apache.brooklyn.enricher.stock;

import com.google.common.base.Predicates;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.trait.Changeable;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;

/**
 * Whenever a sensor is published here, the value is propagated to all members.
 * This will also push all propagated sensors to any members who join (after the sensors were published here).
 */
@Catalog(name = "PropagateToMembers", description = "Propagates sensors from entity to members")
public class PropagateToMembers extends AbstractEnricher implements SensorEventListener<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(PropagateToMembers.class);

    @SetFromFlag("propagating")
    public static final ConfigKey<Collection<? extends Sensor<?>>> PROPAGATING = ConfigKeys.builder(new TypeToken<Collection<? extends Sensor<?>>>() {})
            .name("enricher.propagating.inclusions")
            .description("Collection of sensors to propagate to members")
            .constraint(Predicates.and(Predicates.notNull(), CollectionFunctionals.notEmpty()))
            .build();

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        if (!(entity instanceof Group)) {
            throw new IllegalStateException("Only valid for groups");
        }

        subscriptions().subscribe(entity, Changeable.MEMBER_ADDED, new PropagationSubscriber(this));

        getConfig(PROPAGATING).forEach(sensor ->  subscriptions().subscribe(entity, sensor, this));
    }

    static class PropagationSubscriber implements SensorEventListener<Entity> {
        private final PropagateToMembers adjunct;

        public PropagationSubscriber(PropagateToMembers adjunct) {
            this.adjunct = adjunct;
        }

        @Override
        public void onEvent(SensorEvent<Entity> event) {
            LOG.debug("Propagating '{}' to a new member '{}' ", adjunct.getConfig(PROPAGATING), event.getValue());
            adjunct.getConfig(PROPAGATING).forEach(sensor -> {
                AttributeSensor attributeSensor = (AttributeSensor<?>)sensor;
                Object value = adjunct.entity.getAttribute(attributeSensor);
                if (!Objects.isNull(value)) {
                    LOG.debug("Propagating initial {}: {}", attributeSensor, value);
                    event.getValue().sensors().set(attributeSensor, value);
                }
            });
        }
    }

    @Override
    public void onEvent(SensorEvent<Object> event) {
        if (getConfig(PROPAGATING).contains(event.getSensor())) {
            LOG.debug("Propagating updated {}: {}", event.getSensor(), event.getValue());
            ((Group) entity).getMembers().forEach(member -> {
                if (!Objects.isNull(event.getValue())) {
                    member.sensors().set((AttributeSensor<? super Object>) event.getSensor(), event.getValue());
                }
            });
        }
    }
}
