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
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;

/**
 * An entity that creates an optional child, based on the provisioning location properties.
 * <p>
 * Can use the class name of the {@link MachineProvisioningLocation} or the value of the
 * {@code provider} or {@code iso3166} configuration keys on the provisioning location.
 * The values used to make the decision are set as sensors on this entity. The decision
 * is made based on the first match found, checking the class name first, then the
 * provider and finally the country codes, and map keys representing any or all of these
 * can be used at the same time.
 * <pre>
 * - type: org.apache.brooklyn.entity.stock.LocationEntity
 *   brooklyn.config:
 *     location.entity.spec.default:
 *       $brooklyn:entitySpec:
 *         type: traefik-load-balancer
 *     location.entity.spec.aws-ec2:
 *       $brooklyn:entitySpec:
 *         type: elastic-load-balancer
 *     location.entity.spec.gce:
 *       $brooklyn:entitySpec:
 *         type: google-load-balancer
 * </pre>
 *
 * @see {@link ConditionalEntity} for an explanation of sensor propagation configuration
 */
@Beta
@ImplementedBy(LocationEntityImpl.class)
public interface LocationEntity extends BasicStartable {

    String DEFAULT = "default";

    @SetFromFlag("entitySpec")
    ConfigKey<Map<String,EntitySpec<?>>> LOCATION_ENTITY_SPEC_MAP = ConfigKeys.newConfigKey(new TypeToken<Map<String,EntitySpec<?>>>() { },
            "location.entity.spec", "The mapping of location properties to the entity specification that will be created. "
                    + "Use the key 'default' to specify an entity specification to use if no match is found.");

    @SetFromFlag("propagateSensors")
    ConfigKey<Boolean> PROPAGATE_LOCATION_ENTITY_SENSORS = ConfigKeys.newBooleanConfigKey(
            "location.entity.propagate", "Whether sensors are to be propagated from the child entity", Boolean.TRUE);

    @SetFromFlag("sensorsToPropagate")
    ConfigKey<Collection<AttributeSensor<?>>> LOCATION_ENTITY_SENSOR_LIST = ConfigKeys.newConfigKey(new TypeToken<Collection<AttributeSensor<?>>>() { },
            "location.entity.sensors", "Collection of sensors that are to be propagated from the child entity (all usual sensors if not set, or empty)");

    AttributeSensor<String> LOCATION_TYPE = Sensors.newStringSensor("location.entity.type", "The class name of the entity location");
    AttributeSensor<String> LOCATION_PROVIDER = Sensors.newStringSensor("location.entity.provider", "The provider name for the entity location");
    AttributeSensor<Set<String>> LOCATION_COUNTRY_CODES = Sensors.newSensor(new TypeToken<Set<String>>() { },
            "location.entity.countryCode", "The ISO 3166 country codes for the entity location");

    AttributeSensor<Entity> LOCATION_ENTITY = Sensors.newSensor(Entity.class,
            "location.entity", "The created entity");

}
