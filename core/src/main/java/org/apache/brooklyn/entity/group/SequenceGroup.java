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
package org.apache.brooklyn.entity.group;

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.MethodEffector;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.text.StringPredicates;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.reflect.TypeToken;

/**
 * A group that sets a sequence of values as sensors on its member entities
 * <p>
 * Usage:
 * <pre>{@code
 * - type: org.apache.brooklyn.entity.stock.SequenceGroup
 *   id: entity-sequence
 *   brooklyn.config:
 *     entityFilter:
 *       $brooklyn:object:
 *         type: com.google.common.base.Predicates
 *         factoryMethod.name: "and"
 *         factoryMethod.args:
 *           - $brooklyn:object:
 *               type: org.apache.brooklyn.core.entity.EntityPredicates
 *               factoryMethod.name: "applicationIdEqualTo"
 *               factoryMethod.args:
 *                 - $brooklyn:attributeWhenReady("application.id")
 *           - $brooklyn:object:
 *               type: org.apache.brooklyn.core.entity.EntityPredicates
 *               factoryMethod.name: "configEqualTo"
 *               factoryMethod.args:
 *                 - "sequence.set"
 *                 - true
 *     sequenceStart: 0
 *     sequenceIncrement: 1
 *     sequenceFormat: "Entity %04x"
 *     sequenceValueSensor: $brooklyn:sensor("entity.sequence")
 *     sequenceStringSensor: $brooklyn:sensor("entity.name")
 * }</pre>
 */
@ImplementedBy(SequenceGroupImpl.class)
public interface SequenceGroup extends DynamicGroup {

    AttributeSensor<Integer> SEQUENCE_VALUE = Sensors.builder(Integer.class, "sequence.value")
            .description("The current value of the sequence")
            .build();

    AttributeSensor<String> SEQUENCE_STRING = Sensors.builder(String.class, "sequence.string")
            .description("The current value of the sequence formatted as a string")
            .build();

    ConfigKey<Predicate<? super Entity>> ENTITY_FILTER = ConfigKeys.newConfigKeyWithDefault(DynamicGroup.ENTITY_FILTER, Predicates.alwaysFalse());

    @SetFromFlag("sequenceStart")
    ConfigKey<Integer> SEQUENCE_START = ConfigKeys.builder(Integer.class)
            .name("sequence.start")
            .description("The starting point of the sequence")
            .defaultValue(1)
            .constraint(Predicates.<Integer>notNull())
            .build();

    @SetFromFlag("sequenceIncrement")
    ConfigKey<Integer> SEQUENCE_INCREMENT = ConfigKeys.builder(Integer.class)
            .name("sequence.increment")
            .description("The sequence increment for the next value")
            .defaultValue(1)
            .constraint(Predicates.<Integer>notNull())
            .build();

    @SetFromFlag("sequenceFormat")
    ConfigKey<String> SEQUENCE_FORMAT = ConfigKeys.builder(String.class)
            .name("sequence.format")
            .description("A format used to generate a string representation of the sequence")
            .defaultValue("%d")
            .constraint(StringPredicates.containsRegex("%[-#+ 0,(]*[0-9]*[doxX]"))
            .build();

    @SetFromFlag("sequenceValueSensor")
    ConfigKey<AttributeSensor<Integer>> SEQUENCE_VALUE_SENSOR = ConfigKeys.builder(new TypeToken<AttributeSensor<Integer>>() { })
            .name("sequence.sensor.value")
            .description("The sensor for the sequence value")
            .defaultValue(SEQUENCE_VALUE)
            .constraint(Predicates.<AttributeSensor<Integer>>notNull())
            .build();

    @SetFromFlag("sequenceStringSensor")
    ConfigKey<AttributeSensor<String>> SEQUENCE_STRING_SENSOR = ConfigKeys.builder(new TypeToken<AttributeSensor<String>>() { })
            .name("sequence.sensor.string")
            .description("The sensor for the sequence string")
            .defaultValue(SEQUENCE_STRING)
            .constraint(Predicates.<AttributeSensor<String>>notNull())
            .build();

    AttributeSensor<Entity> SEQUENCE_CURRENT = Sensors.builder(Entity.class, "sequence.current")
            .description("The current entity in the sequence")
            .build();

    AttributeSensor<Integer> SEQUENCE_NEXT = Sensors.builder(Integer.class, "sequence.next")
            .description("The next value of the sequence")
            .build();

    AttributeSensor<Map<String, Integer>> SEQUENCE_CACHE = Sensors.builder(new TypeToken<Map<String, Integer>>() { }, "sequence.cache")
            .description("The current cache of entity ids to sequence numbers")
            .build();

    MethodEffector<Void> RESET = new MethodEffector<Void>(SequenceGroup.class, "reset");

    @Effector(description = "Reset the sequence to initial value")
    Void reset();

}
