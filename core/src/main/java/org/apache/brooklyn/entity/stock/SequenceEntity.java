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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.MethodEffector;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.text.StringPredicates;

import com.google.common.base.Predicates;

/**
 * An entity that supplies a sequence of values through an effector.
 * <p>
 * Usage:
 * <pre>{@code
 * - type: org.apache.brooklyn.entity.stock.SequenceEntity
 *   id: global-sequence
 *   brooklyn.config:
 *     sequence.start: 0
 *     sequence.increment: 1
 *     sequence.format: "global-%03d"
 *     sequence.name: "global"
 * }</pre>
 */
@ImplementedBy(SequenceEntityImpl.class)
public interface SequenceEntity extends Entity, Startable {

    @SetFromFlag("sequenceStart")
    ConfigKey<Integer> SEQUENCE_START = ConfigKeys.builder(Integer.class)
            .name("sequence.start")
            .description("The starting point of the sequence")
            .defaultValue(1)
            .constraint(Predicates.<Integer>notNull())
            .build();

    @SetFromFlag("sequenceIncrement")
    ConfigKey<Integer> SEQUENCE_INCREMENT =  ConfigKeys.builder(Integer.class)
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

    @SetFromFlag("sequenceName")
    AttributeSensorAndConfigKey<String, String> SEQUENCE_NAME = ConfigKeys.newStringSensorAndConfigKey("sequence.name", "The name of the sequence", "sequence");

    AttributeSensor<Integer> SEQUENCE_VALUE = Sensors.builder(Integer.class, "sequence.value")
            .description("The current value of the sequence")
            .build();

    AttributeSensor<String> SEQUENCE_STRING = Sensors.builder(String.class, "sequence.string")
            .description("The current value of the sequence formatted as a string")
            .build();

    MethodEffector<Void> RESET = new MethodEffector<Void>(SequenceEntity.class, "reset");
    MethodEffector<Void> INCREMENT = new MethodEffector<Void>(SequenceEntity.class, "increment");
    MethodEffector<Integer> CURRENT_VALUE = new MethodEffector<Integer>(SequenceEntity.class, "currentValue");
    MethodEffector<String> CURRENT_STRING = new MethodEffector<String>(SequenceEntity.class, "currentString");
    MethodEffector<Integer> NEXT_VALUE = new MethodEffector<Integer>(SequenceEntity.class, "nextValue");
    MethodEffector<String> NEXT_STRING = new MethodEffector<String>(SequenceEntity.class, "nextString");

    @Effector(description = "Reset the sequence to initial value")
    Void reset();

    @Effector(description = "Update the value of the sequence by the configured increment")
    Void increment();

    @Effector(description = "Return the current numeric value of the sequence")
    Integer currentValue();

    @Effector(description = "Return the current string representation of the sequence")
    String currentString();

    @Effector(description = "Update and return the next numeric value of the sequence")
    Integer nextValue();

    @Effector(description = "Update and return the next string representation of the sequence")
    String nextString();
}
