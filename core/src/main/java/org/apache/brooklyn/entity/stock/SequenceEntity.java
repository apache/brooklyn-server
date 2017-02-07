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
import org.apache.brooklyn.core.effector.MethodEffector;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.entity.group.SequenceGroup;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

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
 * }</pre>
 */
@ImplementedBy(SequenceEntityImpl.class)
public interface SequenceEntity extends Entity, Startable {

    AttributeSensor<Integer> SEQUENCE_VALUE = SequenceGroup.SEQUENCE_VALUE;

    AttributeSensor<String> SEQUENCE_STRING = SequenceGroup.SEQUENCE_STRING;

    @SetFromFlag("sequenceStart")
    ConfigKey<Integer> SEQUENCE_START = SequenceGroup.SEQUENCE_START;

    @SetFromFlag("sequenceIncrement")
    ConfigKey<Integer> SEQUENCE_INCREMENT = SequenceGroup.SEQUENCE_INCREMENT;

    @SetFromFlag("sequenceFormat")
    ConfigKey<String> SEQUENCE_FORMAT = SequenceGroup.SEQUENCE_FORMAT;

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
