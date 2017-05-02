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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.effector.MethodEffector;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.entity.group.SequenceGroup;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

import com.google.common.base.Supplier;

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
 * }</pre>
 */
@ImplementedBy(SequenceEntityImpl.class)
public interface SequenceEntity extends Entity, Startable, Supplier<Integer> {

    AttributeSensor<Integer> SEQUENCE_VALUE = SequenceGroup.SEQUENCE_VALUE;

    AttributeSensor<AtomicInteger> SEQUENCE_STATE = SequenceGroup.SEQUENCE_STATE;

    @SetFromFlag("sequenceStart")
    ConfigKey<Integer> SEQUENCE_START = SequenceGroup.SEQUENCE_START;

    @SetFromFlag("sequenceIncrement")
    ConfigKey<Integer> SEQUENCE_INCREMENT = SequenceGroup.SEQUENCE_INCREMENT;

    MethodEffector<Void> RESET = new MethodEffector<Void>(SequenceEntity.class, "reset");
    MethodEffector<Void> INCREMENT = new MethodEffector<Void>(SequenceEntity.class, "increment");
    MethodEffector<Integer> GET = new MethodEffector<Integer>(SequenceEntity.class, "get");
    MethodEffector<Integer> INCREMENT_AND_GET = new MethodEffector<Integer>(SequenceEntity.class, "incrementAndGet");

    @Effector(description = "Reset the sequence to initial value")
    Void reset();

    @Effector(description = "Update the value of the sequence by the configured increment")
    Void increment();

    @Effector(description = "Return the current numeric value of the sequence")
    Integer get();

    @Effector(description = "Update and return the next value of the sequence")
    Integer incrementAndGet();

}
