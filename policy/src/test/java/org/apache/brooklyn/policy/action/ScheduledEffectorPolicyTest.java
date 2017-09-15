/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.policy.action;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ScheduledEffectorPolicyTest extends BrooklynAppUnitTestSupport {

    private static final AttributeSensor<Boolean> START = Sensors.newBooleanSensor("start");

    @Test
    public void testScheduledEffectorFiresImmediately() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(ScheduledEffectorPolicy.class)
                        .configure(ScheduledEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(ScheduledEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(ScheduledEffectorPolicy.TIME, "immediately")
                        .configure(PeriodicEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(ScheduledEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        Asserts.assertTrue(entity.getCallHistory().isEmpty());
        Asserts.assertFalse(policy.config().get(ScheduledEffectorPolicy.RUNNING));

        entity.sensors().set(START, Boolean.TRUE);
        Asserts.eventually(() -> policy.config().get(ScheduledEffectorPolicy.RUNNING), b -> b);
        Asserts.eventually(() -> entity.getCallHistory(), l -> l.contains("myEffector"));
    }

    @Test
    public void testScheduledEffectorFiresAfterDelay() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(ScheduledEffectorPolicy.class)
                        .configure(ScheduledEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(ScheduledEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(ScheduledEffectorPolicy.WAIT, Duration.TEN_SECONDS)
                        .configure(ScheduledEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(ScheduledEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        Asserts.assertTrue(entity.getCallHistory().isEmpty());
        Asserts.assertFalse(policy.config().get(ScheduledEffectorPolicy.RUNNING));

        entity.sensors().set(START, Boolean.TRUE);
        Asserts.eventually(() -> policy.config().get(ScheduledEffectorPolicy.RUNNING), b -> b);
        sleep(Duration.seconds(5));
        Asserts.eventually(() -> entity.getCallHistory(), l -> !l.contains("myEffector"));
        sleep(Duration.seconds(5));
        Asserts.eventually(() -> entity.getCallHistory(), l -> l.contains("myEffector"));
    }

    @Test
    public void testScheduledEffectorFiresOnSensor() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(ScheduledEffectorPolicy.class)
                        .configure(ScheduledEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(ScheduledEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(ScheduledEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(ScheduledEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        Asserts.assertTrue(entity.getCallHistory().isEmpty());
        Asserts.assertFalse(policy.config().get(ScheduledEffectorPolicy.RUNNING));

        entity.sensors().set(START, Boolean.TRUE);
        Asserts.eventually(() -> policy.config().get(ScheduledEffectorPolicy.RUNNING), b -> b);
        sleep(Duration.seconds(5));
        Asserts.eventually(() -> entity.getCallHistory(), l -> !l.contains("myEffector"));

        entity.sensors().set(ScheduledEffectorPolicy.INVOKE_IMMEDIATELY, Boolean.TRUE);
        Asserts.eventually(() -> entity.getCallHistory(), l -> l.contains("myEffector"));
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMilliseconds());
        } catch (InterruptedException ie) {
            Exceptions.propagate(ie);  
        }
    }
}
