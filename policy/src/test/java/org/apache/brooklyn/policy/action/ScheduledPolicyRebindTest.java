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
package org.apache.brooklyn.policy.action;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ScheduledPolicyRebindTest extends RebindTestFixtureWithApp {

    private static final AttributeSensor<Boolean> START = Sensors.newBooleanSensor("start");

    /*
     * This test simulates what happens when the rebind occurs after more than the
     * scheduled period of time has elapsed.
     */
    @Test
    public void testShortPeriodicEffectorFiresAfterRebind() throws Exception {
        TestEntity origEntity = origApp.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(PeriodicEffectorPolicy.class)
                        .configure(PeriodicEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(PeriodicEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(PeriodicEffectorPolicy.PERIOD, Duration.millis(1))
                        .configure(PeriodicEffectorPolicy.TIME, "immediately")
                        .configure(PeriodicEffectorPolicy.START_SENSOR, START)));

        origEntity.sensors().set(START, Boolean.TRUE);
        Asserts.eventually(() -> origEntity.getCallHistory(), l -> l.contains("myEffector"));

        Policy origPolicy = Iterables.tryFind(origEntity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(origPolicy);
        newApp = rebind();
        ((AbstractPolicy) origPolicy).destroy();
        TestEntity newEntity = (TestEntity) Iterables.tryFind(newApp.getChildren(), Predicates.instanceOf(TestEntity.class)).orNull();
        Asserts.assertNotNull(newEntity);
        Policy newPolicy = Iterables.tryFind(newEntity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(newPolicy);

        Asserts.eventually(() -> newPolicy.config().get(PeriodicEffectorPolicy.RUNNING), b -> b);
        int calls = newEntity.getCallHistory().size();
        Asserts.eventually(() -> newEntity.getCallHistory().size(), i -> i > (calls + 500));
    }

    @Test
    public void testLongPeriodicEffectorFiresAfterRebind() throws Exception {
        TestEntity origEntity = origApp.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(PeriodicEffectorPolicy.class)
                        .configure(PeriodicEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(PeriodicEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(PeriodicEffectorPolicy.PERIOD, Duration.seconds(1))
                        .configure(PeriodicEffectorPolicy.TIME, "immediately")
                        .configure(PeriodicEffectorPolicy.START_SENSOR, START)));

        origEntity.sensors().set(START, Boolean.TRUE);
        Asserts.eventually(() -> origEntity.getCallHistory(), l -> l.contains("myEffector"));

        Policy origPolicy = Iterables.tryFind(origEntity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(origPolicy);
        newApp = rebind();
        ((AbstractPolicy) origPolicy).destroy();
        TestEntity newEntity = (TestEntity) Iterables.tryFind(newApp.getChildren(), Predicates.instanceOf(TestEntity.class)).orNull();
        Asserts.assertNotNull(newEntity);
        Policy newPolicy = Iterables.tryFind(newEntity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(newPolicy);

        Asserts.eventually(() -> newPolicy.config().get(PeriodicEffectorPolicy.RUNNING), b -> b);
        int calls = newEntity.getCallHistory().size();
        Asserts.eventually(() -> newEntity.getCallHistory().size(), i -> i > (calls + 5));
    }

    @Test
    public void testPeriodicEffectorStartsAfterRebind() throws Exception {
        TestEntity origEntity = origApp.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(PeriodicEffectorPolicy.class)
                        .configure(PeriodicEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(PeriodicEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(PeriodicEffectorPolicy.PERIOD, Duration.millis(1))
                        .configure(PeriodicEffectorPolicy.TIME, "immediately")
                        .configure(PeriodicEffectorPolicy.START_SENSOR, START)));

        Policy origPolicy = Iterables.tryFind(origEntity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(origPolicy);
        newApp = rebind();
        ((AbstractPolicy) origPolicy).destroy();
        TestEntity newEntity = (TestEntity) Iterables.tryFind(newApp.getChildren(), Predicates.instanceOf(TestEntity.class)).orNull();
        Asserts.assertNotNull(newEntity);
        Policy newPolicy = Iterables.tryFind(newEntity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(newPolicy);

        Asserts.assertFalse(newPolicy.config().get(PeriodicEffectorPolicy.RUNNING));
        Asserts.assertFalse(newEntity.getCallHistory().contains("myEffector"));
        Asserts.assertFalse(origEntity.getCallHistory().contains("myEffector"));

        newEntity.sensors().set(START, Boolean.TRUE);
        Asserts.eventually(() -> newPolicy.config().get(PeriodicEffectorPolicy.RUNNING), b -> b);
        Asserts.eventually(() -> newEntity.getCallHistory(), l -> l.contains("myEffector"));
        int calls = newEntity.getCallHistory().size();
        Asserts.eventually(() -> newEntity.getCallHistory().size(), i -> i > (calls + 500));
    }
}
