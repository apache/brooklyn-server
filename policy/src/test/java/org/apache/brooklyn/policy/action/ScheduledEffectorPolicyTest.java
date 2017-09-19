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
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ScheduledEffectorPolicyTest extends AbstractEffectorPolicyTest {

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
        assertConfigEqualsEventually(policy, ScheduledEffectorPolicy.RUNNING, true);
        assertCallHistoryContainsEventually(entity, "myEffector");
    }

    // Integration because of long wait
    @Test(groups="Integration")
    public void testScheduledEffectorFiresAfterDelay() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(ScheduledEffectorPolicy.class)
                        .configure(ScheduledEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(ScheduledEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(ScheduledEffectorPolicy.WAIT, Duration.FIVE_SECONDS)
                        .configure(ScheduledEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(ScheduledEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        Asserts.assertTrue(entity.getCallHistory().isEmpty());
        Asserts.assertFalse(policy.config().get(ScheduledEffectorPolicy.RUNNING));

        entity.sensors().set(START, Boolean.TRUE);
        assertConfigEqualsEventually(policy, ScheduledEffectorPolicy.RUNNING, true);
        assertCallHistoryNeverContinually(entity, "myEffector");

        Time.sleep(Duration.seconds(5));
        assertCallHistoryContainsEventually(entity, "myEffector");
    }

    // Integration because of long wait
    @Test(groups="Integration")
    public void testSuspendsAndResumes() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(ScheduledEffectorPolicy.class)
                        .configure(ScheduledEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(ScheduledEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(ScheduledEffectorPolicy.WAIT, Duration.FIVE_SECONDS)
                        .configure(ScheduledEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(ScheduledEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        entity.sensors().set(START, Boolean.TRUE);
        assertConfigEqualsEventually(policy, ScheduledEffectorPolicy.RUNNING, true);
        
        policy.suspend();
        policy.resume();

        Time.sleep(Duration.seconds(5));
        assertCallHistoryContainsEventually(entity, "myEffector");
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
        assertConfigEqualsEventually(policy, ScheduledEffectorPolicy.RUNNING, true);
        assertCallHistoryNeverContinually(entity, "myEffector");

        entity.sensors().set(ScheduledEffectorPolicy.INVOKE_IMMEDIATELY, Boolean.TRUE);
        assertCallHistoryContainsEventually(entity, "myEffector");
    }
}
