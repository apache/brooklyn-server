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

public class PeriodicEffectorPolicyTest extends AbstractEffectorPolicyTest {

    @Test
    public void testPeriodicEffectorFires() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(PeriodicEffectorPolicy.class)
                        .configure(PeriodicEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(PeriodicEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(PeriodicEffectorPolicy.PERIOD, Duration.ONE_MILLISECOND)
                        .configure(PeriodicEffectorPolicy.TIME, "immediately")
                        .configure(PeriodicEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        Asserts.assertTrue(entity.getCallHistory().isEmpty());
        Asserts.assertFalse(policy.config().get(PeriodicEffectorPolicy.RUNNING));

        entity.sensors().set(START, Boolean.TRUE);
        assertConfigEqualsEventually(policy, PeriodicEffectorPolicy.RUNNING, true);
        assertCallHistoryEventually(entity, "myEffector", 2);
    }

    // Integration because of long wait
    @Test(groups="Integration")
    public void testPeriodicEffectorFiresAfterDelay() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(PeriodicEffectorPolicy.class)
                        .configure(PeriodicEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(PeriodicEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(PeriodicEffectorPolicy.PERIOD, Duration.ONE_MILLISECOND)
                        .configure(PeriodicEffectorPolicy.WAIT, Duration.FIVE_SECONDS)
                        .configure(PeriodicEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        Asserts.assertTrue(entity.getCallHistory().isEmpty());
        Asserts.assertFalse(policy.config().get(PeriodicEffectorPolicy.RUNNING));

        entity.sensors().set(START, Boolean.TRUE);
        assertConfigEqualsEventually(policy, PeriodicEffectorPolicy.RUNNING, true);
        assertCallHistoryNeverContinually(entity, "myEffector");
        
        Time.sleep(Duration.seconds(5));
        assertCallHistoryEventually(entity, "myEffector", 2);
    }

    @Test
    public void testSuspendsAndResumes() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(PeriodicEffectorPolicy.class)
                        .configure(PeriodicEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(PeriodicEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(PeriodicEffectorPolicy.PERIOD, Duration.ONE_MILLISECOND)
                        .configure(PeriodicEffectorPolicy.TIME, "immediately")
                        .configure(PeriodicEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        entity.sensors().set(START, Boolean.TRUE);
        assertCallHistoryContainsEventually(entity, "myEffector");
        
        policy.suspend();
        assertCallsStopEventually(entity, "myEffector");
        entity.clearCallHistory();
        
        policy.resume();
        assertCallHistoryContainsEventually(entity, "myEffector");
    }
    
    @Test
    public void testSuspendsAndResumeBeforeTriggered() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(PeriodicEffectorPolicy.class)
                        .configure(PeriodicEffectorPolicy.EFFECTOR, "myEffector")
                        .configure(PeriodicEffectorPolicy.EFFECTOR_ARGUMENTS, ImmutableMap.of())
                        .configure(PeriodicEffectorPolicy.PERIOD, Duration.ONE_MILLISECOND)
                        .configure(PeriodicEffectorPolicy.TIME, "immediately")
                        .configure(PeriodicEffectorPolicy.START_SENSOR, START)));
        Policy policy = Iterables.tryFind(entity.policies(), Predicates.instanceOf(PeriodicEffectorPolicy.class)).orNull();
        Asserts.assertNotNull(policy);

        policy.suspend();
        policy.resume();
        assertCallHistoryNeverContinually(entity, "myEffector");
        
        entity.sensors().set(START, Boolean.TRUE);
        assertCallHistoryContainsEventually(entity, "myEffector");
    }
}
