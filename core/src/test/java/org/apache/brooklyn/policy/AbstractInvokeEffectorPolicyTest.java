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

package org.apache.brooklyn.policy;

import java.util.concurrent.CountDownLatch;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;

public class AbstractInvokeEffectorPolicyTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testCountReflectsNumberOfExecutingEffectors() {
        final CountDownLatch effectorLatch = new CountDownLatch(1);
        final AttributeSensor<Boolean> policyIsBusy = Sensors.newBooleanSensor(
                "policyIsBusy");
        final Effector<Void> blockingEffector = Effectors.effector(Void.class, "abstract-invoke-effector-policy-test")
                .impl(new BlockingEffector(effectorLatch))
                .build();
        final TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        final TestAbstractInvokeEffectorPolicy policy = entity.policies().add(
                PolicySpec.create(TestAbstractInvokeEffectorPolicy.class)
                        .configure(AbstractInvokeEffectorPolicy.IS_BUSY_SENSOR_NAME, policyIsBusy.getName()));
        final Task<?> effectorTask = policy.invoke(blockingEffector, ImmutableMap.<String, Object>of());

        // expect isbusy on entity, effector incomplete.
        Supplier<Boolean> effectorTaskDoneSupplier = new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                return effectorTask.isDone();
            }
        };
        Asserts.continually(effectorTaskDoneSupplier, Predicates.equalTo(false));
        EntityAsserts.assertAttributeEqualsEventually(entity, policyIsBusy, true);

        effectorLatch.countDown();

        Asserts.eventually(effectorTaskDoneSupplier, Predicates.equalTo(true));
        EntityAsserts.assertAttributeEqualsEventually(entity, policyIsBusy, false);
    }

    public static class TestAbstractInvokeEffectorPolicy extends AbstractInvokeEffectorPolicy {
        public TestAbstractInvokeEffectorPolicy() {
        }

        @Override
        public void setEntity(EntityLocal entity) {
            super.setEntity(entity);
        }
    }

    private static class BlockingEffector extends EffectorBody<Void> {
        final CountDownLatch latch;

        private BlockingEffector(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public Void call(ConfigBag config) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw Exceptions.propagate(e);
            }
            return null;
        }
    }

}
