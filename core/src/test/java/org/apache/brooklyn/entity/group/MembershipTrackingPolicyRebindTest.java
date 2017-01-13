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

import static org.testng.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.mgmt.rebind.RebindEntityTest;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.test.Asserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;

public class MembershipTrackingPolicyRebindTest extends RebindTestFixtureWithApp {

    private static final Logger LOG = LoggerFactory.getLogger(MembershipTrackingPolicyRebindTest.class);
    private static final AtomicInteger ADDITIONS_COUNTER = new AtomicInteger();

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ADDITIONS_COUNTER.set(0);
    }

    @Test
    public void testMemberAddedNotFiredAfterRebind() throws Exception {
        DynamicGroup group = origApp.createAndManageChild(EntitySpec.create(DynamicGroup.class)
                .configure(DynamicGroup.ENTITY_FILTER, Predicates.instanceOf(RebindEntityTest.MyEntity.class)));

        origApp.policies().add(PolicySpec.create(Tracker.class)
                .displayName("testMemberAddedNotFiredAfterRebind")
                .configure(AbstractMembershipTrackingPolicy.GROUP, group));
        origApp.createAndManageChild(EntitySpec.create(RebindEntityTest.MyEntity.class));

        Runnable assertOneAddition = new Runnable() {
            @Override
            public void run() {
                assertEquals(ADDITIONS_COUNTER.get(), 1);
            }
        };
        Asserts.succeedsEventually(assertOneAddition);
        rebind();
        Asserts.succeedsContinually(assertOneAddition);
    }

    public static class Tracker extends AbstractMembershipTrackingPolicy {
        @Override
        protected void onEntityAdded(Entity member) {
            super.onEntityAdded(member);
            int count = ADDITIONS_COUNTER.incrementAndGet();
            LOG.info("{} notified of new member: {} (count={})",
                    new Object[]{this, member, count});
        }
    }
}
