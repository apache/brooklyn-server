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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.List;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class StopAfterDurationPolicyTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testAppStoppedWhenDurationExpires() {
        // Using a second app for the subscriber. Otherwise when app is stopped+unmanaged, we might not
        // receive the last event because all its subscriptions will be closed!
        final RecordingSensorEventListener<Lifecycle> listener = new RecordingSensorEventListener<>();
        Application app2 = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        app2.subscriptions().subscribe(app, Attributes.SERVICE_STATE_ACTUAL, listener);
        
        PolicySpec<StopAfterDurationPolicy> policy = PolicySpec.create(StopAfterDurationPolicy.class)
                .configure(StopAfterDurationPolicy.LIFETIME, Duration.ONE_MILLISECOND)
                .configure(StopAfterDurationPolicy.POLL_PERIOD, Duration.ONE_MILLISECOND);
        app.policies().add(policy);
        app.start(ImmutableList.of(app.newSimulatedLocation()));
        
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                // TODO In tests, have seen duplicate "starting"; and always begins with "stopped"; and have
                // seen "on-fire" between stopping and stopped! Therefore not using 
                // assertEqualsOrderIgnoringDuplicates.
                List<Lifecycle> states = ImmutableList.copyOf(listener.getEventValues());
                assertContainsOrdered(states, ImmutableList.of(Lifecycle.RUNNING, Lifecycle.STOPPING, Lifecycle.STOPPED));
            }});
    }
    
    @SuppressWarnings("unused")
    private void assertEqualsOrderIgnoringDuplicates(Iterable<?> actual, Iterable<?> expected) {
        List<?> actualNoDups = Lists.newArrayList(Sets.newLinkedHashSet(actual));
        List<?> expectedNoDups = Lists.newArrayList(Sets.newLinkedHashSet(expected));
        assertEquals(actualNoDups, expectedNoDups, "actual="+actual);
    }
    
    /**
     * Asserts the actual contains the expected in the same order (but potentially with 
     * other values interleaved).
     */
    private static void assertContainsOrdered(Iterable<?> actual, Iterable<?> expected) {
        List<?> actualList = Lists.newArrayList(actual);
        int index = 0;
        for (Object exp : expected) {
            int foundIndex = actualList.subList(index, actualList.size()).indexOf(exp);
            if (foundIndex >= 0) {
                index = foundIndex + 1;
            } else {
                fail("Failed to find "+exp+" after index "+index+" in "+actual);
            }
        }
    }
}
