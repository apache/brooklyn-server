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

import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually;

import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class StopAfterDurationPolicyTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testAppStoppedWhenDurationExpires() {
        PolicySpec<StopAfterDurationPolicy> policy = PolicySpec.create(StopAfterDurationPolicy.class)
                .configure(StopAfterDurationPolicy.LIFETIME, Duration.ONE_MILLISECOND)
                .configure(StopAfterDurationPolicy.POLL_PERIOD, Duration.ONE_MILLISECOND);
        app.policies().add(policy);
        app.start(ImmutableList.of(app.newSimulatedLocation()));
        assertAttributeEqualsEventually(app, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertAttributeEqualsEventually(app, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPED);
    }

}
