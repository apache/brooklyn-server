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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class DynamicClusterRebindTest extends RebindTestFixtureWithApp {

    @Test
    public void testThrottleAppliesAfterRebind() throws Exception {
        DynamicCluster cluster = origApp.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MAX_CONCURRENT_CHILD_COMMANDS, 1)
                .configure(DynamicCluster.INITIAL_SIZE, 1)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(DynamicClusterTest.ThrowOnAsyncStartEntity.class))
                        .configure(DynamicClusterTest.ThrowOnAsyncStartEntity.COUNTER, new AtomicInteger()));
        app().start(ImmutableList.of(origApp.newLocalhostProvisioningLocation()));
        EntityAsserts.assertAttributeEquals(cluster, DynamicCluster.GROUP_SIZE, 1);

        rebind(RebindOptions.create().terminateOrigManagementContext(true));
        cluster = Entities.descendants(app(), DynamicCluster.class).iterator().next();
        cluster.resize(10);
        EntityAsserts.assertAttributeEqualsEventually(cluster, DynamicCluster.GROUP_SIZE, 10);
        EntityAsserts.assertAttributeEquals(cluster, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
    }

}