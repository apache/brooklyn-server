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
package org.apache.brooklyn.policy.autoscaling;

import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.trait.Resizable;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestCluster;
import org.apache.brooklyn.core.test.entity.TestSizeRecordingCluster;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.util.collections.MutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

public class AutoScalerPolicyPoolSizeTest extends BrooklynAppUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AutoScalerPolicyPoolSizeTest.class);

    private static final int CLUSTER_INIITIAL_SIZE = 3;
    private static final int CLUSTER_MIN_SIZE = 2;
    private static final int CLUSTER_MAX_SIZE = 4;

    AutoScalerPolicy policy;
    TestSizeRecordingCluster cluster;
    List<Integer> resizes = MutableList.of();

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        LOG.info("resetting " + getClass().getSimpleName());
        cluster = app.createAndManageChild(EntitySpec.create(TestSizeRecordingCluster.class)
                .configure(TestCluster.INITIAL_SIZE, CLUSTER_INIITIAL_SIZE)
                .configure(TestCluster.MEMBER_SPEC, EntitySpec.create(BasicStartable.class))
        );
        PolicySpec<AutoScalerPolicy> policySpec = PolicySpec.create(AutoScalerPolicy.class)
                .configure(AutoScalerPolicy.RESIZE_OPERATOR, new ResizeOperator() {
                    @Override
                    public Integer resize(Entity entity, Integer desiredSize) {
                        LOG.info("resizing to " + desiredSize);
                        resizes.add(desiredSize);
                        return ((Resizable) entity).resize(desiredSize);
                    }
                })
                .configure(AutoScalerPolicy.MIN_POOL_SIZE, CLUSTER_MIN_SIZE)
                .configure(AutoScalerPolicy.MAX_POOL_SIZE, CLUSTER_MAX_SIZE);
        policy = cluster.policies().add(policySpec);
        app.start(ImmutableList.of());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        try {
            if (policy != null) policy.destroy();
        } finally {
            super.tearDown();
            cluster = null;
            policy = null;
        }
    }

    @Test
    public void testResizeUp() throws Exception {
        EntityAsserts.assertAttributeEqualsEventually(cluster, TestCluster.GROUP_SIZE, CLUSTER_INIITIAL_SIZE);
        // Simulate user expunging the entities manually
        for (int i = 0; i < CLUSTER_MAX_SIZE - CLUSTER_MIN_SIZE; i++) {
            Entities.destroyCatching(cluster.getMembers().iterator().next());
        }
        EntityAsserts.assertAttributeEqualsEventually(cluster, TestSizeRecordingCluster.SIZE_HISTORY_RECORD_COUNT, 2);
        Assert.assertEquals((int)cluster.getSizeHistory().get(0), CLUSTER_INIITIAL_SIZE);
        Assert.assertEquals((int)cluster.getSizeHistory().get(1), CLUSTER_MIN_SIZE);
    }

    @Test
    public void testResizeDown() throws Exception {
        EntityAsserts.assertAttributeEqualsEventually(cluster, TestCluster.GROUP_SIZE, CLUSTER_INIITIAL_SIZE);
        cluster.resize(CLUSTER_MAX_SIZE + 2);
        EntityAsserts.assertAttributeEqualsEventually(cluster, TestSizeRecordingCluster.SIZE_HISTORY_RECORD_COUNT, 3);
        Assert.assertEquals((int)cluster.getSizeHistory().get(0), CLUSTER_INIITIAL_SIZE);
        Assert.assertEquals((int)cluster.getSizeHistory().get(1), CLUSTER_MAX_SIZE + 2);
        Assert.assertEquals((int)cluster.getSizeHistory().get(2), CLUSTER_MAX_SIZE);
    }
}
