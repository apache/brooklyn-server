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
package org.apache.brooklyn.camp.brooklyn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager.RebindFailureMode;
import org.apache.brooklyn.core.mgmt.rebind.RebindExceptionHandlerImpl;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.policy.autoscaling.AutoScalerPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

@Test
public class AutoScalerPolicyYamlTest extends AbstractYamlRebindTest {
    static final Logger log = LoggerFactory.getLogger(AutoScalerPolicyYamlTest.class);

    // TODO Fails because resize(Up|Down)StabilizationDelay is resolved in policy.init, but 
    // it can't find the entity at that point in time.
    @Test(groups="Broken")
    public void testDslForConfig() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + DynamicCluster.class.getName(),
                "  id: test-cluster",
                "  brooklyn.config:",
                "    initialSize: 0",
                "    memberSpec:",
                "      $brooklyn:entitySpec:",
                "        type: org.apache.brooklyn.core.test.entity.TestEntity",
                "    cluster.metric: myCpu",
                "    cluster.metricLowerBound: 99",
                "    cluster.metricUpperBound: 100",
                "    cluster.minPoolSize: 2",
                "    cluster.maxPoolSize: 3",
                "    cluster.resizeUpStabilizationDelay: 4",
                "    cluster.resizeDownStabilizationDelay: 5",
                "  brooklyn.policies:",
                "  - type: " + AutoScalerPolicy.class.getName(),
                "    brooklyn.config:",
                "      autoscaler.metric:",
                "        $brooklyn:entity(\"test-cluster\").config(\"cluster.metric\")",
                "      autoscaler.metricLowerBound:",
                "        $brooklyn:entity(\"test-cluster\").config(\"cluster.metricLowerBound\")",
                "      autoscaler.metricUpperBound:",
                "        $brooklyn:entity(\"test-cluster\").config(\"cluster.metricUpperBound\")",
                "      autoscaler.minPoolSize:",
                "        $brooklyn:entity(\"test-cluster\").config(\"cluster.minPoolSize\")",
                "      autoscaler.maxPoolSize:",
                "        $brooklyn:entity(\"test-cluster\").config(\"cluster.maxPoolSize\")",
                "      autoscaler.resizeUpStabilizationDelay:",
                "        $brooklyn:entity(\"test-cluster\").config(\"cluster.resizeUpStabilizationDelay\")",
                "      autoscaler.resizeDownStabilizationDelay:",
                "        $brooklyn:entity(\"test-cluster\").config(\"cluster.resizeDownStabilizationDelay\")");
        waitForApplicationTasks(app);

        DynamicCluster cluster = (DynamicCluster) Iterables.getOnlyElement(app.getChildren());
        AutoScalerPolicy policy = Iterables.getOnlyElement(Iterables.filter(cluster.policies().asList(), AutoScalerPolicy.class));
        assertNotNull(policy);
        
        // Rebind
        newApp = rebind(RebindOptions.create().exceptionHandler(RebindExceptionHandlerImpl.builder()
                .rebindFailureMode(RebindFailureMode.FAIL_AT_END)
                .addPolicyFailureMode(RebindFailureMode.FAIL_AT_END)
                .build()));
        
        // Assert policy's config is as expected
        DynamicCluster newCluster = (DynamicCluster) Iterables.getOnlyElement(newApp.getChildren());
        AutoScalerPolicy newPolicy = Iterables.getOnlyElement(Iterables.filter(newCluster.policies().asList(), AutoScalerPolicy.class));
        assertNotNull(newPolicy);

        assertEquals(newPolicy.config().get(AutoScalerPolicy.METRIC), Sensors.newSensor(Object.class, "myCpu"));
        assertEquals(newPolicy.config().get(AutoScalerPolicy.METRIC_LOWER_BOUND), 99);
        assertEquals(newPolicy.config().get(AutoScalerPolicy.METRIC_UPPER_BOUND), 100);
        assertEquals(newPolicy.config().get(AutoScalerPolicy.MIN_POOL_SIZE), Integer.valueOf(2));
        assertEquals(newPolicy.config().get(AutoScalerPolicy.MAX_POOL_SIZE), Integer.valueOf(3));
        assertEquals(newPolicy.config().get(AutoScalerPolicy.RESIZE_UP_STABILIZATION_DELAY), 4);
        assertEquals(newPolicy.config().get(AutoScalerPolicy.RESIZE_UP_STABILIZATION_DELAY), 5);
    }
}
