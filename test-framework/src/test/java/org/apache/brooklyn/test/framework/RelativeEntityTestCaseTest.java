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

package org.apache.brooklyn.test.framework;

import static org.apache.brooklyn.test.Asserts.assertTrue;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.Cluster;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RelativeEntityTestCaseTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testParentAndChildScope() {
        TestEntity parent = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(BrooklynCampConstants.PLAN_ID, "parent-plan"));
        TestEntity child = parent.addChild(EntitySpec.create(TestEntity.class)
                .configure(BrooklynCampConstants.PLAN_ID, "child-plan"));

        parent.sensors().set(TestEntity.NAME, "parent");
        child.sensors().set(TestEntity.NAME, "child");

        app.start(ImmutableList.of(app.newSimulatedLocation()));

        TestCase testCase = app.createAndManageChild(EntitySpec.create(TestCase.class)
                .child(relativeEntityTestCaseEntitySpec(parent, "child-plan", DslComponent.Scope.CHILD, "child"))
                .child(relativeEntityTestCaseEntitySpec(child, "parent-plan", DslComponent.Scope.PARENT, "parent")));

        testCase.start(app.getLocations());
        assertTrue(testCase.sensors().get(Attributes.SERVICE_UP), "Test case did not pass: " + testCase);
    }

    @Test
    public void testSiblingScope() {
        TestEntity brother = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(BrooklynCampConstants.PLAN_ID, "brother-plan"));
        TestEntity sister = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(BrooklynCampConstants.PLAN_ID, "sister-plan"));

        brother.sensors().set(TestEntity.NAME, "brother");
        sister.sensors().set(TestEntity.NAME, "sister");

        app.start(ImmutableList.of(app.newSimulatedLocation()));

        TestCase testCase = app.createAndManageChild(EntitySpec.create(TestCase.class)
                .child(relativeEntityTestCaseEntitySpec(brother, "sister-plan", DslComponent.Scope.SIBLING, "sister"))
                .child(relativeEntityTestCaseEntitySpec(sister, "brother-plan", DslComponent.Scope.SIBLING, "brother")));

        testCase.start(app.getLocations());
        assertTrue(testCase.sensors().get(Attributes.SERVICE_UP), "Test case did not pass: " + testCase);
    }

    @Test
    public void testCombinationWithLoopOverGroupMembersTest() {
        final String sensorName = TestEntity.NAME.getName();
        final String sensorValue = "test-sensor-value";
        EntityInitializer staticSensor = new StaticSensor<>(ConfigBag.newInstance(ImmutableMap.of(
                StaticSensor.SENSOR_NAME, sensorName,
                StaticSensor.STATIC_VALUE, sensorValue)));

        // Application entities

        EntitySpec<TestEntity> childSpec = EntitySpec.create(TestEntity.class)
                .configure(BrooklynCampConstants.PLAN_ID, "child-plan")
                .addInitializer(staticSensor);
        EntitySpec<TestEntity> groupMemberSpec = EntitySpec.create(TestEntity.class)
                .configure(BrooklynCampConstants.PLAN_ID, "group-member-plan")
                .child(childSpec);
        Entity cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, groupMemberSpec)
                .configure(Cluster.INITIAL_SIZE, 3));

        // Start the cluster.
        app.start(ImmutableList.of(app.newSimulatedLocation()));

        LoopOverGroupMembersTestCase groupTest = app.createAndManageChild(EntitySpec.create(LoopOverGroupMembersTestCase.class)
                .configure(LoopOverGroupMembersTestCase.TARGET_ENTITY, cluster)
                .configure(LoopOverGroupMembersTestCase.TEST_SPEC, relativeEntityTestCaseEntitySpec(
                        /* set by group-loop */ null, "child-plan", DslComponent.Scope.CHILD, sensorValue)));

        groupTest.start(app.getLocations());

        // Specifically check the result of the loop test.
        assertTrue(groupTest.sensors().get(Attributes.SERVICE_UP), "Test case did not pass: " + groupTest);
    }

    private EntitySpec<RelativeEntityTestCase> relativeEntityTestCaseEntitySpec(
            Entity testRoot, String targetEntityPlanId, DslComponent.Scope scope, String expectedSensorValue) {
        EntitySpec<TestSensor> sensorTest = sensorHasValueTest(TestEntity.NAME, expectedSensorValue);

        return EntitySpec.create(RelativeEntityTestCase.class)
                .configure(RelativeEntityTestCase.TARGET_ENTITY, testRoot)
                .configure(RelativeEntityTestCase.COMPONENT, new DslComponent(scope, targetEntityPlanId))
                .child(sensorTest);
    }

    private EntitySpec<TestSensor> sensorHasValueTest(Sensor<?> sensorName, Object expectedValue) {
        return EntitySpec.create(TestSensor.class)
                .configure(TestSensor.SENSOR_NAME, sensorName.getName())
                .configure(TestSensor.ASSERTIONS, ImmutableMap.of(
                        TestFrameworkAssertions.EQUAL_TO, expectedValue));
    }

}
