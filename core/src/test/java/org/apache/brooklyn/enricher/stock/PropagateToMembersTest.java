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
package org.apache.brooklyn.enricher.stock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.group.DynamicGroup;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.junit.Assert;
import org.testng.annotations.Test;

public class PropagateToMembersTest extends BrooklynAppUnitTestSupport {

    /**
     * Tests {@link PropagateToMembers} to throw a {@link RuntimeException} if enricher is set to a non-group entity.
     */
    @Test(expectedExceptions = RuntimeException.class)
    public void testPropagateToMembers_NonGroupEntity() {

        // Prepare sensor for Enricher to propagate to members.
        final AttributeSensor<String> sensorBeingModified = Sensors.newStringSensor("sensor-being-modified");

        // Create non-group entity and configure enricher to test.
        app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .enricher(EnricherSpec.create(PropagateToMembers.class)
                        .configure(PropagateToMembers.PROPAGATING, ImmutableList.of(sensorBeingModified))));
    }

    /**
     * Tests {@link PropagateToMembers} to set initial value of the propagated sensor to an existing member.
     */
    @Test
    public void testPropagateToMembers_PropagateInitialValue() {

        final String sensorInitialValue = "Initial value";

        // Prepare sensor for Enricher to propagate to members.
        final AttributeSensor<String> sensorToPropagate = Sensors.newStringSensor("sensor-being-modified");

        // Create member entity to propagate attribute to.
        final BasicEntity memberEntity = app.createAndManageChild(EntitySpec.create(BasicEntity.class));

        // Create group entity, configure enricher and set member to test propagation at.
        final DynamicGroup groupEntity = app.createAndManageChild(EntitySpec.create(DynamicGroup.class)
                .members(ImmutableList.of(memberEntity))
                .addInitializer(new StaticSensor<String>(ConfigBag.newInstance(ImmutableMap.of(
                        StaticSensor.SENSOR_NAME, "sensor-being-modified",
                        StaticSensor.STATIC_VALUE, sensorInitialValue))))
                .enricher(EnricherSpec.create(PropagateToMembers.class)
                        .configure(PropagateToMembers.PROPAGATING, ImmutableList.of(sensorToPropagate))));

        // Verify that entity and member sensor have expected initial value.
        EntityAsserts.assertAttributeEqualsEventually(groupEntity, sensorToPropagate, sensorInitialValue);
        EntityAsserts.assertAttributeEqualsEventually(memberEntity, sensorToPropagate, sensorInitialValue);
    }

    /**
     * Tests {@link PropagateToMembers} to set initial value of the propagated sensor to a newly added member.
     */
    @Test
    public void testPropagateToMembers_PropagateToAddedMember() {

        final String sensorInitialValue = "Initial value";

        // Prepare sensor for Enricher to propagate to members.
        final AttributeSensor<String> sensorToPropagate = Sensors.newStringSensor("sensor-being-modified");

        // Create member entity to propagate attribute to.
        final BasicEntity memberEntity = app.createAndManageChild(EntitySpec.create(BasicEntity.class));

        // Create group entity, configure enricher and set member to test propagation at.
        final DynamicGroup groupEntity = app.createAndManageChild(EntitySpec.create(DynamicGroup.class)
                .addInitializer(new StaticSensor<String>(ConfigBag.newInstance(ImmutableMap.of(
                        StaticSensor.SENSOR_NAME, "sensor-being-modified",
                        StaticSensor.STATIC_VALUE, sensorInitialValue))))
                .enricher(EnricherSpec.create(PropagateToMembers.class)
                        .configure(PropagateToMembers.PROPAGATING, ImmutableList.of(sensorToPropagate))));

        // Verify that entity has an expected initial value and member has not.
        EntityAsserts.assertAttributeEqualsEventually(groupEntity, sensorToPropagate, sensorInitialValue);
        Assert.assertNotEquals(sensorInitialValue, memberEntity.getAttribute(sensorToPropagate));

        // Add member and verify that member has an expected initial value.
        groupEntity.addMember(memberEntity);
        EntityAsserts.assertAttributeEqualsEventually(memberEntity, sensorToPropagate, sensorInitialValue);
    }

    /**
     * Tests {@link PropagateToMembers} to propagate updated sensor value to an existing member.
     */
    @Test
    public void testPropagateToMembers_PropagateOnValueChange() {

        final String sensorModifiedValue = "Modified value";

        // Prepare sensor for Enricher to propagate to members.
        final AttributeSensor<String> sensorToPropagate = Sensors.newStringSensor("sensor-being-modified");

        // Create member entity to propagate attribute to.
        final BasicEntity memberEntity = app.createAndManageChild(EntitySpec.create(BasicEntity.class));

        // Create group entity, configure enricher and set member to test propagation at.
        final DynamicGroup groupEntity = app.createAndManageChild(EntitySpec.create(DynamicGroup.class)
                .members(ImmutableList.of(memberEntity))
                .enricher(EnricherSpec.create(PropagateToMembers.class)
                        .configure(PropagateToMembers.PROPAGATING, ImmutableList.of(sensorToPropagate))));

        // Verify that sensor has no initial value.
        Assert.assertNotEquals(sensorModifiedValue, groupEntity.getAttribute(sensorToPropagate));
        Assert.assertNotEquals(sensorModifiedValue, memberEntity.getAttribute(sensorToPropagate));

        // Set the the sensor value to propagate.
        groupEntity.sensors().set(sensorToPropagate, sensorModifiedValue);
        EntityAsserts.assertAttributeEqualsEventually(groupEntity, sensorToPropagate, sensorModifiedValue);

        // Expect the member to have a changed sensor value, propagated from the group entity.
        EntityAsserts.assertAttributeEqualsEventually(memberEntity, sensorToPropagate, sensorModifiedValue);
    }

    /**
     * Tests {@link PropagateToMembers} to propagate multiple sensors to multiple members.
     */
    @Test
    public void testPropagateToMembers_MultipleValuesMultipleMembers() {

        final String sensorModifiedValue = "Modified value";

        // Prepare sensors for Enricher to propagate to members.
        final AttributeSensor<String> sensorToPropagateX = Sensors.newStringSensor("sensor-being-modified-x");
        final AttributeSensor<String> sensorToPropagateY = Sensors.newStringSensor("sensor-being-modified-y");

        // Create member entities to propagate attribute to.
        final BasicEntity memberEntityA = app.createAndManageChild(EntitySpec.create(BasicEntity.class));
        final BasicEntity memberEntityB = app.createAndManageChild(EntitySpec.create(BasicEntity.class));

        // Create group entity, configure enricher and set member to test propagation at.
        final DynamicGroup groupEntity = app.createAndManageChild(EntitySpec.create(DynamicGroup.class)
                .members(ImmutableList.of(memberEntityA, memberEntityB))
                .enricher(EnricherSpec.create(PropagateToMembers.class)
                        .configure(PropagateToMembers.PROPAGATING, ImmutableList.of(sensorToPropagateX, sensorToPropagateY))));

        // Verify that sensors have no initial value.
        Assert.assertNotEquals(sensorModifiedValue, groupEntity.getAttribute(sensorToPropagateX));
        Assert.assertNotEquals(sensorModifiedValue, groupEntity.getAttribute(sensorToPropagateY));

        // Set the new value to sensors to propagate.
        groupEntity.sensors().set(sensorToPropagateX, sensorModifiedValue);
        groupEntity.sensors().set(sensorToPropagateY, sensorModifiedValue);

        // Verify that sensors of the group entity and members have expected values.
        EntityAsserts.assertAttributeEqualsEventually(groupEntity, sensorToPropagateX, sensorModifiedValue);
        EntityAsserts.assertAttributeEqualsEventually(groupEntity, sensorToPropagateY, sensorModifiedValue);
        EntityAsserts.assertAttributeEqualsEventually(memberEntityA, sensorToPropagateX, sensorModifiedValue);
        EntityAsserts.assertAttributeEqualsEventually(memberEntityA, sensorToPropagateY, sensorModifiedValue);
        EntityAsserts.assertAttributeEqualsEventually(memberEntityB, sensorToPropagateX, sensorModifiedValue);
        EntityAsserts.assertAttributeEqualsEventually(memberEntityB, sensorToPropagateY, sensorModifiedValue);
    }
}
