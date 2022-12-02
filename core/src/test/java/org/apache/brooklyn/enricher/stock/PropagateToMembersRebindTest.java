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
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.BasicGroup;
import org.apache.brooklyn.entity.group.DynamicMultiGroup;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.junit.Assert;
import org.testng.annotations.Test;

import static com.google.common.base.Predicates.instanceOf;
import static org.apache.brooklyn.entity.group.DynamicGroup.ENTITY_FILTER;
import static org.apache.brooklyn.entity.group.DynamicMultiGroupImpl.bucketFromAttribute;

public class PropagateToMembersRebindTest extends RebindTestFixtureWithApp {

    private static final AttributeSensor<String> SENSOR = Sensors.newSensor(String.class, "multigroup.test");

    /**
     * Tests {@link PropagateToMembers} to set initial value of the propagated sensor to an existing member.
     */
    @Test
    public void testPropagateToMembers_PropagateInitialValue() throws Exception {

        final String sensorInitialValue = "Initial value";

        // Prepare sensor for Enricher to propagate to members.
        final AttributeSensor<String> sensorToPropagate = Sensors.newStringSensor("sensor-being-modified");

        // Create member entity to propagate attribute to.
        final BasicEntity memberEntity = app().createAndManageChild(EntitySpec.create(BasicEntity.class));

        // Create group entity, configure enricher and set member to test propagation at.
        final BasicGroup groupEntity = app().createAndManageChild(EntitySpec.create(BasicGroup.class)
                .members(ImmutableList.of(memberEntity))
                .addInitializer(new StaticSensor<String>(ConfigBag.newInstance(ImmutableMap.of(
                        StaticSensor.SENSOR_NAME, sensorToPropagate.getName(),
                        StaticSensor.STATIC_VALUE, sensorInitialValue))))
                .enricher(EnricherSpec.create(PropagateToMembers.class)
                        .configure(PropagateToMembers.PROPAGATING, ImmutableList.of(sensorToPropagate))));

        // Verify that entity and member sensor have expected initial value.
        EntityAsserts.assertAttributeEqualsEventually(groupEntity, sensorToPropagate, sensorInitialValue);
        EntityAsserts.assertAttributeEqualsEventually(memberEntity, sensorToPropagate, sensorInitialValue);

        rebind();
    }

    /**
     * Tests {@link PropagateToMembers} to set initial value of the propagated sensor to an existing member.
     */
    @Test
    public void testPropagateToMembers_DynamicMultiGroup() throws Exception {
        TestApplication app = app();

        final String sensorInitialValue = "Initial value";

        // Prepare sensor for Enricher to propagate to members.
        final AttributeSensor<String> sensorToPropagate = Sensors.newStringSensor("sensor-being-modified");

        Group group = app.createAndManageChild(EntitySpec.create(BasicGroup.class));
        final DynamicMultiGroup dmg = app.createAndManageChild(
                EntitySpec.create(DynamicMultiGroup.class)
                        .configure(ENTITY_FILTER, instanceOf(TestEntity.class))
                        .configure(DynamicMultiGroup.BUCKET_EXPRESSION, "${entity.sensor['"+SENSOR.getName()+"']}")
                        .configure(DynamicMultiGroup.BUCKET_SPEC, EntitySpec.create(BasicGroup.class)
                                .enricher(EnricherSpec.create(PropagateToMembers.class)
                                        .configure(PropagateToMembers.PROPAGATING, ImmutableList.of(sensorToPropagate))) )
                        .addInitializer(new StaticSensor<String>(ConfigBag.newInstance(ImmutableMap.of(
                                StaticSensor.SENSOR_NAME, sensorToPropagate.getName(),
                                StaticSensor.STATIC_VALUE, sensorInitialValue))))
                        .enricher(EnricherSpec.create(PropagateToMembers.class)
                                .configure(PropagateToMembers.PROPAGATING, ImmutableList.of(sensorToPropagate)))
        );
        app.subscriptions().subscribeToChildren(group, SENSOR, new SensorEventListener<String>() {
            @Override
            public void onEvent(SensorEvent<String> event) { dmg.rescanEntities(); }
        });

        EntitySpec<TestEntity> childSpec = EntitySpec.create(TestEntity.class);
        TestEntity child1 = group.addChild(EntitySpec.create(childSpec).displayName("child1"));
        TestEntity child2 = group.addChild(EntitySpec.create(childSpec).displayName("child2"));

        EntityAsserts.assertAttributeEqualsEventually(child1, sensorToPropagate, sensorInitialValue);

        rebind();
    }

}
