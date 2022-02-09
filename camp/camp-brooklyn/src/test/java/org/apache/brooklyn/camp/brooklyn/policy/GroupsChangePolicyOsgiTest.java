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
package org.apache.brooklyn.camp.brooklyn.policy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.effector.CompositeEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.*;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.core.sensor.password.CreatePasswordSensor;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.DynamicGroup;
import org.apache.brooklyn.entity.group.GroupsChangePolicy;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.policy.InvokeEffectorOnSensorChange;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.Serializable;
import java.util.Objects;

import static org.apache.brooklyn.test.Asserts.*;

public class GroupsChangePolicyOsgiTest extends AbstractYamlTest {

    private TestApplication app;

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
    }

    @Override
    protected boolean disableOsgi() {
        return false; // enable OSGI
    }

    /** Test effector that adds a sensor with a test value. */
    public static final class EffectorForThisTest extends EntityInitializers.InitializerPatternWithConfigKeys implements Serializable {

        public static final String NAME = "testEffector";
        public static final String SENSOR = "testSensor";
        public static final String SENSOR_VALUE = "testSensorValue";

        @Override
        public void apply(final EntityLocal entity) {
            ((EntityInternal) entity).getMutableEntityType().addEffector(Effectors.effector(Void.class, NAME).impl(new EffectorBody<Void>() {
                @Override
                public Void call(ConfigBag parameters) {
                    entity().sensors().set(Sensors.newStringSensor(SENSOR), SENSOR_VALUE);
                    return null;
                }
            }).build());
        }
    }

    @Test
    public void testAddInitializers() {

        final String STATIC_SENSOR_VALUE = "staticSensorValue";

        // Create a dynamic group.
        DynamicGroup dynamicGroup = app.addChild(EntitySpec.create(DynamicGroup.class));

        // Create a GroupsChangePolicy policy spec for the dynamic group.
        PolicySpec<GroupsChangePolicy> policySpec =
                PolicySpec.create(GroupsChangePolicy.class)
                        .configure(GroupsChangePolicy.GROUP, dynamicGroup)
                        .configure(GroupsChangePolicy.INITIALIZERS,
                                ImmutableList.of(
                                        ImmutableMap.of(
                                                "type", StaticSensor.class.getName(),
                                                "brooklyn.config", ImmutableMap.of(
                                                        "name", "member-sensor",
                                                        "target.type", "string",
                                                        "static.value", STATIC_SENSOR_VALUE))));

        // Add GroupsChangePolicy to the dynamic group.
        dynamicGroup.policies().add(policySpec);

        // Create an entity to add as a dynamic group member to test GroupsChangePolicy.
        TestEntity member = app.addChild(EntitySpec.create(TestEntity.class));

        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));

        assertEqualsIgnoringOrder(dynamicGroup.getMembers(), ImmutableList.of(member));

        Asserts.eventually(() -> member.getAttribute(Sensors.newStringSensor("member-sensor")), STATIC_SENSOR_VALUE::equals);
    }

    @Test
    public void testAddInitializersPassword() {

        // Create a dynamic group.
        DynamicGroup dynamicGroup = app.addChild(EntitySpec.create(DynamicGroup.class));

        // Create a GroupsChangePolicy policy spec for the dynamic group.
        PolicySpec<GroupsChangePolicy> policySpec =
                PolicySpec.create(GroupsChangePolicy.class)
                        .configure(GroupsChangePolicy.GROUP, dynamicGroup)
                        .configure(GroupsChangePolicy.INITIALIZERS,
                                ImmutableList.of(
                                        ImmutableMap.of(
                                                "type", CreatePasswordSensor.class.getName(),
                                                "brooklyn.config", ImmutableMap.of(
                                                        "name", "member-sensor",
                                                        "password.length", 15))));

        // Add GroupsChangePolicy to the dynamic group.
        dynamicGroup.policies().add(policySpec);

        // Create an entity to add as a dynamic group member to test GroupsChangePolicy.
        TestEntity member = app.addChild(EntitySpec.create(TestEntity.class));

        // Set a dynamic group entity filter so that member entity can join.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));

        // Verify that member has joined.
        assertEqualsIgnoringOrder(dynamicGroup.getMembers(), ImmutableList.of(member));

        Asserts.eventually(() -> member.getAttribute(Sensors.newStringSensor("member-sensor")), input -> input != null && input.length() == 15);
    }

    @Test
    public void testAddCompositeEffectorInitializer() {

        final String COMPOSITE_EFFECTOR_NAME = "compositeEffector";

        // Create a dynamic group.
        DynamicGroup dynamicGroup = app.addChild(EntitySpec.create(DynamicGroup.class));

        // Create a GroupsChangePolicy policy spec for the dynamic group.
        PolicySpec<GroupsChangePolicy> policySpec =
                PolicySpec.create(GroupsChangePolicy.class)
                        .configure(GroupsChangePolicy.GROUP, dynamicGroup)
                        .configure(GroupsChangePolicy.INITIALIZERS,
                                ImmutableList.of(
                                        ImmutableMap.of(
                                                "type", EffectorForThisTest.class.getName()),
                                        ImmutableMap.of(
                                                "type", CompositeEffector.class.getName(),
                                                "brooklyn.config", ImmutableMap.of(
                                                        "name", COMPOSITE_EFFECTOR_NAME, // <--- THIS IS THE EFFECTOR TO CALL IN THIS TEST
                                                        "effectors", ImmutableList.of(EffectorForThisTest.NAME)))));

        // Add GroupsChangePolicy to the dynamic group.
        dynamicGroup.policies().add(policySpec);

        // Create an entity to add as a dynamic group member to test GroupsChangePolicy.
        TestEntity member = app.addChild(EntitySpec.create(TestEntity.class));

        // Set a dynamic group entity filter so that member entity can join.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));

        // Verify that member has joined.
        assertEqualsIgnoringOrder(dynamicGroup.getMembers(), ImmutableList.of(member));

        // Wait for sensor that is expected to be added by EffectorForThisTest, which is expected to be called by CompositeEffector configured above.
        Asserts.eventually(() -> {

            // Call the composite effector as member got it from the GroupsChangePolicy.
            Effector<?> helloEffector = member.getEffector(COMPOSITE_EFFECTOR_NAME);
            if (!Objects.isNull(helloEffector)) {
                invokeEffector(member, helloEffector);
            }

            return member.getAttribute(Sensors.newStringSensor(EffectorForThisTest.SENSOR));
        }, EffectorForThisTest.SENSOR_VALUE::equals);
    }

    @Test
    public void testAddPolicies() {

        // Load types for OSGI to resolve on member addition in GroupsChangePolicy
        addCatalogItems("brooklyn.catalog:",
                "  items:",
                "  - id: " + InvokeEffectorOnSensorChange.class.getName(),
                "    itemType: policy",
                "    item:",
                "      type: " + InvokeEffectorOnSensorChange.class.getName());

        // Add GroupsChangePolicy to the dynamic group.
        DynamicGroup dynamicGroup = app.addChild(EntitySpec.create(DynamicGroup.class));
        PolicySpec<GroupsChangePolicy> policySpec =
                PolicySpec.create(GroupsChangePolicy.class)
                        .configure(GroupsChangePolicy.GROUP, dynamicGroup)
                        .configure(GroupsChangePolicy.POLICIES,
                                ImmutableList.of(
                                        ImmutableMap.of(
                                                "type", InvokeEffectorOnSensorChange.class.getName(),
                                                "brooklyn.config", ImmutableMap.of(
                                                        "sensor.producer", dynamicGroup,
                                                        "sensor", "service.isUp",
                                                        "effector", "stop"))));

        // Add GroupsChangePolicy to the dynamic group.
        dynamicGroup.policies().add(policySpec);

        // Create an entity to add as a dynamic group member to test GroupsChangePolicy.
        TestEntity member = app.addChild(EntitySpec.create(TestEntity.class));

        // Set a dynamic group entity filter so that member entity can join.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));

        // Verify that member has joined.
        assertEqualsIgnoringOrder(dynamicGroup.getMembers(), ImmutableList.of(member));

        // Verify that policy is added to the group member.
        Asserts.eventually(() -> member.policies().size(), size -> size == 1);
    }

    @Test
    public void testAddLocations() {

        // Load types for OSGI to resolve on member addition in GroupsChangePolicy
        addCatalogItems("brooklyn.catalog:",
                "  items:",
                "  - id: " + SshMachineLocation.class.getName(),
                "    itemType: location",
                "    item:",
                "      type: " + SshMachineLocation.class.getName());

        // Add GroupsChangePolicy to the dynamic group.
        DynamicGroup dynamicGroup = app.addChild(EntitySpec.create(DynamicGroup.class));
        PolicySpec<GroupsChangePolicy> policySpec =
                PolicySpec.create(GroupsChangePolicy.class)
                        .configure(GroupsChangePolicy.GROUP, dynamicGroup)
                        .configure(GroupsChangePolicy.LOCATIONS,
                                ImmutableList.of(
                                        ImmutableMap.of(
                                                "type", SshMachineLocation.class.getName(),
                                                "brooklyn.config", ImmutableMap.of(
                                                        "user", "user",
                                                        "address", "127.0.0.1",
                                                        "privateKeyData", "-----BEGIN RSA PRIVATE KEY-----etc.."))));

        // Add GroupsChangePolicy to the dynamic group.
        dynamicGroup.policies().add(policySpec);

        // Create an entity to add as a dynamic group member to test GroupsChangePolicy.
        TestEntity member = app.addChild(EntitySpec.create(TestEntity.class));

        // Set a dynamic group entity filter so that member entity can join.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));

        // Verify that member has joined.
        assertEqualsIgnoringOrder(dynamicGroup.getMembers(), ImmutableList.of(member));

        // Verify that location is added to the group member.
        eventually(() -> member.getLocations().size(), size -> size == 1);
    }

    /**
     * Helper to invoke effector on entity.
     */
    private void invokeEffector(final Entity entity, final Effector<?> effector) {
        final TaskAdaptable<?> stop = Entities.submit(entity, Effectors.invocation(entity, effector, ConfigBag.EMPTY));
        stop.asTask().blockUntilEnded();
    }
}