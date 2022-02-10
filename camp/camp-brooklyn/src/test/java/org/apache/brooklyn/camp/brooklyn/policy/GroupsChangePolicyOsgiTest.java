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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.effector.CompositeEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.*;
import org.apache.brooklyn.core.objs.AdjunctType;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.core.sensor.password.CreatePasswordSensor;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.enricher.stock.Transformer;
import org.apache.brooklyn.entity.group.DynamicGroup;
import org.apache.brooklyn.entity.group.GroupsChangePolicy;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.policy.InvokeEffectorOnSensorChange;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.brooklyn.test.Asserts.*;

/**
 *  Tests for adding {@link GroupsChangePolicy#INITIALIZERS}, {@link GroupsChangePolicy#LOCATIONS},
 *  {@link GroupsChangePolicy#ENRICHERS} and {@link GroupsChangePolicy#POLICIES} to members once joined. Config keys and
 *  attributes are expected to be resolved in the member context in all cases.
 */
public class GroupsChangePolicyOsgiTest extends AbstractYamlTest {

    private ExecutorService executor;

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newCachedThreadPool();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (executor != null) executor.shutdownNow();
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

    /**
     * Tests {@link GroupsChangePolicy#INITIALIZERS} of the member while joining the {@link DynamicGroup},
     * {@link StaticSensor} in particular.
     */
    @Test
    public void testAddInitializers() throws Exception {

        // Member properties, these are expected to be resolved with DSL in the member context.
        final String MEMBER_SENSOR_VALUE = "member-sensor-value";

        // Group properties, these should not preempt member properties.
        final String GROUP_SENSOR_VALUE = "group-sensor-value";

        // Config names.
        final String SENSOR_TO_COPY_FROM = "sensor-to-copy-from";
        final String MEMBER_SENSOR_TO_CREATE = "sensor-to-create";

        // Blueprint to test.
        Pair<DynamicGroup, Entity> pair = createAppAndGetGroupAndMember(Joiner.on("\n").join(
                "services:",
                "- type: " + DynamicGroup.class.getName(),
                "  brooklyn.policies:",
                "  - type: " + GroupsChangePolicy.class.getName(),
                "    brooklyn.config:",
                "      group: $brooklyn:self()",
                "      member.initializers:",
                "      - type: " + StaticSensor.class.getName(),
                "        brooklyn.config:",
                "          name: " + MEMBER_SENSOR_TO_CREATE,
                "          target.type: string",
                "          static.value: $brooklyn:attributeWhenReady(\"" + SENSOR_TO_COPY_FROM + "\")",
                "- type: " + TestEntity.class.getName()));

        DynamicGroup dynamicGroup = pair.getLeft();
        Entity member = pair.getRight();

        // Verify that member has joined.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));
        Asserts.assertEquals(dynamicGroup.getMembers().size(), 1);

        // Emmit dynamic attributes for both: group and its member.
        executor.submit(() -> dynamicGroup.sensors().set(Sensors.newStringSensor(SENSOR_TO_COPY_FROM), GROUP_SENSOR_VALUE));
        executor.submit(() -> member.sensors().set(Sensors.newStringSensor(SENSOR_TO_COPY_FROM), MEMBER_SENSOR_VALUE));

        // Verify that location is added to the group member with expected member properties.
        Asserts.eventually(() -> member.getAttribute(Sensors.newStringSensor(MEMBER_SENSOR_TO_CREATE)), MEMBER_SENSOR_VALUE::equals);
    }

    /**
     * Tests {@link GroupsChangePolicy#INITIALIZERS} of the member while joining the {@link DynamicGroup},
     * {@link CreatePasswordSensor} in particular.
     */
    @Test
    public void testAddInitializers_CreatePasswordSensor() throws Exception {

        // Config names.
        final Integer SENSOR_VALUE = 127;
        final String MEMBER_SENSOR_TO_CREATE = "sensor-to-create";

        // Blueprint to test.
        Pair<DynamicGroup, Entity> pair = createAppAndGetGroupAndMember(Joiner.on("\n").join(
                "services:",
                "- type: " + DynamicGroup.class.getName(),
                "  brooklyn.policies:",
                "  - type: " + GroupsChangePolicy.class.getName(),
                "    brooklyn.config:",
                "      group: $brooklyn:self()",
                "      member.initializers:",
                "      - type: " + CreatePasswordSensor.class.getName(),
                "        brooklyn.config:",
                "          name: " + MEMBER_SENSOR_TO_CREATE,
                "          password.length: " + SENSOR_VALUE,
                "- type: " + TestEntity.class.getName()));

        DynamicGroup dynamicGroup = pair.getLeft();
        Entity member = pair.getRight();

        // Verify that member has joined.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));
        Asserts.assertEquals(dynamicGroup.getMembers().size(), 1);

        // Verify expected length of the generated password.
        Asserts.eventually(() -> member.getAttribute(Sensors.newStringSensor(MEMBER_SENSOR_TO_CREATE)), input -> input != null && input.length() == SENSOR_VALUE);
    }

    /**
     * Tests {@link GroupsChangePolicy#INITIALIZERS} of the member while joining the {@link DynamicGroup},
     * {@link CompositeEffector} in particular since it has a peculiar way to resolve effectors to call together.
     */
    @Test
    public void testAddInitializers_CompositeEffector() throws Exception {

        // Config names.
        final String COMPOSITE_EFFECTOR_NAME = "composite-effector";

        // Blueprint to test.
        Pair<DynamicGroup, Entity> pair = createAppAndGetGroupAndMember(Joiner.on("\n").join(
                "services:",
                "- type: " + DynamicGroup.class.getName(),
                "  brooklyn.policies:",
                "  - type: " + GroupsChangePolicy.class.getName(),
                "    brooklyn.config:",
                "      group: $brooklyn:self()",
                "      member.initializers:",
                "      - type: " + EffectorForThisTest.class.getName(),
                "      - type: " + CompositeEffector.class.getName(),
                "        brooklyn.config:",
                "          name: " + COMPOSITE_EFFECTOR_NAME, // <--- THIS IS THE EFFECTOR TO CALL IN THIS TEST
                "          effectors:",
                "            - " + EffectorForThisTest.NAME,
                "- type: " + TestEntity.class.getName()));

        DynamicGroup dynamicGroup = pair.getLeft();
        Entity member = pair.getRight();

        // Verify that member has joined.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));
        Asserts.assertEquals(dynamicGroup.getMembers().size(), 1);

        // Wait for sensor that is expected to be added by EffectorForThisTest, which is expected to be called by CompositeEffector configured above.
        Asserts.eventually(() -> {

            // Call the composite effector as member got it from the GroupsChangePolicy.
            Effector<?> helloEffector = ((EntityInternal) member).getEffector(COMPOSITE_EFFECTOR_NAME);
            if (!Objects.isNull(helloEffector)) {
                invokeEffector(member, helloEffector); // <-- THE EFFECTOR TO CALL IN THIS TEST
            }

            return member.getAttribute(Sensors.newStringSensor(EffectorForThisTest.SENSOR));
        }, EffectorForThisTest.SENSOR_VALUE::equals);
    }

    /**
     * Tests {@link GroupsChangePolicy#POLICIES} of the member while joining the {@link DynamicGroup},
     * {@link InvokeEffectorOnSensorChange} in particular.
     */
    @Test
    public void testAddPolicies() throws Exception {

        // Load types for OSGI to resolve on member addition in GroupsChangePolicy
        addClassNameType(InvokeEffectorOnSensorChange.class, "policy");

        // Member properties, these are expected to be resolved with DSL in the member context.
        final String MEMBER_SENSOR_TO_WATCH = "member-sensor";
        final String MEMBER_EFFECTOR_TO_CALL = "member-effector";

        // Group properties, these should not preempt member properties.
        final String GROUP_SENSOR_TO_WATCH = "group-sensor";
        final String GROUP_EFFECTOR_TO_CALL = "group-effector";

        // Config names.
        final String CONFIG_EFFECTOR = "effector";
        final String CONFIG_SENSOR = "sensor";
        final String CONFIG_SENSOR_PRODUCER = "sensor.producer";
        final String EFFECTOR_TO_CALL = "effector-to-call";
        final String SENSOR_TO_WATCH = "sensor-to-watch";

        // Blueprint to test.
        Pair<DynamicGroup, Entity> pair = createAppAndGetGroupAndMember(Joiner.on("\n").join(
                        "services:",
                        "- type: " + DynamicGroup.class.getName(),
                        "  brooklyn.config:",
                        "    " + SENSOR_TO_WATCH + ": " + GROUP_SENSOR_TO_WATCH,
                        "  brooklyn.policies:",
                        "  - type: " + GroupsChangePolicy.class.getName(),
                        "    brooklyn.config:",
                        "      group: $brooklyn:self()",
                        "      member.policies:",
                        "      - type: " + InvokeEffectorOnSensorChange.class.getName(),
                        "        brooklyn.config:",
                        "          " + CONFIG_SENSOR_PRODUCER + ": $brooklyn:self()", // $brooklyn:self() is referring to member entity
                        "          " + CONFIG_SENSOR + ": $brooklyn:config(\"" + SENSOR_TO_WATCH + "\")",
                        "          " + CONFIG_EFFECTOR + ": $brooklyn:attributeWhenReady(\"" + EFFECTOR_TO_CALL + "\")",
                        "- type: " + TestEntity.class.getName(),
                        "  brooklyn.config:",
                        "    " + SENSOR_TO_WATCH + ": " + MEMBER_SENSOR_TO_WATCH));

        DynamicGroup dynamicGroup = pair.getLeft();
        Entity member = pair.getRight();

        // Verify that member has joined.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));
        Asserts.assertEquals(dynamicGroup.getMembers().size(), 1);

        // Emmit dynamic attributes for both: group and its member.
        executor.submit(() -> dynamicGroup.sensors().set(Sensors.newStringSensor(EFFECTOR_TO_CALL), GROUP_EFFECTOR_TO_CALL));
        executor.submit(() -> member.sensors().set(Sensors.newStringSensor(EFFECTOR_TO_CALL), MEMBER_EFFECTOR_TO_CALL));

        // Verify that location is added to the group member with expected member properties.
        eventually(member::policies, policies -> {
            if (policies.size() != 1) return false;
            InvokeEffectorOnSensorChange policy = (InvokeEffectorOnSensorChange) policies.iterator().next();
            AdjunctType policyAdjunctType = policy.getAdjunctType();
            final Object sensor = policy.config().get(policyAdjunctType.getConfigKey(CONFIG_SENSOR));
            final Object sensorProducer = policy.config().get(policyAdjunctType.getConfigKey(CONFIG_SENSOR_PRODUCER));
            final Object effector = policy.config().get(policyAdjunctType.getConfigKey(CONFIG_EFFECTOR));
            return member.equals(sensorProducer) && MEMBER_SENSOR_TO_WATCH.equals(sensor) && MEMBER_EFFECTOR_TO_CALL.equals(effector);
        });
    }

    /**
     * Tests {@link GroupsChangePolicy#LOCATIONS} of the member while joining the {@link DynamicGroup},
     * {@link SshMachineLocation} in particular since this likely to be the most common use-case to add to a member.
     */
    @Test
    public void testAddLocations() throws Exception {

        // Load types for OSGI to resolve on member addition in GroupsChangePolicy
        addClassNameType(SshMachineLocation.class, "location");

        // Member properties, these are expected to be resolved with DSL in the member context.
        final String MEMBER_OS_USER = "member-os-user";
        final String MEMBER_IP_ADDRESS = "127.1.2.3";

        // Group properties, these should not preempt member properties.
        final String GROUP_OS_USER = "group-os-user";
        final String GROUP_IP_ADDRESS = "127.1.2.4";

        // Config names.
        final String OS_USER = "os-user";
        final String IP_ADDRESS = "ip-address";

        // Blueprint to test.
        Pair<DynamicGroup, Entity> pair = createAppAndGetGroupAndMember(Joiner.on("\n").join(
                "services:",
                "- type: " + DynamicGroup.class.getName(),
                "  brooklyn.config:",
                "    " + OS_USER + ": " + GROUP_OS_USER,
                "  brooklyn.policies:",
                "  - type: " + GroupsChangePolicy.class.getName(),
                "    brooklyn.config:",
                "      group: $brooklyn:self()",
                "      member.locations:",
                "      - type: " + SshMachineLocation.class.getName(),
                "        brooklyn.config:",
                "          user: $brooklyn:config(\"" + OS_USER + "\")",
                "          address: $brooklyn:attributeWhenReady(\"" + IP_ADDRESS + "\")",
                "          privateKeyData: -----BEGIN RSA PRIVATE KEY-----etc..",
                "- type: " + TestEntity.class.getName(),
                "  brooklyn.config:",
                "    " + OS_USER + ": " + MEMBER_OS_USER));

        DynamicGroup dynamicGroup = pair.getLeft();
        Entity member = pair.getRight();

        // Emmit dynamic attributes for both: group and its member.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));
        Asserts.assertEquals(dynamicGroup.getMembers().size(), 1);

        // Emmit host address attribute for both: group and its member.
        executor.submit(() -> dynamicGroup.sensors().set(Sensors.newStringSensor(IP_ADDRESS), GROUP_IP_ADDRESS));
        executor.submit(() -> member.sensors().set(Sensors.newStringSensor(IP_ADDRESS), MEMBER_IP_ADDRESS));

        // Verify that location is added to the group member with expected member properties.
        Asserts.eventually(member::getLocations, locations -> {
            if (locations.size() != 1) return false;
            SshMachineLocation location = (SshMachineLocation) locations.stream().findFirst().get();
            return MEMBER_IP_ADDRESS.equals(location.getAddress().getHostAddress()) && MEMBER_OS_USER.equals(location.getUser());
        });
    }

    /**
     * Tests {@link GroupsChangePolicy#ENRICHERS} of the member while joining the {@link DynamicGroup},
     * {@link Transformer} in particular.
     */
    @Test
    public void testAddEnrichers() throws Exception {

        // Load types for OSGI to resolve on member addition in GroupsChangePolicy
        addClassNameType(Transformer.class, "enricher");

        // Member properties, these are expected to be resolved with DSL in the member context.
        final String MEMBER_IP_ADDRESS = "127.1.2.3";

        // Group properties, these should not preempt member properties.
        final String GROUP_IP_ADDRESS = "127.1.2.4";

        // Config names.
        final String SOURCE_IP_ADDRESS = "source-ip-address";
        final String TARGET_IP_ADDRESS = "target-ip-address";

        // Blueprint to test.
        Pair<DynamicGroup, Entity> pair = createAppAndGetGroupAndMember(Joiner.on("\n").join(
                "services:",
                "- type: " + DynamicGroup.class.getName(),
                "  brooklyn.policies:",
                "  - type: " + GroupsChangePolicy.class.getName(),
                "    brooklyn.config:",
                "      group: $brooklyn:self()",
                "      member.enrichers:",
                "      - type: " + Transformer.class.getName(),
                "        brooklyn.config:",
                "          enricher.sourceSensor: $brooklyn:sensor(\"" + SOURCE_IP_ADDRESS + "\")",
                "          enricher.targetSensor: $brooklyn:sensor(\"" + TARGET_IP_ADDRESS + "\")",
                "          enricher.targetValue: $brooklyn:attributeWhenReady(\"" + SOURCE_IP_ADDRESS + "\")",
                "- type: " + TestEntity.class.getName()));

        DynamicGroup dynamicGroup = pair.getLeft();
        Entity member = pair.getRight();

        // Emmit host address attribute for both: group and its member.
        executor.submit(() -> dynamicGroup.sensors().set(Sensors.newStringSensor(SOURCE_IP_ADDRESS), GROUP_IP_ADDRESS));
        executor.submit(() -> member.sensors().set(Sensors.newStringSensor(SOURCE_IP_ADDRESS), MEMBER_IP_ADDRESS));

        // Verify expected sensor value copied with the Transformer.
        Asserts.eventually(() -> member.getAttribute(Sensors.newStringSensor(TARGET_IP_ADDRESS)), MEMBER_IP_ADDRESS::equals);
    }

    /** Helper to invoke effector on entity. */
    private void invokeEffector(final Entity entity, final Effector<?> effector) {
        final TaskAdaptable<?> stop = Entities.submit(entity, Effectors.invocation(entity, effector, ConfigBag.EMPTY));
        stop.asTask().blockUntilEnded();
    }

    /** Helper to add required items to catalog. */
    public void addClassNameType(Class<?> type, String itemType) {
        addCatalogItems(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + type.getName(),
                "    itemType: " + itemType,
                "    item:",
                "      type: " + type.getName());
    }

    /** Creates app from a blueprint that expects a {@link DynamicGroup} member and {@link TestEntity} in services */
    Pair<DynamicGroup, Entity> createAppAndGetGroupAndMember(String yaml) throws Exception {

        final Entity app = createStartWaitAndLogApplication(yaml);
        Asserts.assertNotNull(app);

        final DynamicGroup dynamicGroup = (DynamicGroup) Iterables.getFirst(app.getChildren(), null);
        Asserts.assertNotNull(dynamicGroup);

        final TestEntity member = (TestEntity) Iterables.getLast(app.getChildren());
        Asserts.assertNotNull(member);

        // Verify that member has joined.
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(member.getId()));
        Asserts.assertEquals(dynamicGroup.getMembers().size(), 1);

        return Pair.of(dynamicGroup, member);
    }
}