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
package org.apache.brooklyn.tasks.kubectl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.Test;

@SuppressWarnings( "UnstableApiUsage")
@Test(groups = {"Live"})
public class ContainerSensorTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testEchoPerlCommand() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerCommons.CONTAINER_IMAGE, "perl",
                ContainerCommons.CONTAINER_IMAGE_PULL_POLICY, PullPolicy.IF_NOT_PRESENT,
                ContainerCommons.COMMAND, ImmutableList.of("/bin/bash", "-c","echo " + message) ,
                ContainerSensor.SENSOR_PERIOD, "1s",
                ContainerSensor.SENSOR_NAME, "test-echo-sensor"));

        ContainerSensor<String> initializer = new ContainerSensor<>(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEventually(parentEntity, Sensors.newStringSensor("test-echo-sensor"), s -> s.contains(message));
    }

    @Test
    public void testEchoPerlCommandAndArgs() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerCommons.CONTAINER_IMAGE, "perl",
                ContainerCommons.COMMAND, ImmutableList.of("/bin/bash") ,
                ContainerCommons.ARGUMENTS, ImmutableList.of("-c", "echo " + message) ,
                ContainerSensor.SENSOR_PERIOD, "1s",
                ContainerSensor.SENSOR_NAME, "test-echo-sensor"));

        ContainerSensor<String> initializer = new ContainerSensor<>(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEventually(parentEntity, Sensors.newStringSensor("test-echo-sensor"), s -> s.contains(message));
    }

    @Test
    public void testEchoPerlArgs() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerCommons.CONTAINER_IMAGE, "perl",
                ContainerCommons.ARGUMENTS, ImmutableList.of("echo", message) ,
                ContainerSensor.SENSOR_PERIOD, "1s",
                ContainerSensor.SENSOR_NAME, "test-echo-sensor"));

        ContainerSensor<String> initializer = new ContainerSensor<>(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEventually(parentEntity, Sensors.newStringSensor("test-echo-sensor"), s -> s.contains(message));
    }

    @Test
    public void testEchoBashArgs() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerCommons.CONTAINER_IMAGE, "bash",
                ContainerCommons.ARGUMENTS, ImmutableList.of("-c", "echo " + message) ,
                ContainerSensor.SENSOR_PERIOD, "1s",
                ContainerSensor.SENSOR_NAME, "test-echo-sensor"));

        ContainerSensor<String> initializer = new ContainerSensor<>(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEventually(parentEntity, Sensors.newStringSensor("test-echo-sensor"), s -> s.contains(message));
    }

    @Test
    public void testTfVersionSensor() {
        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerCommons.CONTAINER_IMAGE, "hashicorp/terraform:1.3.0-alpha20220622",
                ContainerCommons.COMMAND, ImmutableList.of("terraform", "version" ),
                ContainerSensor.SENSOR_PERIOD, "1s",
                ContainerSensor.SENSOR_NAME, "tf-version-sensor"));

        ContainerSensor<String> initializer = new ContainerSensor<>(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEventually(parentEntity, Sensors.newStringSensor("tf-version-sensor"), s -> s.contains("Terraform"));
    }

    @Test
    public void testTriggeredContainerSensor() {
        AttributeSensor<Object> trigger = Sensors.newSensor(Object.class, "the-trigger");
        AttributeSensor<Object> triggered = Sensors.newSensor(Object.class, "triggered");
        ConfigBag parameters = ConfigBag.newInstance(MutableMap.of(
                ContainerCommons.CONTAINER_IMAGE, "stedolan/jq",
                ContainerCommons.CONTAINER_IMAGE_PULL_POLICY, PullPolicy.IF_NOT_PRESENT,
                ContainerCommons.SHELL_ENVIRONMENT, MutableMap.of("LAST_TRIGGER", DependentConfiguration.attributeWhenReady(app, trigger)),
                ContainerCommons.BASH_SCRIPT, ImmutableList.of("echo " + "$LAST_TRIGGER" + " | jq .value"),
                ContainerSensor.SENSOR_TRIGGERS, MutableList.of(MutableMap.of("entity", app.getId(), "sensor", "the-trigger")),
                ContainerSensor.SENSOR_NAME, "triggered"));

        ContainerSensor<String> initializer = new ContainerSensor<>(parameters);
        TestEntity child = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEquals(child, triggered, null);
        app.sensors().set(trigger, "{ \"name\": \"bob\", \"value\": 3 }");

        Time.sleep(Duration.ONE_SECOND);
        Dumper.dumpInfo(app);

        EntityAsserts.assertAttributeEventuallyNonNull(child, triggered);
        EntityAsserts.assertAttributeEquals(child, triggered, "3");
    }
}
