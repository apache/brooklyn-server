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
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * These tests require Minikube installed locally
 */
@SuppressWarnings( "UnstableApiUsage")
@Test(groups = {"Live"})
public class DockerEffectorTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testEchoPerlCommand() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                DockerEffector.EFFECTOR_NAME, "test-container-effector",
                ContainerCommons.CONTAINER_IMAGE, "perl",
                ContainerCommons.CONTAINER_IMAGE_PULL_POLICY, PullPolicy.IF_NOT_PRESENT,
                ContainerCommons.COMMANDS, ImmutableList.of("/bin/bash", "-c", "echo " + message),
                BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("HELLO", "WORLD")));

        DockerEffector initializer = new DockerEffector(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        Object output = Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        assertTrue(output.toString().contains(message));
    }

    @Test
    public void testEchoBashCommand() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                DockerEffector.EFFECTOR_NAME, "test-container-effector",
                ContainerCommons.CONTAINER_IMAGE, "bash",
                ContainerCommons.ARGUMENTS, ImmutableList.of( "-c", "echo " + message),
                BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("HELLO", "WORLD")));

        DockerEffector initializer = new DockerEffector(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        Object output = Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        assertTrue(output.toString().contains(message));
    }

    @Test
    public void testEchoVarBashCommand() {
        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                DockerEffector.EFFECTOR_NAME, "test-container-effector",
                ContainerCommons.CONTAINER_IMAGE, "bash",
                ContainerCommons.ARGUMENTS, ImmutableList.of( "-c", "echo $HELLO"),
                BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("HELLO", "WORLD")));

        DockerEffector initializer = new DockerEffector(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        Object output = Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        assertTrue(output.toString().contains("WORLD"));
    }

    @Test
    public void testEchoMultiBashCommand() {
        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                DockerEffector.EFFECTOR_NAME, "test-container-effector",
                ContainerCommons.CONTAINER_IMAGE, "bash",
                ContainerCommons.ARGUMENTS, ImmutableList.of( "-c", "date; echo $HELLO"),
                BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("HELLO", "WORLD")));

        DockerEffector initializer = new DockerEffector(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        Object output = Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        assertTrue(output.toString().contains("WORLD"));
    }
}
