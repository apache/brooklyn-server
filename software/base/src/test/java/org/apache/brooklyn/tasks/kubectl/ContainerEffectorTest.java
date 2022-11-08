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
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.testng.Assert.assertTrue;

/**
 * These tests require kubectl installed and working locally, eg minikube, or docker desktop
 */
@SuppressWarnings( "UnstableApiUsage")
@Test(groups = {"Live"})
public class ContainerEffectorTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testEchoPerlCommand() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerEffector.EFFECTOR_NAME, "test-container-effector",
                ContainerCommons.CONTAINER_IMAGE, "perl",
                ContainerCommons.CONTAINER_IMAGE_PULL_POLICY, PullPolicy.IF_NOT_PRESENT,
//                ContainerCommons.COMMAND, ImmutableList.of("/bin/bash", "-c", "echo " + message),
                ContainerCommons.BASH_SCRIPT, "echo "+message+" ${HELLO}",
                BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("HELLO", "WORLD")));

        ContainerEffector initializer = new ContainerEffector(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        Object output = Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        assertTrue(output.toString().contains(message+" WORLD"));
    }

    @Test
    public void testEchoBashCommandAsContainerEffector() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();
        Object output = doTestEchoBashCommand(() -> {
            ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                    ContainerEffector.EFFECTOR_NAME, "test-container-effector",
                    ContainerCommons.CONTAINER_IMAGE, "bash",
                    ContainerCommons.ARGUMENTS, ImmutableList.of("-c", "echo " + message)));

            return new ContainerEffector(parameters);
        });
        Asserts.assertEquals(output.toString().trim(), message);
    }

    @Test
    public void testEchoBashCommandAsContainerEffectorWithVar() {
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();
        Object output = doTestEchoBashCommand(() -> {
            ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                    ContainerEffector.EFFECTOR_NAME, "test-container-effector",
                    ContainerCommons.CONTAINER_IMAGE, "bash",
                    ContainerCommons.ARGUMENTS, ImmutableList.of("-c", "echo " + message + " ${VAR}"),
                    BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("VAR", "world")));

            return new ContainerEffector(parameters);
        });
        Asserts.assertEquals(output.toString().trim(), message + " world");
    }

    // `bash` installs to /usr/local/bin/bash but we hardcode /bin/bash so can't use `bash`
    public static final String BASH_SCRIPT_CONTAINER = "perl";

    @Test
    public void testEchoBashCommandAsWorkflowEffector() {
        WorkflowBasicTest.addRegisteredTypeBean(mgmt, "container", ContainerWorkflowStep.class);
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        Object output = doTestEchoBashCommand(() -> {
            ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                    WorkflowEffector.EFFECTOR_NAME, "test-container-effector",
                    WorkflowEffector.STEPS, MutableList.of("container " + BASH_SCRIPT_CONTAINER + " echo " + message)));

            return new WorkflowEffector(parameters);
        });
        Asserts.assertEquals( ((Map)output).get("stdout").toString().trim(), message);
    }

    @Test
    public void testEchoBashCommandAsWorkflowEffectorWithVar() {
        WorkflowBasicTest.addRegisteredTypeBean(mgmt, "container", ContainerWorkflowStep.class);
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        Object output = doTestEchoBashCommand(() -> {
            ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                    WorkflowEffector.EFFECTOR_NAME, "test-container-effector",
                    WorkflowEffector.STEPS, MutableList.of(
                            MutableMap.<String, Object>of("s", "container " + BASH_SCRIPT_CONTAINER + " echo " + message + " $VAR",
                                    "input",
                                    MutableMap.of("env", MutableMap.of("VAR", "world")),
                                    "output", "${stdout}"))));

            return new WorkflowEffector(parameters);
        });
        Asserts.assertEquals(output.toString().trim(), message + " world");
    }

    @Test
    public void testEchoBashCommandAsWorkflowEffectorWithDollarBraceVar() {
        // we cannot say `echo " + message + " ${VAR}"` because Freemarker interprets that
        // instead the pattern is to write the script to a var

        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        WorkflowBasicTest.addRegisteredTypeBean(mgmt, "container", ContainerWorkflowStep.class);

        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        Object output = doTestEchoBashCommand(() -> {
            ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                    WorkflowEffector.EFFECTOR_NAME, "test-container-effector",
                    WorkflowEffector.STEPS, MutableList.of(
                            "let script = \"echo "+message+" ${VAR}\" ;",
                            MutableMap.<String, Object>of("s", "container " + BASH_SCRIPT_CONTAINER + " ${script}",
                                    "input",
                                    MutableMap.of("env", MutableMap.of("VAR", "world")),
                                    "output", "${stdout}"))));

            return new WorkflowEffector(parameters);
        });
        Asserts.assertEquals(output.toString().trim(), message + " world");
    }

    protected Object doTestEchoBashCommand(Supplier<EntityInitializer> initializerMaker) {
        return doTestEchoBashCommand(app, initializerMaker, null);
    }

    // used also in WorkflowYamlTest
    public static Object doTestEchoBashCommand(TestApplication app, Supplier<EntityInitializer> initializerMaker, Consumer<TestEntity> appCustomizer) {
        EntityInitializer initializer = initializerMaker.get();

        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        if (appCustomizer!=null) appCustomizer.accept(parentEntity);
        return Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
    }

    @Test
    public void testEchoVarBashCommand() {
        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerEffector.EFFECTOR_NAME, "test-container-effector",
                ContainerCommons.CONTAINER_IMAGE, "bash",
                ContainerCommons.ARGUMENTS, ImmutableList.of( "-c", "echo $HELLO"),
                BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("HELLO", "WORLD")));

        ContainerEffector initializer = new ContainerEffector(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        Object output = Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        assertTrue(output.toString().contains("WORLD"));
    }

    @Test
    public void testEchoMultiBashCommand() {
        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerEffector.EFFECTOR_NAME, "test-container-effector",
                ContainerCommons.CONTAINER_IMAGE, "bash",
                ContainerCommons.ARGUMENTS, ImmutableList.of( "-c", "date; echo $HELLO"),
                BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("HELLO", "WORLD")));

        ContainerEffector initializer = new ContainerEffector(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        Object output = Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        assertTrue(output.toString().contains("WORLD"));
    }

    @Test
    public void testEchoMultiBashCommandWithEnv() {
        ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                ContainerEffector.EFFECTOR_NAME, "test-container-effector",
                ContainerCommons.CONTAINER_IMAGE, "bash",
                ContainerCommons.ARGUMENTS, ImmutableList.of( "-c", "date; echo $HELLO"),
                BrooklynConfigKeys.SHELL_ENVIRONMENT, ImmutableMap.<String, Object>of("HELLO", "$brooklyn:config(\"hello\")")));

        ContainerEffector initializer = new ContainerEffector(parameters);
        TestEntity parentEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class).configure("hello", "world").addInitializer(initializer));
        app.start(ImmutableList.of());

        EntityAsserts.assertAttributeEqualsEventually(parentEntity, Attributes.SERVICE_UP, true);
        Object output = Entities.invokeEffector(app, parentEntity, parentEntity.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        assertTrue(output.toString().contains("WORLD"), "Wrong output: "+output);
    }
}
