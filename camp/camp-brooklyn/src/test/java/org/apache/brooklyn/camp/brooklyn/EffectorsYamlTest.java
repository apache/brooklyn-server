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

import static com.google.common.collect.Iterables.filter;
import static org.apache.brooklyn.core.entity.EntityPredicates.displayNameEqualTo;
import static org.testng.Assert.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

@Test
public class EffectorsYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(EffectorsYamlTest.class);

    @Test
    public void testEffectorWithVoidReturnInvokedByGetConfig() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  id: entity1",
            "  brooklyn.config:",
            "    test.confName: $brooklyn:entity(\"entity1\").effector(\"myEffector\")"
        );
        TestEntity testEntity = (TestEntity)Iterables.getOnlyElement(app.getChildren());

        assertCallHistory(testEntity, "start");

        // invoke the effector
        Assert.assertNull(testEntity.getConfig(TestEntity.CONF_NAME));

        assertCallHistory(testEntity, "start", "myEffector");
    }


    @Test(enabled = false, description = "currently not possible to say '$brooklyn:entity(\"entity1\").effector:'")
    public void testEffectorMultiLine() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  id: entity1",
            "  brooklyn.config:",
            "    test.confName: ",
            "      $brooklyn:entity(\"entity1\").effector:",
            "      - myEffector"
        );
        TestEntity testEntity = (TestEntity)Iterables.getOnlyElement(app.getChildren());

        assertCallHistory(testEntity, "start");

        Assert.assertNull(testEntity.getConfig(TestEntity.CONF_NAME));

        assertCallHistory(testEntity, "start", "myEffector");
    }

    @Test
    public void testEntityEffectorWithReturn() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  id: entity1",
            "  brooklyn.config:",
            "    test.confName: ",
            "      $brooklyn:entity(\"entity1\").effector(\"identityEffector\", \"hello\")"
        );
        TestEntity testEntity = (TestEntity)Iterables.getOnlyElement(app.getChildren());
        Assert.assertEquals(testEntity.getConfig(TestEntity.CONF_NAME), "hello");
        assertCallHistory(testEntity, "start", "identityEffector");
    }

    @Test
    public void testOwnEffectorWithReturn() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  brooklyn.config:",
            "    test.confName: ",
            "      $brooklyn:effector(\"identityEffector\", \"my own effector\")"
        );
        TestEntity testEntity = (TestEntity)Iterables.getOnlyElement(app.getChildren());
        Assert.assertEquals(testEntity.getConfig(TestEntity.CONF_NAME), "my own effector");
        assertCallHistory(testEntity, "start", "identityEffector");
    }

    @Test
    public void testEffectorOnOtherEntityWithReturn() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  id: entityOne",
            "  name: entityOne",
            "- type: " + TestEntity.class.getName(),
            "  id: entityTwo",
            "  name: entityTwo",
            "  brooklyn.config:",
            "    test.confName: ",
            "      $brooklyn:entity(\"entityOne\").effector(\"identityEffector\", \"entityOne effector\")"
        );
        TestEntity entityOne = (TestEntity) filter(app.getChildren(), displayNameEqualTo("entityOne")).iterator().next();
        TestEntity entityTwo = (TestEntity) filter(app.getChildren(), displayNameEqualTo("entityTwo")).iterator().next();
        Assert.assertEquals(entityTwo.getConfig(TestEntity.CONF_NAME), "entityOne effector");
        assertCallHistory(entityOne, "start", "identityEffector");
    }

    @Test
    public void testEffectorCalledOncePerConfigKey() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  id: entity1",
            "  brooklyn.config:",
            "    test.confName: ",
            "      $brooklyn:effector(\"sequenceEffector\")",
            "    test.confObject: ",
            "      $brooklyn:effector(\"sequenceEffector\")"
        );
        TestEntity testEntity = (TestEntity)Iterables.getOnlyElement(app.getChildren());

        List<String> callHistory = testEntity.getCallHistory();
        Assert.assertFalse(callHistory.contains("myEffector"), "history = " + callHistory);

        final String firstGetConfig = testEntity.getConfig(TestEntity.CONF_NAME);
        Assert.assertEquals(firstGetConfig, "1");
        final String secondGetConfig = testEntity.getConfig(TestEntity.CONF_NAME);
        Assert.assertEquals(secondGetConfig, "1");
        Assert.assertEquals(testEntity.getConfig(TestEntity.CONF_OBJECT), Integer.valueOf(2));
        assertCallHistory(testEntity, "start", "sequenceEffector", "sequenceEffector");
    }

    @Test(groups = "Integration")
    public void testSshCommandSensorWithEffectorInEnv() throws Exception {
        final Path tempFile = Files.createTempFile("testSshCommandSensorWithEffectorInEnv", ".txt");
        getLogger().info("Temp file is {}", tempFile.toAbsolutePath());

        try {
            Entity app = createAndStartApplication(
                "location: localhost:(name=localhost)",
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  id: testEnt1",
                "  name: testEnt1",
                "- type: " + VanillaSoftwareProcess.class.getName(),
                "  id: vsp",
                "  brooklyn.config:",
                "    launch.command: echo ${MY_ENV_VAR} > " + tempFile.toAbsolutePath(),
                "    checkRunning.command: true",
                "    shell.env:",
                "      MY_ENV_VAR:" ,
                "        $brooklyn:entity(\"testEnt1\").effector(\"identityEffector\", \"from effector\")"
            );
            waitForApplicationTasks(app);

            final TestEntity testEnt1 =
                (TestEntity) Iterables.filter(app.getChildren(), displayNameEqualTo("testEnt1")).iterator().next();
            assertCallHistory(testEnt1, "start", "identityEffector");
            final String contents = new String(Files.readAllBytes(tempFile)).trim();
            assertEquals(contents, "from effector", "file contents: " + contents);

        } finally {
            Files.delete(tempFile);
        }
    }

    public static void assertCallHistory(TestEntity testEntity, String... expectedCalls) {
        List<String> callHistory = testEntity.getCallHistory();
        Assert.assertEquals(callHistory.size(), expectedCalls.length, "history = " + callHistory);
        int c = 0;
        for (String expected : expectedCalls) {
            Assert.assertEquals(callHistory.get(c++), expected, "history = " + callHistory);
        }
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
