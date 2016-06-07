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

import static org.apache.brooklyn.core.entity.trait.Startable.SERVICE_UP;
import static org.apache.brooklyn.test.framework.TargetableTestComponent.TARGET_ENTITY;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.CONTAINS;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.EQUALS;
import static org.apache.brooklyn.test.framework.TestSshCommand.ASSERT_ERR;
import static org.apache.brooklyn.test.framework.TestSshCommand.ASSERT_OUT;
import static org.apache.brooklyn.test.framework.TestSshCommand.ASSERT_STATUS;
import static org.apache.brooklyn.test.framework.TestSshCommand.COMMAND;
import static org.apache.brooklyn.test.framework.TestSshCommand.DOWNLOAD_URL;
import static org.apache.brooklyn.test.framework.TestSshCommand.SHELL_ENVIRONMENT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestSshCommandTest extends BrooklynAppUnitTestSupport {

    private TestEntity testEntity;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        LocationSpec<SshMachineLocation> machineSpec = LocationSpec.create(SshMachineLocation.class)
                .configure("address", "1.2.3.4")
                .configure("sshToolClass", RecordingSshTool.class.getName());
        testEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .location(machineSpec));
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshTool.clear();
    }
    
    @DataProvider(name = "shouldInsistOnJustOneOfCommandAndScript")
    public Object[][] createData1() {
        return new Object[][] {
                { "pwd", "pwd.sh", Boolean.FALSE },
                { null, null, Boolean.FALSE },
                { "pwd", null, Boolean.TRUE },
                { null, "pwd.sh", Boolean.TRUE }
        };
    }

    @Test(dataProvider = "shouldInsistOnJustOneOfCommandAndScript")
    public void shouldInsistOnJustOneOfCommandAndScript(String command, String script, boolean valid) throws Exception {
        Path scriptPath = null;
        String scriptUrl = null;
        if (null != script) {
            scriptPath = createTempScript("pwd", "pwd");
            scriptUrl = "file:" + scriptPath;
        }

        try {
            app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
                    .configure(TARGET_ENTITY, testEntity)
                    .configure(COMMAND, command)
                    .configure(DOWNLOAD_URL, scriptUrl));

            app.start(ImmutableList.<Location>of());
            if (!valid) {
                Asserts.shouldHaveFailedPreviously();
            }

        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Must specify exactly one of download.url and command");

        } finally {
            if (null != scriptPath) {
                Files.delete(scriptPath);
            }
        }
    }

    @Test
    public void shouldSucceedUsingSuccessfulExitAsDefaultCondition() {
        TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, "uptime"));

        app.start(ImmutableList.<Location>of());

        assertServiceHealthy(test);
        assertThat(RecordingSshTool.getLastExecCmd().commands).isEqualTo(ImmutableList.of("uptime"));
    }

    @Test
    public void shouldFailUsingSuccessfulExitAsDefaultCondition() {
        String cmd = "commandExpectedToFail-" + Identifiers.randomLong();
        RecordingSshTool.setCustomResponse(cmd, new RecordingSshTool.CustomResponse(1, null, null));
        
        TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, cmd));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable t) {
            Asserts.expectedFailureContains(t, "exit code equals 0");
        }

        assertServiceFailed(test);
        assertThat(RecordingSshTool.getLastExecCmd().commands).isEqualTo(ImmutableList.of(cmd));
    }

    @Test
    public void shouldMatchStdoutAndStderr() {
        String cmd = "stdoutAndStderr-" + Identifiers.randomLong();
        RecordingSshTool.setCustomResponse(cmd, new RecordingSshTool.CustomResponse(0, "mystdout", "mystderr"));
        
        TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, cmd)
            .configure(ASSERT_OUT, makeAssertions(ImmutableMap.of(CONTAINS, "mystdout")))
            .configure(ASSERT_ERR, makeAssertions(ImmutableMap.of(CONTAINS, "mystderr"))));

        app.start(ImmutableList.<Location>of());

        assertServiceHealthy(test);
    }

    @Test
    public void shouldFailOnUnmatchedStdout() {
        String cmd = "stdoutAndStderr-" + Identifiers.randomLong();
        RecordingSshTool.setCustomResponse(cmd, new RecordingSshTool.CustomResponse(0, "wrongstdout", null));
        
        TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, cmd)
            .configure(ASSERT_OUT, makeAssertions(ImmutableMap.of(CONTAINS, "mystdout"))));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable t) {
            Asserts.expectedFailureContains(t, "stdout contains mystdout");
        }

        assertServiceFailed(test);
    }

    @Test
    public void shouldFailOnUnmatchedStderr() {
        String cmd = "stdoutAndStderr-" + Identifiers.randomLong();
        RecordingSshTool.setCustomResponse(cmd, new RecordingSshTool.CustomResponse(0, null, "wrongstderr"));
        
        TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, cmd)
            .configure(ASSERT_ERR, makeAssertions(ImmutableMap.of(CONTAINS, "mystderr"))));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable t) {
            Asserts.expectedFailureContains(t, "stderr contains mystderr");
        }

        assertServiceFailed(test);
    }

    @Test
    public void shouldNotBeUpIfAssertionsFail() {
        Map<String, Object> equalsOne = MutableMap.of();
        equalsOne.put(EQUALS, 1);

        Map<String, Object> equals255 = MutableMap.of();
        equals255.put(EQUALS, 255);

        TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, "uptime")
            .configure(ASSERT_STATUS, makeAssertions(equalsOne, equals255)));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "exit code equals 1", "exit code equals 255");
        }

        assertServiceFailed(test);
    }

    @Test
    public void shouldInvokeScript() throws Exception {
        String text = "hello world";
        Path testScript = createTempScript("script", "echo " + text);

        try {
            Map<String, Object> equalsZero = MutableMap.of();
            equalsZero.put(EQUALS, 0);

            Map<String, Object> containsText = MutableMap.of();
            containsText.put(CONTAINS, text);

            TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
                .configure(TARGET_ENTITY, testEntity)
                .configure(DOWNLOAD_URL, "file:" + testScript)
                .configure(ASSERT_STATUS, makeAssertions(equalsZero)));

            app.start(ImmutableList.<Location>of());

            assertServiceHealthy(test);
            assertThat(RecordingSshTool.getLastExecCmd().commands.toString()).contains("TestSshCommandTest-script");

        } finally {
            Files.delete(testScript);
        }
    }

    @Test
    public void shouldFailIfTestEntityHasNoMachine() throws Exception {
        TestEntity testEntityWithNoMachine = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntityWithNoMachine)
            .configure(COMMAND, "mycmd"));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No instances of class "+SshMachineLocation.class.getName()+" available");
        }

        assertServiceFailed(test);
    }
    
    @Test
    public void shouldIncludeEnv() throws Exception {
        Map<String, Object> env = ImmutableMap.<String, Object>of("ENV1", "val1", "ENV2", "val2");
        
        TestSshCommand test = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, "mycmd")
            .configure(SHELL_ENVIRONMENT, env));

        app.start(ImmutableList.<Location>of());

        assertServiceHealthy(test);
        
        ExecCmd cmdExecuted = RecordingSshTool.getLastExecCmd();
        assertThat(cmdExecuted.commands).isEqualTo(ImmutableList.of("mycmd"));
        assertThat(cmdExecuted.env).isEqualTo(env);
    }

    private Path createTempScript(String filename, String contents) {
        try {
            Path tempFile = Files.createTempFile("TestSshCommandTest-" + filename, ".sh");
            Files.write(tempFile, contents.getBytes());
            return tempFile;
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }
    
    private void assertServiceFailed(TestSshCommand test) {
        assertThat(test.sensors().get(SERVICE_UP)).isFalse()
            .withFailMessage("Service should be down");
        assertThat(ServiceStateLogic.getExpectedState(test)).isEqualTo(Lifecycle.ON_FIRE)
            .withFailMessage("Service should be marked on fire");
    }

    private void assertServiceHealthy(TestSshCommand test) {
        assertThat(test.sensors().get(SERVICE_UP)).isTrue()
            .withFailMessage("Service should be up");
        assertThat(ServiceStateLogic.getExpectedState(test)).isEqualTo(Lifecycle.RUNNING)
            .withFailMessage("Service should be marked running");
    }

    private List<Map<String, ?>> makeAssertions(Map<String, ?> map) {
        return ImmutableList.<Map<String, ?>>of(map);
    }

    private List<Map<String, ?>> makeAssertions(Map<String, ?> map1, Map<String, ?> map2) {
        return ImmutableList.of(map1, map2);
    }
}
