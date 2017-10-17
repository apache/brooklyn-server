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

import static org.apache.brooklyn.core.entity.EntityAsserts.assertEntityFailed;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertEntityHealthy;
import static org.apache.brooklyn.test.framework.BaseTest.TIMEOUT;
import static org.apache.brooklyn.test.framework.TargetableTestComponent.TARGET_ENTITY;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.CONTAINS;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.EQUALS;
import static org.apache.brooklyn.test.framework.TestWinrmCommand.ASSERT_ERR;
import static org.apache.brooklyn.test.framework.TestWinrmCommand.ASSERT_OUT;
import static org.apache.brooklyn.test.framework.TestWinrmCommand.ASSERT_STATUS;
import static org.apache.brooklyn.test.framework.TestWinrmCommand.COMMAND;
import static org.apache.brooklyn.test.framework.TestWinrmCommand.PS_SCRIPT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool.ExecType;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestWinrmCommandTest extends BrooklynAppUnitTestSupport {

    private TestEntity testEntity;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        RecordingWinRmTool.clear();
        LocationSpec<WinRmMachineLocation> machineSpec = LocationSpec.create(WinRmMachineLocation.class)
                .configure("address", "1.2.3.4")
                .configure(WinRmMachineLocation.WINRM_TOOL_CLASS, RecordingWinRmTool.class.getName());
        testEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .location(machineSpec));
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingWinRmTool.clear();
    }
    
    @DataProvider(name = "insistOnJustOneOfCommandAndPsScript")
    public Object[][] commandAndPsScriptPermutations() {
        return new Object[][] {
                { "pwd", "pwd", Boolean.FALSE },
                { null, null, Boolean.FALSE },
                { "pwd", null, Boolean.TRUE },
                { null, "pwd", Boolean.TRUE }
        };
    }

    @Test(dataProvider = "insistOnJustOneOfCommandAndPsScript")
    public void shouldInsistOnJustOneOfCommandAndPsScript(String command, String psScript, boolean valid) throws Exception {
        try {
            app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
                    .configure(TARGET_ENTITY, testEntity)
                    .configure(COMMAND, command)
                    .configure(PS_SCRIPT, psScript));

            app.start(ImmutableList.<Location>of());
            if (!valid) {
                Asserts.shouldHaveFailedPreviously();
            }

        } catch (Exception e) {
            if (valid) {
                throw e;
            }
            Asserts.expectedFailureContains(e, "Must specify exactly one of psScript and command");
        }
    }
    
    @Test
    public void shouldInvokePsScript() throws Exception {
        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(PS_SCRIPT, "uptime"));

        app.start(ImmutableList.<Location>of());

        assertEntityHealthy(test);
        assertThat(RecordingWinRmTool.getLastExec().commands.toString()).contains("uptime");
        assertThat(RecordingWinRmTool.getLastExec().type).isEqualTo(ExecType.POWER_SHELL);
    }

    @Test
    public void shouldSucceedUsingSuccessfulExitAsDefaultCondition() {
        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, "uptime"));

        app.start(ImmutableList.<Location>of());

        assertEntityHealthy(test);
        assertThat(RecordingWinRmTool.getLastExec().commands).isEqualTo(ImmutableList.of("uptime"));
        assertThat(RecordingWinRmTool.getLastExec().type).isEqualTo(ExecType.COMMAND);
    }

    @Test
    public void shouldFailUsingSuccessfulExitAsDefaultCondition() {
        String cmd = "commandExpectedToFail-" + Identifiers.randomLong();
        RecordingWinRmTool.setCustomResponse(cmd, new RecordingWinRmTool.CustomResponse(1, null, null));
        
        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
            .configure(TestWinrmCommand.ITERATION_LIMIT, 1)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, cmd));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable t) {
            Asserts.expectedFailureContains(t, "exit code expected equals 0 but found 1");
        }

        assertEntityFailed(test);
        assertThat(RecordingWinRmTool.getLastExec().commands).isEqualTo(ImmutableList.of(cmd));
    }

    @Test
    public void shouldMatchStdoutAndStderr() {
        String cmd = "stdoutAndStderr-" + Identifiers.randomLong();
        RecordingWinRmTool.setCustomResponse(cmd, new RecordingWinRmTool.CustomResponse(0, "mystdout", "mystderr"));
        
        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, cmd)
            .configure(ASSERT_OUT, makeAssertions(ImmutableMap.of(CONTAINS, "mystdout")))
            .configure(ASSERT_ERR, makeAssertions(ImmutableMap.of(CONTAINS, "mystderr"))));

        app.start(ImmutableList.<Location>of());

        assertEntityHealthy(test);
    }

    @Test
    public void shouldFailOnUnmatchedStdout() {
        String cmd = "stdoutAndStderr-" + Identifiers.randomLong();
        RecordingWinRmTool.setCustomResponse(cmd, new RecordingWinRmTool.CustomResponse(0, "wrongstdout", null));
        
        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
            .configure(TestWinrmCommand.ITERATION_LIMIT, 1)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, cmd)
            .configure(ASSERT_OUT, makeAssertions(ImmutableMap.of(CONTAINS, "mystdout"))));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable t) {
            Asserts.expectedFailureContains(t, "stdout expected contains mystdout but found wrongstdout");
        }

        assertEntityFailed(test);
    }

    @Test
    public void shouldFailOnUnmatchedStderr() {
        String cmd = "stdoutAndStderr-" + Identifiers.randomLong();
        RecordingWinRmTool.setCustomResponse(cmd, new RecordingWinRmTool.CustomResponse(0, null, "wrongstderr"));
        
        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
            .configure(TestWinrmCommand.ITERATION_LIMIT, 1)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, cmd)
            .configure(ASSERT_ERR, makeAssertions(ImmutableMap.of(CONTAINS, "mystderr"))));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable t) {
            Asserts.expectedFailureContains(t, "stderr expected contains mystderr but found wrongstderr");
        }

        assertEntityFailed(test);
    }

    @Test
    public void shouldNotBeUpIfAssertionsFail() {
        Map<String, ?> equalsOne = ImmutableMap.of(EQUALS, 1);

        Map<String, ?> equals255 = ImmutableMap.of(EQUALS, 255);

        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
            .configure(TestWinrmCommand.ITERATION_LIMIT, 1)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, "uptime")
            .configure(ASSERT_STATUS, makeAssertions(equalsOne, equals255)));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Dumper.dumpInfo(app);
            Asserts.expectedFailureContains(e, "exit code expected equals 1 but found 0", "exit code expected equals 255 but found 0");
        }

        assertEntityFailed(test);
    }

    @Test
    public void shouldFailIfTestEntityHasNoMachine() throws Exception {
        TestEntity testEntityWithNoMachine = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
            .configure(TARGET_ENTITY, testEntityWithNoMachine)
            .configure(COMMAND, "mycmd"));

        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No instances of class "+WinRmMachineLocation.class.getName()+" available");
        }

        assertEntityFailed(test);
    }
    
    @Test
    public void shouldFailFastIfNoCommand() throws Exception {
        Duration longTimeout = Asserts.DEFAULT_LONG_TIMEOUT;
        
        Map<String, ?> equalsZero = ImmutableMap.of(EQUALS, 0);
        
        TestWinrmCommand test = app.createAndManageChild(EntitySpec.create(TestWinrmCommand.class)
                .configure(TIMEOUT, longTimeout.multiply(2))
                .configure(TARGET_ENTITY, testEntity)
                .configure(ASSERT_STATUS, makeAssertions(equalsZero)));

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            app.start(ImmutableList.<Location>of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            // note: sleep(1000) can take a few millis less than 1000ms, according to a stopwatch.
            Asserts.expectedFailureContains(e, "Must specify exactly one of psScript and command");
            Duration elapsed = Duration.of(stopwatch);
            Asserts.assertTrue(elapsed.isShorterThan(longTimeout.subtract(Duration.millis(20))), "elapsed="+elapsed);
        }

        assertEntityFailed(test);
    }
    
    private List<Map<String, ?>> makeAssertions(Map<String, ?> map) {
        return ImmutableList.<Map<String, ?>>of(map);
    }

    private List<Map<String, ?>> makeAssertions(Map<String, ?> map1, Map<String, ?> map2) {
        return ImmutableList.of(map1, map2);
    }
}
