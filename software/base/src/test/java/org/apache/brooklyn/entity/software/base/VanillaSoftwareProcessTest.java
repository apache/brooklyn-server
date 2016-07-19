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
package org.apache.brooklyn.entity.software.base;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmdPredicates;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecParams;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class VanillaSoftwareProcessTest extends BrooklynAppUnitTestSupport {

    private Location loc;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = app.getManagementContext().getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .configure(FixedListMachineProvisioningLocation.MACHINE_SPECS, ImmutableList.<LocationSpec<? extends MachineLocation>>of(
                        LocationSpec.create(SshMachineLocation.class)
                                .configure("address", "1.2.3.4")
                                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()))));
        
        RecordingSshTool.clear();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshTool.clear();
    }
    
    @Test
    public void testAllCmds() throws Exception {
        app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "installCommand")
                .configure(VanillaSoftwareProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(VanillaSoftwareProcess.PRE_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(VanillaSoftwareProcess.CUSTOMIZE_COMMAND, "customizeCommand")
                .configure(VanillaSoftwareProcess.POST_CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(VanillaSoftwareProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "launchCommand")
                .configure(VanillaSoftwareProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "checkRunningCommand")
                .configure(VanillaSoftwareProcess.STOP_COMMAND, "stopCommand"));
        app.start(ImmutableList.of(loc));

        assertExecsContain(RecordingSshTool.getExecCmds(), ImmutableList.of(
                "preInstallCommand", "installCommand", "postInstallCommand", 
                "preCustomizeCommand", "customizeCommand", "postCustomizeCommand", 
                "preLaunchCommand", "launchCommand", "postLaunchCommand", 
                "checkRunningCommand"));
        
        app.stop();

        assertExecContains(RecordingSshTool.getLastExecCmd(), "stopCommand");
    }

    // See https://issues.apache.org/jira/browse/BROOKLYN-273
    @Test
    public void testRestartCmds() throws Exception {
        VanillaSoftwareProcess entity = app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "installCommand")
                .configure(VanillaSoftwareProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(VanillaSoftwareProcess.PRE_CUSTOMIZE_COMMAND, "customizeCommand")
                .configure(VanillaSoftwareProcess.CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(VanillaSoftwareProcess.POST_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(VanillaSoftwareProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "launchCommand")
                .configure(VanillaSoftwareProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "checkRunningCommand")
                .configure(VanillaSoftwareProcess.STOP_COMMAND, "stopCommand"));
        app.start(ImmutableList.of(loc));

        // Stop the entity, and clear out all record of previous execs
        Entities.invokeEffector(app, entity, VanillaSoftwareProcess.STOP, ImmutableMap.of(
                VanillaSoftwareProcess.StopSoftwareParameters.STOP_MACHINE_MODE.getName(), VanillaSoftwareProcess.StopSoftwareParameters.StopMode.NEVER,
                VanillaSoftwareProcess.StopSoftwareParameters.STOP_PROCESS_MODE.getName(), VanillaSoftwareProcess.StopSoftwareParameters.StopMode.ALWAYS))
                .get();

        RecordingSshTool.clearCmdHistory();

        // Invoke restart(), and check if all steps were executed
        Entities.invokeEffector(app, entity, VanillaSoftwareProcess.RESTART, ImmutableMap.of(
                VanillaSoftwareProcess.RestartSoftwareParameters.RESTART_CHILDREN.getName(), false,
                VanillaSoftwareProcess.RestartSoftwareParameters.RESTART_MACHINE.getName(), VanillaSoftwareProcess.RestartSoftwareParameters.RestartMachineMode.FALSE))
                .get();

        assertExecsContain(RecordingSshTool.getExecCmds(), ImmutableList.of(
                "checkRunningCommand", "stopCommand",  
                "preLaunchCommand", "launchCommand", "postLaunchCommand", 
                "checkRunningCommand"));
    }

    
    @Test
    public void testSkipInstallation() throws Exception {
        app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.SKIP_INSTALLATION, true)
                .configure(VanillaSoftwareProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "installCommand")
                .configure(VanillaSoftwareProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(VanillaSoftwareProcess.PRE_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(VanillaSoftwareProcess.CUSTOMIZE_COMMAND, "customizeCommand")
                .configure(VanillaSoftwareProcess.POST_CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(VanillaSoftwareProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "launchCommand")
                .configure(VanillaSoftwareProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "checkRunningCommand")
                .configure(VanillaSoftwareProcess.STOP_COMMAND, "stopCommand"));
        app.start(ImmutableList.of(loc));

        assertExecsContain(RecordingSshTool.getExecCmds(), ImmutableList.of(
                "preCustomizeCommand", "customizeCommand", "postCustomizeCommand", 
                "preLaunchCommand", "launchCommand", "postLaunchCommand", 
                "checkRunningCommand"));
        
        assertExecsNotContains(RecordingSshTool.getExecCmds(), ImmutableList.of(
                "preInstallCommand", "installCommand", "postInstallCommand"));
    }

    @Test
    public void testSkipEntityStartIfRunningWhenAlreadyRunning() throws Exception {
        app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.SKIP_ENTITY_START_IF_RUNNING, true)
                .configure(VanillaSoftwareProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "installCommand")
                .configure(VanillaSoftwareProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(VanillaSoftwareProcess.PRE_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(VanillaSoftwareProcess.CUSTOMIZE_COMMAND, "customizeCommand")
                .configure(VanillaSoftwareProcess.POST_CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(VanillaSoftwareProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "launchCommand")
                .configure(VanillaSoftwareProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "checkRunningCommand")
                .configure(VanillaSoftwareProcess.STOP_COMMAND, "stopCommand"));
        app.start(ImmutableList.of(loc));

        assertExecsContain(RecordingSshTool.getExecCmds(), ImmutableList.of(
                "checkRunningCommand"));
        
        assertExecsNotContains(RecordingSshTool.getExecCmds(), ImmutableList.of(
                "launchCommand"));
    }

    @Test
    public void testSkipEntityStartIfRunningWhenNotYetRunning() throws Exception {
        // The custom-responses are so that checkRunning returns success only after launch is done
        final AtomicBoolean isStarted = new AtomicBoolean();
        RecordingSshTool.setCustomResponse(".*checkRunningCommand.*", new RecordingSshTool.CustomResponseGenerator() {
            @Override public CustomResponse generate(ExecParams execParams) {
                int exitCode = isStarted.get() ? 0 : 1;
                return new CustomResponse(exitCode, "", "");
            }});
        RecordingSshTool.setCustomResponse(".*launchCommand.*", new RecordingSshTool.CustomResponseGenerator() {
            @Override public CustomResponse generate(ExecParams execParams) {
                isStarted.set(true);
                return new CustomResponse(0, "", "");
            }});
        RecordingSshTool.setCustomResponse(".*stopCommand.*", new RecordingSshTool.CustomResponseGenerator() {
            @Override public CustomResponse generate(ExecParams execParams) {
                isStarted.set(false);
                return new CustomResponse(0, "", "");
            }});

        app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.SKIP_ENTITY_START_IF_RUNNING, true)
                .configure(VanillaSoftwareProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "installCommand")
                .configure(VanillaSoftwareProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(VanillaSoftwareProcess.PRE_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(VanillaSoftwareProcess.CUSTOMIZE_COMMAND, "customizeCommand")
                .configure(VanillaSoftwareProcess.POST_CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(VanillaSoftwareProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "launchCommand")
                .configure(VanillaSoftwareProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "checkRunningCommand")
                .configure(VanillaSoftwareProcess.STOP_COMMAND, "stopCommand"));
        app.start(ImmutableList.of(loc));

        assertExecsContain(RecordingSshTool.getExecCmds(), ImmutableList.of(
                "checkRunningCommand",
                "preInstallCommand", "installCommand", "postInstallCommand", 
                "preCustomizeCommand", "customizeCommand", "postCustomizeCommand", 
                "preLaunchCommand", "launchCommand", "postLaunchCommand", 
                "checkRunningCommand"));
    }

    @Test
    public void testShellEnv() throws Exception {
        app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.SHELL_ENVIRONMENT.subKey("KEY1"), "VAL1")
                .configure(VanillaSoftwareProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "installCommand")
                .configure(VanillaSoftwareProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(VanillaSoftwareProcess.PRE_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(VanillaSoftwareProcess.CUSTOMIZE_COMMAND, "customizeCommand")
                .configure(VanillaSoftwareProcess.POST_CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(VanillaSoftwareProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "launchCommand")
                .configure(VanillaSoftwareProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "checkRunningCommand")
                .configure(VanillaSoftwareProcess.STOP_COMMAND, "stopCommand"));
        app.start(ImmutableList.of(loc));

        Map<String, String> expectedEnv = ImmutableMap.of("KEY1", "VAL1");
        
        assertExecsSatisfy(RecordingSshTool.getExecCmds(), ImmutableList.of(
                Predicates.and(ExecCmdPredicates.containsCmd("preInstallCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("installCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("postInstallCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("preCustomizeCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("customizeCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("postCustomizeCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("preLaunchCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("launchCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("postLaunchCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("checkRunningCommand"), ExecCmdPredicates.containsEnv(expectedEnv))));
        
        app.stop();

        assertExecSatisfies(
                RecordingSshTool.getLastExecCmd(),
                Predicates.and(ExecCmdPredicates.containsCmd("stopCommand"), ExecCmdPredicates.containsEnv(expectedEnv)));
    }
    
    protected void assertExecsContain(List<ExecCmd> actuals, List<String> expectedCmds) {
        String errMsg = "actuals="+actuals+"; expected="+expectedCmds;
        assertTrue(actuals.size() >= expectedCmds.size(), "actualSize="+actuals.size()+"; expectedSize="+expectedCmds.size()+"; "+errMsg);
        for (int i = 0; i < expectedCmds.size(); i++) {
            assertExecContains(actuals.get(i), expectedCmds.get(i), errMsg);
        }
    }

    protected void assertExecContains(ExecCmd actual, String expectedCmdRegex) {
        assertExecContains(actual, expectedCmdRegex, null);
    }
    
    protected void assertExecContains(ExecCmd actual, String expectedCmdRegex, String errMsg) {
        for (String cmd : actual.commands) {
            if (cmd.matches(expectedCmdRegex)) {
                return;
            }
        }
        fail(expectedCmdRegex + " not matched by any commands in " + actual+(errMsg != null ? "; "+errMsg : ""));
    }

    protected void assertExecsNotContains(List<? extends ExecCmd> actuals, List<String> expectedNotCmdRegexs) {
        for (ExecCmd actual : actuals) {
            assertExecContains(actual, expectedNotCmdRegexs);
        }
    }
    
    protected void assertExecContains(ExecCmd actual, List<String> expectedNotCmdRegexs) {
        for (String cmdRegex : expectedNotCmdRegexs) {
            for (String subActual : actual.commands) {
                if (subActual.matches(cmdRegex)) {
                    fail("Exec should not contain " + cmdRegex + ", but matched by " + actual);
                }
            }
        }
    }

    protected void assertExecsSatisfy(List<ExecCmd> actuals, List<? extends Predicate<? super ExecCmd>> expectedCmds) {
        String errMsg = "actuals="+actuals+"; expected="+expectedCmds;
        assertTrue(actuals.size() >= expectedCmds.size(), "actualSize="+actuals.size()+"; expectedSize="+expectedCmds.size()+"; "+errMsg);
        for (int i = 0; i < expectedCmds.size(); i++) {
            assertExecSatisfies(actuals.get(i), expectedCmds.get(i), errMsg);
        }
    }

    protected void assertExecSatisfies(ExecCmd actual, Predicate<? super ExecCmd> expected) {
        assertExecSatisfies(actual, expected, null);
    }
    
    protected void assertExecSatisfies(ExecCmd actual, Predicate<? super ExecCmd> expected, String errMsg) {
        if (!expected.apply(actual)) {
            fail(expected + " not matched by " + actual + (errMsg != null ? "; "+errMsg : ""));
        }
    }
}
