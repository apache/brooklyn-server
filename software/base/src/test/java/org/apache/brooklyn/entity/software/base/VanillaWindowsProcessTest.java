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

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool.ExecParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class VanillaWindowsProcessTest extends BrooklynAppUnitTestSupport {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(VanillaWindowsProcessTest.class);

    private Location loc;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = app.getManagementContext().getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .configure(FixedListMachineProvisioningLocation.MACHINE_SPECS, ImmutableList.<LocationSpec<? extends MachineLocation>>of(
                        LocationSpec.create(WinRmMachineLocation.class)
                                .configure("address", "1.2.3.4")
                                .configure(WinRmMachineLocation.WINRM_TOOL_CLASS, RecordingWinRmTool.class.getName()))));
        
        RecordingWinRmTool.clear();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingWinRmTool.clear();
    }
    
    @Test
    public void testAllCmds() throws Exception {
        app.createAndManageChild(EntitySpec.create(VanillaWindowsProcess.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
                .configure(VanillaWindowsProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(VanillaWindowsProcess.INSTALL_COMMAND, "installCommand")
                .configure(VanillaWindowsProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(VanillaWindowsProcess.PRE_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(VanillaWindowsProcess.CUSTOMIZE_COMMAND, "customizeCommand")
                .configure(VanillaWindowsProcess.POST_CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(VanillaWindowsProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(VanillaWindowsProcess.LAUNCH_COMMAND, "launchCommand")
                .configure(VanillaWindowsProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(VanillaWindowsProcess.CHECK_RUNNING_COMMAND, "checkRunningCommand")
                .configure(VanillaWindowsProcess.STOP_COMMAND, "stopCommand"));
        app.start(ImmutableList.of(loc));

        assertExecsContain(RecordingWinRmTool.getExecs(), ImmutableList.of(
                "preInstallCommand", "installCommand", "postInstallCommand", 
                "preCustomizeCommand", "customizeCommand", "postCustomizeCommand", 
                "preLaunchCommand", "launchCommand", "postLaunchCommand", 
                "checkRunningCommand"));
        
        app.stop();

        assertExecContains(RecordingWinRmTool.getLastExec(), "stopCommand");
    }
    
    @Test
    public void testAllPowershell() throws Exception {
        app.createAndManageChild(EntitySpec.create(VanillaWindowsProcess.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
                .configure(VanillaWindowsProcess.PRE_INSTALL_POWERSHELL_COMMAND, "preInstallCommand")
                .configure(VanillaWindowsProcess.INSTALL_POWERSHELL_COMMAND, "installCommand")
                .configure(VanillaWindowsProcess.POST_INSTALL_POWERSHELL_COMMAND, "postInstallCommand")
                .configure(VanillaWindowsProcess.PRE_CUSTOMIZE_POWERSHELL_COMMAND, "preCustomizeCommand")
                .configure(VanillaWindowsProcess.CUSTOMIZE_POWERSHELL_COMMAND, "customizeCommand")
                .configure(VanillaWindowsProcess.POST_CUSTOMIZE_POWERSHELL_COMMAND, "postCustomizeCommand")
                .configure(VanillaWindowsProcess.PRE_LAUNCH_POWERSHELL_COMMAND, "preLaunchCommand")
                .configure(VanillaWindowsProcess.LAUNCH_POWERSHELL_COMMAND, "launchCommand")
                .configure(VanillaWindowsProcess.POST_LAUNCH_POWERSHELL_COMMAND, "postLaunchCommand")
                .configure(VanillaWindowsProcess.CHECK_RUNNING_POWERSHELL_COMMAND, "checkRunningCommand")
                .configure(VanillaWindowsProcess.STOP_POWERSHELL_COMMAND, "stopCommand"));
        app.start(ImmutableList.of(loc));

        assertExecsContain(RecordingWinRmTool.getExecs(), ImmutableList.of(
                "preInstallCommand", "installCommand", "postInstallCommand", 
                "preCustomizeCommand", "customizeCommand", "postCustomizeCommand", 
                "preLaunchCommand", "launchCommand", "postLaunchCommand", 
                "checkRunningCommand"));
        
        app.stop();

        assertExecContains(RecordingWinRmTool.getLastExec(), "stopCommand");
    }
    
    protected void assertExecsContain(List<? extends ExecParams> actuals, List<String> expectedCmds) {
        String errMsg = "actuals="+actuals+"; expected="+expectedCmds;
        assertTrue(actuals.size() >= expectedCmds.size(), "actualSize="+actuals.size()+"; expectedSize="+expectedCmds.size()+"; "+errMsg);
        for (int i = 0; i < expectedCmds.size(); i++) {
            assertExecContains(actuals.get(i), expectedCmds.get(i), errMsg);
        }
    }

    protected void assertExecContains(ExecParams actual, String expectedCmdRegex) {
        assertExecContains(actual, expectedCmdRegex, null);
    }
    
    protected void assertExecContains(ExecParams actual, String expectedCmdRegex, String errMsg) {
        for (String cmd : actual.commands) {
            if (cmd.matches(expectedCmdRegex)) {
                return;
            }
        }
        fail(expectedCmdRegex + " not matched by any commands in " + actual+(errMsg != null ? "; "+errMsg : ""));
    }

    protected void assertExecsNotContains(List<? extends ExecParams> actuals, List<String> expectedNotCmdRegexs) {
        for (ExecParams actual : actuals) {
            assertExecContains(actual, expectedNotCmdRegexs);
        }
    }
    
    protected void assertExecContains(ExecParams actual, List<String> expectedNotCmdRegexs) {
        for (String cmdRegex : expectedNotCmdRegexs) {
            for (String subActual : actual.commands) {
                if (subActual.matches(cmdRegex)) {
                    fail("Exec should not contain " + cmdRegex + ", but matched by " + actual);
                }
            }
        }
    }
}
