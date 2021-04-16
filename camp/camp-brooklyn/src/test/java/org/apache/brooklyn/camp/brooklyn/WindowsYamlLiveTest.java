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

import org.apache.brooklyn.core.mgmt.BrooklynTaskTags.WrappedStream;
import org.apache.brooklyn.location.winrm.PlainWinRmExecTaskFactory;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.VanillaWindowsProcess;
import org.apache.brooklyn.entity.software.base.test.location.WindowsTestFixture;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.task.TaskPredicates;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Tests Windows YAML blueprint features.
 */
@Test
public class WindowsYamlLiveTest extends AbstractWindowsYamlTest {
    
    // set EXISTING_WINDOWS_TEST_USER_PASS_HOST_ENV_VAR as per WindowsTestFixture to re-use existing machines
    
    private static final Logger log = LoggerFactory.getLogger(WindowsYamlLiveTest.class);

    /**
     * Maps from the task names that are used to the names used in log/exception messages.
     */
    private static final Map<String, String> TASK_REGEX_TO_COMMAND = ImmutableMap.<String, String>builder()
            .put("winrm: pre-install-command.*", "pre-install-command")
            .put("winrm: install.*", "install-command")
            .put("winrm: post-install-command.*", "post-install-command")
            .put("winrm: customize.*", "customize-command")
            .put("winrm: pre-launch-command.*", "pre-launch-command")
            .put("winrm: launch.*", "launch-command")
            .put("winrm: post-launch-command.*", "post-launch-command")
            .put("winrm: stop-command.*", "stop-command")
            .put("winrm: is-running-command.*", "is-running-command")
            .build();

    protected List<String> yamlLocation;
    protected MachineProvisioningLocation<WinRmMachineLocation> location;
    protected WinRmMachineLocation machine;
    protected Entity app;
    
    protected boolean useDefaultProperties() {
        return true;
    }
    
    @BeforeClass(alwaysRun = true)
    public void setUpClass() throws Exception {
        super.setUp();
        
        location = WindowsTestFixture.setUpWindowsLocation(mgmt());
        machine = location.obtain(ImmutableMap.of());

        yamlLocation = ImmutableList.of(
                "location:",
                "  byon:",
                "    hosts:",
                "    - winrm: "+machine.getAddress().getHostAddress()
                        // this is the default, probably not necessary but kept for posterity
                        +":5985",
                "      password: "+JavaStringEscapes.wrapJavaString(machine.config().get(WinRmMachineLocation.PASSWORD)),
                "      user: "+machine.config().get(WinRmMachineLocation.USER),
                "      osFamily: windows");
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() throws Exception {
        try {
            if (location != null) location.release(machine);
        } catch (Throwable t) {
            log.error("Caught exception in tearDownClass method", t);
        } finally {
            super.tearDown();
        }
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() {
        // no-op; everything done @BeforeClass
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() {
        try {
            if (app != null) Entities.destroy(app);
        } catch (Throwable t) {
            log.error("Caught exception in tearDown method", t);
        } finally {
            app = null;
        }
    }

    @Override
    protected ManagementContextInternal mgmt() {
        return (ManagementContextInternal) super.mgmt();
    }

    @Test(groups="Live")
    public void testPowershellMinimalist() throws Exception {
        Map<String, String> cmds = ImmutableMap.<String, String>builder()
                .put("myarg", "myval")
                .put("launch.powershell.command", JavaStringEscapes.wrapJavaString("& \"$Env:INSTALL_DIR\\exit0.ps1\""))
                .put("checkRunning.powershell.command", JavaStringEscapes.wrapJavaString("& \"$Env:INSTALL_DIR\\exit0.bat\""))
                .build();

        Map<String, List<String>> stdouts = ImmutableMap.of();

        runWindowsApp(cmds, stdouts, true, null);
    }

    @Test(groups="Live")
    public void testPowershell() throws Exception {
        Map<String, String> cmds = ImmutableMap.<String, String>builder()
                .put("myarg", "myval")
                .put("pre.install.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("install.powershell.command", "\"& c:\\\\echoMyArg.ps1 -myarg myInstall\"")
                .put("post.install.powershell.command", "\"& c:\\\\echoArg.bat myPostInstall\"")
                .put("customize.powershell.command", "\"& c:\\\\echoFreemarkerMyarg.bat\"")
                .put("pre.launch.powershell.command", "\"& c:\\\\echoFreemarkerMyarg.ps1\"")
                .put("launch.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("post.launch.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("checkRunning.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("stop.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .build();

        Map<String, List<String>> stdouts = ImmutableMap.<String, List<String>>builder()
                .put("winrm: install.*", ImmutableList.of("myInstall"))
                .put("winrm: post-install-command.*", ImmutableList.of("myPostInstall"))
                .put("winrm: customize.*", ImmutableList.of("myval"))
                .put("winrm: pre-launch-command.*", ImmutableList.of("myval"))
                .build();

        runWindowsApp(cmds, stdouts, false, null);
    }

    @Test(groups="Live")
    public void testBatch() throws Exception {
        Map<String, String> cmds = ImmutableMap.<String, String>builder()
                .put("myarg", "myval")
                .put("pre.install.command", "\"PowerShell -NonInteractive -NoProfile -Command c:\\\\exit0.ps1\"")
                .put("install.command", "\"PowerShell -NonInteractive -NoProfile -Command c:\\\\echoMyArg.ps1 -myarg myInstall\"")
                .put("post.install.command", "\"c:\\\\echoArg.bat myPostInstall\"")
                .put("customize.command", "\"c:\\\\echoFreemarkerMyarg.bat\"")
                .put("pre.launch.command", "\"PowerShell -NonInteractive -NoProfile -Command c:\\\\echoFreemarkerMyarg.ps1\"")
                .put("launch.command", "\"PowerShell -NonInteractive -NoProfile -Command c:\\\\exit0.ps1\"")
                .put("post.launch.command", "\"PowerShell -NonInteractive -NoProfile -Command c:\\\\exit0.ps1\"")
                .put("checkRunning.command", "\"PowerShell -NonInteractive -NoProfile -Command c:\\\\exit0.ps1\"")
                .put("stop.command", "\"PowerShell -NonInteractive -NoProfile -Command c:\\\\exit0.ps1\"")
                .build();

        Map<String, List<String>> stdouts = ImmutableMap.<String, List<String>>builder()
                .put("winrm: install.*", ImmutableList.of("myInstall"))
                .put("winrm: post-install-command.*", ImmutableList.of("myPostInstall"))
                .put("winrm: customize.*", ImmutableList.of("myval"))
                .put("winrm: pre-launch-command.*", ImmutableList.of("myval"))
                .build();

        runWindowsApp(cmds, stdouts, false, null);
    }

    @Test(groups="Live")
    public void testPowershellExit1() throws Exception {
        Map<String, String> cmds = ImmutableMap.<String, String>builder()
                .put("myarg", "myval")
                .put("pre.install.powershell.command", "\"& c:\\\\exit1.ps1\"")
                .put("install.powershell.command", "\"& c:\\\\echoMyArg.ps1 -myarg myInstall\"")
                .put("post.install.powershell.command", "\"& c:\\\\echoArg.bat myPostInstall\"")
                .put("customize.powershell.command", "\"& c:\\\\echoFreemarkerMyarg.bat\"")
                .put("pre.launch.powershell.command", "\"& c:\\\\echoFreemarkerMyarg.ps1\"")
                .put("launch.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("post.launch.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("checkRunning.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("stop.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .build();

        Map<String, List<String>> stdouts = ImmutableMap.of();

        runWindowsApp(cmds, stdouts, false, "winrm: pre-install-command.*");
    }

    // FIXME Failing to match the expected exception, but looks fine! Needs more investigation.
    @Test(groups="Live")
    public void testPowershellCheckRunningExit1() throws Exception {
        Map<String, String> cmds = ImmutableMap.<String, String>builder()
                .put("myarg", "myval")
                .put("pre.install.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("install.powershell.command", "\"& c:\\\\echoMyArg.ps1 -myarg myInstall\"")
                .put("post.install.powershell.command", "\"& c:\\\\echoArg.bat myPostInstall\"")
                .put("customize.powershell.command", "\"& c:\\\\echoFreemarkerMyarg.bat\"")
                .put("pre.launch.powershell.command", "\"& c:\\\\echoFreemarkerMyarg.ps1\"")
                .put("launch.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("post.launch.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("checkRunning.powershell.command", "\"& c:\\\\exit1.ps1\"")
                .put("stop.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .build();

        Map<String, List<String>> stdouts = ImmutableMap.of();

        runWindowsApp(cmds, stdouts, false, "winrm: is-running-command.*");
    }

    // FIXME Needs more work to get the stop's task that failed, so can assert got the right error message
    @Test(groups="Live")
    public void testPowershellStopExit1() throws Exception {
        Map<String, String> cmds = ImmutableMap.<String, String>builder()
                .put("myarg", "myval")
                .put("pre.install.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("install.powershell.command", "\"& c:\\\\echoMyArg.ps1 -myarg myInstall\"")
                .put("post.install.powershell.command", "\"& c:\\\\echoArg.bat myPostInstall\"")
                .put("customize.powershell.command", "\"& c:\\\\echoFreemarkerMyarg.bat\"")
                .put("pre.launch.powershell.command", "\"& c:\\\\echoFreemarkerMyarg.ps1\"")
                .put("launch.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("post.launch.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("checkRunning.powershell.command", "\"& c:\\\\exit0.ps1\"")
                .put("stop.powershell.command", "\"& c:\\\\exit1.ps1\"")
                .build();

        Map<String, List<String>> stdouts = ImmutableMap.of();

        runWindowsApp(cmds, stdouts, false, "winrm: stop-command.*");
    }

    protected void runWindowsApp(Map<String, String> commands, Map<String, List<String>> stdouts, boolean useInstallDir, String taskRegexFailed) throws Exception {
        String cmdFailed = (taskRegexFailed == null) ? null : TASK_REGEX_TO_COMMAND.get(taskRegexFailed);

        List<String> yaml = Lists.newArrayList();
        yaml.addAll(yamlLocation);
        String prefix = useInstallDir ? "" : "c:\\";
        yaml.addAll(ImmutableList.of(
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.VanillaWindowsProcess",
                "  brooklyn.config:",
                "    templates.preinstall:",
                "      classpath://org/apache/brooklyn/camp/brooklyn/echoFreemarkerMyarg.bat: c:\\echoFreemarkerMyarg.bat",
                "      classpath://org/apache/brooklyn/camp/brooklyn/echoFreemarkerMyarg.ps1: c:\\echoFreemarkerMyarg.ps1",
                "    files.preinstall:",
                "      classpath://org/apache/brooklyn/camp/brooklyn/echoArg.bat: "+prefix+"echoArg.bat",
                "      classpath://org/apache/brooklyn/camp/brooklyn/echoMyArg.ps1: "+prefix+"echoMyArg.ps1",
                "      classpath://org/apache/brooklyn/camp/brooklyn/exit0.bat: "+prefix+"exit0.bat",
                "      classpath://org/apache/brooklyn/camp/brooklyn/exit1.bat: "+prefix+"exit1.bat",
                "      classpath://org/apache/brooklyn/camp/brooklyn/exit0.ps1: "+prefix+"exit0.ps1",
                "      classpath://org/apache/brooklyn/camp/brooklyn/exit1.ps1: "+prefix+"exit1.ps1",
                ""));

        for (Map.Entry<String, String> entry : commands.entrySet()) {
            yaml.add("    "+entry.getKey()+": "+entry.getValue());
        }

        if (Strings.isBlank(cmdFailed)) {
            app = createAndStartApplication(Joiner.on("\n").join(yaml));
            waitForApplicationTasks(app);
            log.info("App started:");
            Dumper.dumpInfo(app);

            VanillaWindowsProcess entity = (VanillaWindowsProcess) app.getChildren().iterator().next();

            EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
            assertStreams(entity, stdouts);

        } else if (cmdFailed.equals("stop-command")) {
            app = createAndStartApplication(Joiner.on("\n").join(yaml));
            waitForApplicationTasks(app);
            log.info("App started:");
            Dumper.dumpInfo(app);
            VanillaWindowsProcess entity = (VanillaWindowsProcess) app.getChildren().iterator().next();
            EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);

            entity.stop();
            assertSubTaskFailures(entity, ImmutableMap.of(taskRegexFailed, StringPredicates.containsLiteral("for "+cmdFailed)));

        } else {
            try {
                app = createAndStartApplication(Joiner.on("\n").join(yaml));
                fail("start should have failed for app="+app);
            } catch (Exception e) {
                if (!e.toString().contains("invalid result") || !e.toString().contains("for "+cmdFailed)) throw e;
            }
        }
    }

    @Test(groups="Live")
    public void testEnvVarResolution() throws Exception {
        List<String> yaml = Lists.newArrayList();
        yaml.addAll(yamlLocation);
        String in = "%KEY1%: %ADDR_RESOLVED%";
        yaml.addAll(ImmutableList.of(
            "services:",
            "  - type: org.apache.brooklyn.entity.software.base.VanillaWindowsProcess",
            "    brooklyn.config:",
            "      install.command: "+JavaStringEscapes.wrapJavaString("echo "+in),
            "      customize.command: "+JavaStringEscapes.wrapJavaString("echo "+in),
            "      launch.command: "+JavaStringEscapes.wrapJavaString("echo "+in),
            "      stop.command: echo true",
            "      checkRunning.command: echo true",
            "      shell.env:",
            "        KEY1: Address",
            "        ADDR_RESOLVED: $brooklyn:attributeWhenReady(\"host.address\")"));

        app = createAndStartApplication(Joiner.on("\n").join(yaml));
        waitForApplicationTasks(app);
        log.info("App started:");
        Dumper.dumpInfo(app);


        Entity win = Iterables.getOnlyElement(app.getChildren());
        String out = "Address: "+win.sensors().get(SoftwareProcess.ADDRESS);
        assertPhaseStreamSatisfies(win, "install", "stdout", Predicates.equalTo(in));
        assertPhaseStreamSatisfies(win, "customize", "stdout", Predicates.equalTo(out));
        assertPhaseStreamSatisfies(win, "launch", "stdout", Predicates.equalTo(out));
    }

    @Test(groups="Live")
    public void testDifferentLogLevels() throws Exception {
        List<String> yaml = Lists.newArrayList();
        yaml.addAll(yamlLocation);

        String hostMsg = "Host Message";
        String progressMsg = "Progress Message";
        String outputMsg = "Output Message";
        String errMsg = "Error Message";
        String warningMsg = "Warning Message";
        String verboseMsg = "Verbose Message";
        String debugMsg = "Debug Message";
        String informationMsg = "Information Message";

        String cmd =
                "$DebugPreference = \"Continue\"\n" +
                "$VerbosePreference = \"Continue\"\n" +
                "\n" +
                "Write-Host \"" + hostMsg + "\"\n" +
                "Write-Output \"" + outputMsg + "\"\n" +
                "Write-Progress \"" + progressMsg + "\"\n" +
                "Write-Error \"" + errMsg + "\" \n" +
                "Write-Warning \"" + warningMsg + "\"\n" +
                "Write-Verbose \"" + verboseMsg + "\"\n" +
                "Write-Debug \"" + debugMsg + "\" \n" +
                // "Write-Information \"" + informationMsg + "\" \n" + // Write-Host is a wrapper for Write-Information since PowerShell 5.0
                "";

        yaml.addAll(ImmutableList.of(
                "services:",
                "  - type: org.apache.brooklyn.entity.software.base.VanillaWindowsProcess",
                "    brooklyn.config:",
                "      checkRunning.command: echo true",
                "      install.powershell.command: |",
                Strings.indent(8, cmd) ));

        app = createAndStartApplication(Joiner.on("\n").join(yaml));
        waitForApplicationTasks(app);
        log.info("App started:");
        Dumper.dumpInfo(app);


        Entity win = Iterables.getOnlyElement(app.getChildren());

        // Verify stdout output.
        assertPhaseStreamSatisfies(win, "install", "stdout", s ->
                s.trim().equalsIgnoreCase(hostMsg+"\n"+outputMsg));

        // Verify WinRM output to contain error, warning, verbose and debug messages in CLI XML format.
        assertPhaseStreamSatisfies(win, "install", PlainWinRmExecTaskFactory.WINRM_STREAM, s ->
                Strings.countOccurrences(s, errMsg + "_x000D__x000A_</S>") == 1 && // error always includes \r\n markers
                Strings.countOccurrences(s, warningMsg + "</S>") == 1 &&
                Strings.countOccurrences(s, verboseMsg + "</S>") == 1 &&
                Strings.countOccurrences(s, debugMsg + "</S>") == 1 &&
                Strings.countOccurrences(s, "<Obj S=\"progress\"") == 1 && // Progress is a composite object;
                Strings.countOccurrences(s, progressMsg + "</AV>") == 1 && // message is wrapped with 'AV' tag.
                // host and output are not included
                Strings.countOccurrences(s, hostMsg + "</S>") == 0 &&
                Strings.countOccurrences(s, outputMsg + "</S>") == 0 &&
                Strings.countOccurrences(s, informationMsg + "</S>") == 0);
    }
    
    private void assertPhaseStreamSatisfies(Entity entity, String phase, String stream, Predicate<String> check) {
        WrappedStream streamV = getWrappedStream(entity, phase, stream);
        Asserts.assertNotNull(streamV, "phase "+phase+" stream "+stream+" not found");
        Asserts.assertThat(streamV.streamContents.get().trim(), check, "phase "+phase+" stream "+stream+" not as expected: '"+streamV.streamContents.get().trim()+"'");
    }

    private WrappedStream getWrappedStream(Entity entity, String phase, String stream) {
        Optional<Task<?>> t = findTaskOrSubTask(entity, TaskPredicates.displayNameSatisfies(StringPredicates.startsWith("winrm: "+ phase)));
        Asserts.assertTrue(t.isPresent(), "phase "+ phase +" not found in tasks");
        WrappedStream streamV = BrooklynTaskTags.stream(t.get(), stream);
        return streamV;
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
