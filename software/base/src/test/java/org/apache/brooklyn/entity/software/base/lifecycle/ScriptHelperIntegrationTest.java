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
package org.apache.brooklyn.entity.software.base.lifecycle;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

import java.io.StringReader;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.entity.software.base.AbstractSoftwareProcessSshDriver;
import org.apache.brooklyn.entity.software.base.SoftwareProcessDriver;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcessImpl;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.system.internal.ExecWithLoggingHelpers;
import org.apache.brooklyn.util.stream.ReaderInputStream;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ScriptHelperIntegrationTest extends BrooklynAppLiveTestSupport
{
    private static final Logger log = LoggerFactory.getLogger(ScriptHelperIntegrationTest.class);
    
    private Location loc;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = app.getManagementContext().getLocationRegistry().getLocationManaged("localhost");
    }

//    Fails with:
//    Message: SIGTERM should be tried one time expected [1] but found [16]
//
//        Stacktrace:
//
//        at org.testng.Assert.fail(Assert.java:94)
//        at org.testng.Assert.failNotEquals(Assert.java:513)
//        at org.testng.Assert.assertEqualsImpl(Assert.java:135)
//        at org.testng.Assert.assertEquals(Assert.java:116)
//        at org.testng.Assert.assertEquals(Assert.java:389)
//        at org.apache.brooklyn.entity.software.base.lifecycle.ScriptHelperIntegrationTest.testStopCommandWaitsToStopWithSigTerm(ScriptHelperIntegrationTest.java:83)
//        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    @Test(groups={"Integration","Broken"})
    public void testStopCommandWaitsToStopWithSigTerm() {
        StopCommandSoftwareProcess entity = app.createAndManageChild(EntitySpec.create(StopCommandSoftwareProcess.class, StopCommandSoftwareProcessImpl.class));
        entity.start(ImmutableList.of(loc));
        VanillaSoftwareProcessSshDriver driver = (VanillaSoftwareProcessSshDriver) entity.getDriver();
        String launchContents = Joiner.on('\n')
                .join("#!/usr/bin/env bash",
                        "function trap_handler_command {",
                        "  echo stopping...",
                        "  exit 13",
                        "}",
                        "trap \"trap_handler_command\" SIGTERM",
                        "while true; do",
                        "  sleep 1",
                        "done");
        driver.copyResource(ImmutableMap.of(), new ReaderInputStream(new StringReader(launchContents), "UTF-8"), "launch.sh", true);
        driver.executeLaunchCommand("nohup bash launch.sh > /dev/null &");
        ScriptHelper stopCommandScriptHelper = driver.stopCommandScriptHelper();
        stopCommandScriptHelper.execute();
        assertEquals(StringUtils.countMatches(stopCommandScriptHelper.getResultStdout(), "Attempted to stop PID"), 1, "SIGTERM should be tried one time");
        assertFalse(stopCommandScriptHelper.getResultStdout().contains("stopped with SIGKILL"), "SIGKILL should not be sent after SIGTERM fails.");

        SshMachineLocation machineLocation = (SshMachineLocation) Iterables.getFirst(entity.getLocations(), null);
        int checkPidFileExitCode = machineLocation.execCommands("Check for pid file", ImmutableList.of("ls " + driver.getRunDir() + '/' + VanillaSoftwareProcessSshDriver.PID_FILENAME));
        assertEquals(checkPidFileExitCode, 2, "pid file should be deleted.");
    }

//    Fails with:
//    Exception java.lang.AssertionError
//
//    Message: null
//
//    Stacktrace:
//
//        at org.testng.Assert.fail(Assert.java:94)
//        at org.testng.Assert.assertNotEquals(Assert.java:832)
//        at org.testng.Assert.assertNotEquals(Assert.java:837)
//        at org.apache.brooklyn.entity.software.base.lifecycle.ScriptHelperIntegrationTest.testStopWithSigtermIsKilledWithSigKill(ScriptHelperIntegrationTest.java:126)
    @Test(groups={"Integration","Broken"})
    public void testStopWithSigtermIsKilledWithSigKill() {
        StopCommandSoftwareProcess entity = app.createAndManageChild(EntitySpec.create(StopCommandSoftwareProcess.class, StopCommandSoftwareProcessImpl.class));
        entity.start(ImmutableList.of(loc));
        VanillaSoftwareProcessSshDriver driver = (VanillaSoftwareProcessSshDriver) entity.getDriver();
        String launchContents = Joiner.on('\n')
                .join("#!/usr/bin/env bash",
                        "function trap_handler_command {",
                        "  while true; do",
                        "    echo \"Do nothing.\"",
                        "    sleep 1",
                        "  done",
                        "}",
                        "trap \"trap_handler_command\" SIGTERM",
                        "while true; do",
                        "  sleep 1",
                        "done");
        driver.copyResource(ImmutableMap.of(), new ReaderInputStream(new StringReader(launchContents), "UTF-8"), "launch.sh", true);
        driver.executeLaunchCommand("nohup bash launch.sh > /dev/null &");
        ByteArrayOutputStream stdOut = new ByteArrayOutputStream(15);
        SshMachineLocation machineLocation = (SshMachineLocation) Iterables.getFirst(entity.getLocations(), null);
        machineLocation.execCommands(
                ImmutableMap.<String, Object>of(ExecWithLoggingHelpers.STDOUT.getName(), stdOut),
                "check process is stopped",
                ImmutableList.of("cat "+driver.getRunDir() + '/' + VanillaSoftwareProcessSshDriver.PID_FILENAME),
                MutableMap.<String,Object>of());
        int launchedProcessPid = Integer.parseInt(Strings.trimEnd(new String(stdOut.toByteArray())));
        log.info(format("Pid of launched long running process %d.", launchedProcessPid));
        ScriptHelper stopCommandScriptHelper = driver.stopCommandScriptHelper();
        stopCommandScriptHelper.execute();
        assertEquals(StringUtils.countMatches(stopCommandScriptHelper.getResultStdout(), "Attempted to stop PID"), 16, "SIGTERM should be tried one time");
        assertEquals(StringUtils.countMatches(stopCommandScriptHelper.getResultStdout(), "Sent SIGKILL to " + launchedProcessPid), 1, "SIGKILL should be sent after SIGTERM fails.");

        int processCheckExitCode = machineLocation.execCommands("check whether process is still running", ImmutableList.of("ps -p " + launchedProcessPid + " > /dev/null"));
        assertNotEquals(processCheckExitCode, 0);
        int checkPidFileExitCode = machineLocation.execCommands("Check for pid file", ImmutableList.of("ls " + driver.getRunDir() + '/' + VanillaSoftwareProcessSshDriver.PID_FILENAME));
        assertEquals(checkPidFileExitCode, 2, "pid file should be deleted.");
    }

    public interface StopCommandSoftwareProcess extends VanillaSoftwareProcess {
        SoftwareProcessDriver getDriver();
    }
    public static class StopCommandSoftwareProcessImpl extends VanillaSoftwareProcessImpl implements StopCommandSoftwareProcess {
        @Override public Class<?> getDriverInterface() {
            return VanillaSoftwareProcessSshDriver.class;
        }
    }

    public static class VanillaSoftwareProcessSshDriver extends AbstractSoftwareProcessSshDriver {
        public VanillaSoftwareProcessSshDriver(
                @SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity,
                SshMachineLocation machine) {
            super(entity, machine);
        }

        public int executeLaunchCommand(String command) {
            return newScript(MutableMap.of(USE_PID_FILE, true), LAUNCHING)
                    .updateTaskAndFailOnNonZeroResultCode()
                    .body.append(command)
                    .gatherOutput()
                    .execute();
        }

        public ScriptHelper stopCommandScriptHelper() {
            return newScript(MutableMap.of(USE_PID_FILE, true), STOPPING).gatherOutput();
        }

        @Override
        public boolean isRunning() {
            return true;
        }

        @Override
        public void stop() {
            ScriptHelperIntegrationTest.log.debug(getClass().getName() + " stop");
        }

        @Override
        public void install() {
            ScriptHelperIntegrationTest.log.debug(getClass().getName() + " install");
        }

        @Override
        public void customize() {
            ScriptHelperIntegrationTest.log.debug(getClass().getName() + " customize");
        }

        @Override
        public void launch() {
            ScriptHelperIntegrationTest.log.info(getClass().getName() + " launch");
        }
    }
}
