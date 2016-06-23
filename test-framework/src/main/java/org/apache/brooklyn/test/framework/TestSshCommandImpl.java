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

import static org.apache.brooklyn.core.entity.lifecycle.Lifecycle.ON_FIRE;
import static org.apache.brooklyn.core.entity.lifecycle.Lifecycle.RUNNING;
import static org.apache.brooklyn.core.entity.lifecycle.Lifecycle.STARTING;
import static org.apache.brooklyn.core.entity.lifecycle.Lifecycle.STOPPED;
import static org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.setExpectedState;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.checkActualAgainstAssertions;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.getAssertions;
import static org.apache.brooklyn.util.text.Strings.isBlank;
import static org.apache.brooklyn.util.text.Strings.isNonBlank;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.ssh.SshTasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class TestSshCommandImpl extends TargetableTestComponentImpl implements TestSshCommand {

    private static final Logger LOG = LoggerFactory.getLogger(TestSshCommandImpl.class);
    private static final int A_LINE = 80;
    private static final String DEFAULT_NAME = "download.sh";
    private static final String CD = "cd";

    @Override
    public void start(Collection<? extends Location> locations) {
        setExpectedState(this, STARTING);
        execute();
    }

    @Override
    public void stop() {
        LOG.debug("{} Stopping simple command", this);
        setUpAndRunState(false, STOPPED);
    }

    @Override
    public void restart() {
        LOG.debug("{} Restarting simple command", this);
        execute();
    }

    private void setUpAndRunState(boolean up, Lifecycle status) {
        sensors().set(SERVICE_UP, up);
        setExpectedState(this, status);
    }

    private static class Result {
        int exitCode;
        String stdout;
        String stderr;
        public Result(final ProcessTaskWrapper<Integer> job) {
            exitCode = job.get();
            stdout = job.getStdout().trim();
            stderr = job.getStderr().trim();
        }
        public int getExitCode() {
            return exitCode;
        }
        public String getStdout() {
            return stdout;
        }
        public String getStderr() {
            return stderr;
        }
    }

    private String shorten(String text) {
        return Strings.maxlenWithEllipsis(text, A_LINE);
    }

    @SuppressWarnings("serial")
    private static class MarkerException extends Exception {
        public MarkerException(Throwable cause) {
            super(cause);
        }
    }

    public void execute() {
        try {
            checkConfig();
            final SshMachineLocation machineLocation =
                    Machines.findUniqueMachineLocation(resolveTarget().getLocations(), SshMachineLocation.class).get();
            final Duration timeout = getRequiredConfig(TIMEOUT);

            ReferenceWithError<Boolean> result = Repeater.create("Running ssh-command tests")
                    .limitTimeTo(timeout)
                    .every(timeout.multiply(0.1))
                    .until(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            try {
                                Result result = executeCommand(machineLocation);
                                handle(result);
                            } catch (AssertionError e) {
                                // Repeater will only handle Exceptions gracefully. Other Throwables are thrown
                                // immediately, so the AssertionError thrown by handle causes early exit.
                                throw new MarkerException(e);
                            }
                            return true;
                        }
                    })
                    .runKeepingError();

            if (!result.hasError()) {
                setUpAndRunState(true, RUNNING);
            } else {
                setUpAndRunState(false, ON_FIRE);
                Throwable error = result.getError();
                if (error instanceof MarkerException) {
                    error = error.getCause();
                }
                throw Exceptions.propagate(error);
            }

        } catch (Throwable t) {
            setUpAndRunState(false, ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }

    private Result executeCommand(SshMachineLocation machineLocation) {
        Result result = null;
        String downloadUrl = getConfig(DOWNLOAD_URL);
        String command = getConfig(COMMAND);

        ShellEnvironmentSerializer envSerializer = new ShellEnvironmentSerializer(getManagementContext());
        Map<String, String> env = envSerializer.serialize(getConfig(SHELL_ENVIRONMENT));
        if (env == null) env = ImmutableMap.of();
        
        if (isNonBlank(downloadUrl)) {
            String scriptDir = getConfig(SCRIPT_DIR);
            String scriptPath = calculateDestPath(downloadUrl, scriptDir);
            result = executeDownloadedScript(machineLocation, downloadUrl, scriptPath, env);
        } else if (isNonBlank(command)) {
            result = executeShellCommand(machineLocation, command, env);
        } else {
            // should have been caught by checkConfig() earlier; maybe someone reconfigured it on-the-fly?!
            throw illegal("Must specify exactly one of", DOWNLOAD_URL.getName(), "and", COMMAND.getName());
        }

        return result;
    }
    
    protected void checkConfig() {
        String downloadUrl = getConfig(DOWNLOAD_URL);
        String command = getConfig(COMMAND);

        if (!(isNonBlank(downloadUrl) ^ isNonBlank(command))) {
            String downloadName = DOWNLOAD_URL.getName();
            String commandName = COMMAND.getName();
            throw illegal("Must specify exactly one of", downloadName, "and", commandName);
        }
    }

    protected void handle(Result result) {
        LOG.debug("{}, Result is {}\nwith output [\n{}\n] and error [\n{}\n]", new Object[] {
            this, result.getExitCode(), shorten(result.getStdout()), shorten(result.getStderr())
        });
        TestFrameworkAssertions.AssertionSupport support = new TestFrameworkAssertions.AssertionSupport();
        for (Map<String, Object> assertion : exitCodeAssertions()) {
            checkActualAgainstAssertions(support, assertion, "exit code", result.getExitCode());
        }
        for (Map<String, Object> assertion : getAssertions(this, ASSERT_OUT)) {
            checkActualAgainstAssertions(support, assertion, "stdout", result.getStdout());
        }
        for (Map<String, Object> assertion : getAssertions(this, ASSERT_ERR)) {
            checkActualAgainstAssertions(support, assertion, "stderr", result.getStderr());
        }
        support.validate();
    }

    private Result executeDownloadedScript(SshMachineLocation machineLocation, String url, String scriptPath, Map<String, String> env) {

        TaskFactory<?> install = SshTasks.installFromUrl(ImmutableMap.<String, Object>of(), machineLocation, url, scriptPath);
        DynamicTasks.queue(install);
        DynamicTasks.waitForLast();

        List<String> commands = ImmutableList.<String>builder()
                .add("chmod u+x " + scriptPath)
                .addAll(maybeCdToRunDirCmd())
                .add(scriptPath)
                .build();

        return runCommands(machineLocation, commands, env);
    }

    private Result executeShellCommand(SshMachineLocation machineLocation, String command, Map<String, String> env) {

        List<String> commands = ImmutableList.<String>builder()
                .addAll(maybeCdToRunDirCmd())
                .add(command)
                .build();

        return runCommands(machineLocation, commands, env);
    }

    private List<String> maybeCdToRunDirCmd() {
        String runDir = getConfig(RUN_DIR);
        if (!isBlank(runDir)) {
            return ImmutableList.of(CD + " " + runDir);
        } else {
            return ImmutableList.of();
        }
    }

    private Result runCommands(SshMachineLocation machine, List<String> commands, Map<String, String> env) {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        SshEffectorTasks.SshEffectorTaskFactory<Integer> etf = SshEffectorTasks.ssh(commands.toArray(new String[]{}))
                .environmentVariables(env)
                .machine(machine);

        ProcessTaskWrapper<Integer> job = DynamicTasks.queue(etf);
        job.asTask().blockUntilEnded();
        return new Result(job);
    }



    private IllegalArgumentException illegal(String message, String... messages) {
        Iterable<String> allmsgs = Iterables.concat(MutableList.of(this.toString() + ":", message), Arrays.asList(messages));
        return new IllegalArgumentException(Joiner.on(' ').join(allmsgs));
    }

    private String calculateDestPath(String url, String directory) {
        try {
            URL asUrl = new URL(url);
            Iterable<String> path = Splitter.on("/").split(asUrl.getPath());
            String scriptName = getLastPartOfPath(path, DEFAULT_NAME);
            return Joiner.on("/").join(directory, "test-" + Identifiers.makeRandomId(8), scriptName);
        } catch (MalformedURLException e) {
            throw illegal("Malformed URL:", url);
        }
    }

    private static String getLastPartOfPath(Iterable<String> path, String defaultName) {
        MutableList<String> parts = MutableList.copyOf(path);
        Collections.reverse(parts);
        Iterator<String> it = parts.iterator();
        String scriptName = null;

        // strip any trailing "/" parts of URL
        while (isBlank(scriptName) && it.hasNext()) {
            scriptName = it.next();
        }
        if (isBlank(scriptName)) {
            scriptName = defaultName;
        }
        return scriptName;
    }
    

    private List<Map<String, Object>> exitCodeAssertions() {

        List<Map<String, Object>> assertStatus = getAssertions(this, ASSERT_STATUS);
        List<Map<String, Object>> assertOut = getAssertions(this, ASSERT_OUT);
        List<Map<String, Object>> assertErr = getAssertions(this, ASSERT_ERR);

        List<Map<String, Object>> result;
        if (assertStatus.isEmpty() && assertOut.isEmpty() && assertErr.isEmpty()) {
            Map<String, Object> shouldSucceed = DEFAULT_ASSERTION;
            result = MutableList.of(shouldSucceed);
        } else {
            result = assertStatus;
        }
        return result;
    }

}
