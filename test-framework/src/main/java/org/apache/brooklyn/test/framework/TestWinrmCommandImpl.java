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
import static org.apache.brooklyn.util.text.Strings.isNonBlank;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.internal.winrm.WinRmToolResponse;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class TestWinrmCommandImpl extends TargetableTestComponentImpl implements TestWinrmCommand {

    // FIXME Do we want dest-path?
    // FIXME Do I need COMPUTER_NAME in flags?

    private static final Logger LOG = LoggerFactory.getLogger(TestWinrmCommandImpl.class);
    
    private static final int A_LINE = 80;

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
            final WinRmMachineLocation machineLocation =
                    Machines.findUniqueMachineLocation(resolveTarget().getLocations(), WinRmMachineLocation.class).get();
            final Duration timeout = getRequiredConfig(TIMEOUT);

            ReferenceWithError<Boolean> result = Repeater.create("Running winrm-command tests")
                    .limitTimeTo(timeout)
                    .every(timeout.multiply(0.1))
                    .until(new Callable<Boolean>() {
                        @Override
                        public Boolean call() throws Exception {
                            try {
                                WinRmToolResponse result = execute(machineLocation);
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

    private WinRmToolResponse execute(WinRmMachineLocation machineLocation) {
        WinRmToolResponse result = null;
        String psScript = getConfig(PS_SCRIPT);
        String command = getConfig(COMMAND);

        if (isNonBlank(psScript)) {
            result = executePsScript(machineLocation, psScript);
        } else if (isNonBlank(command)) {
            result = executeCommand(machineLocation, command);
        } else {
            // should have been caught by checkConfig() earlier; maybe someone reconfigured it on-the-fly?!
            throw illegal("Must specify exactly one of", PS_SCRIPT.getName(), "and", COMMAND.getName());
        }

        return result;
    }
    
    protected void checkConfig() {
        String psScript = getConfig(PS_SCRIPT);
        String command = getConfig(COMMAND);

        if (!(isNonBlank(psScript) ^ isNonBlank(command))) {
            String psScriptName = PS_SCRIPT.getName();
            String commandName = COMMAND.getName();
            throw illegal("Must specify exactly one of", psScriptName, "and", commandName);
        }
    }

    protected void handle(WinRmToolResponse result) {
        LOG.debug("{}, Result is {}\nwith output [\n{}\n] and error [\n{}\n]", new Object[] {
            this, result.getStatusCode(), shorten(result.getStdOut()), shorten(result.getStdErr())
        });
        TestFrameworkAssertions.AssertionSupport support = new TestFrameworkAssertions.AssertionSupport();
        for (Map<String, Object> assertion : exitCodeAssertions()) {
            checkActualAgainstAssertions(support, assertion, "exit code", result.getStatusCode());
        }
        for (Map<String, Object> assertion : getAssertions(this, ASSERT_OUT)) {
            checkActualAgainstAssertions(support, assertion, "stdout", result.getStdOut());
        }
        for (Map<String, Object> assertion : getAssertions(this, ASSERT_ERR)) {
            checkActualAgainstAssertions(support, assertion, "stderr", result.getStdErr());
        }
        support.validate();
    }

    private WinRmToolResponse executeCommand(final WinRmMachineLocation machine, final String command) {
        TaskBuilder<WinRmToolResponse> tb = Tasks.<WinRmToolResponse>builder().displayName("winrm-exec").body(
                new Callable<WinRmToolResponse>() {
                    public WinRmToolResponse call() throws Exception {
                        return machine.executeCommand(command);
                    }
                });
        return execute(tb, command);
    }
    
    private WinRmToolResponse executePsScript(final WinRmMachineLocation machine, final String psScript) {
        TaskBuilder<WinRmToolResponse> tb = Tasks.<WinRmToolResponse>builder().displayName("winrm-exec").body(
                new Callable<WinRmToolResponse>() {
                    public WinRmToolResponse call() throws Exception {
                        // FIXME Do I need COMPUTER_NAME in flags?
                        return machine.executePsScript(psScript);
                    }
                });
        return execute(tb, psScript);
    }
    
    private WinRmToolResponse execute(TaskBuilder<WinRmToolResponse> tb, String cmdIn) {
        try {
            ByteArrayOutputStream stdin = new ByteArrayOutputStream();
            if (cmdIn != null) {
                stdin.write(cmdIn.getBytes());
            }
            tb.tag(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDIN, stdin));
        } catch (IOException e) {
            LOG.warn("Error registering stream "+BrooklynTaskTags.STREAM_STDIN+" on "+tb+": "+e, e);
        }

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        tb.tag(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDOUT, stdout));
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        tb.tag(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDERR, stderr));
        
        Task<WinRmToolResponse> task = tb.build();
        
        WinRmToolResponse result;
        DynamicTasks.queueIfPossible(task).orSubmitAndBlock().getTask().blockUntilEnded();
        try {
            result = task.get();
            if (result.getStdOut() != null) stdout.write(result.getStdOut().getBytes());
            if (result.getStdErr() != null) stderr.write(result.getStdErr().getBytes());
        } catch (InterruptedException | ExecutionException e) {
            throw Exceptions.propagate(e);
        } catch (IOException e) {
            // Should not happen, as just using ByteArrayOutputStream
            throw Exceptions.propagate(e);
        }
        
        return result;
    }

    private IllegalArgumentException illegal(String message, String... messages) {
        Iterable<String> allmsgs = Iterables.concat(MutableList.of(this.toString() + ":", message), Arrays.asList(messages));
        return new IllegalArgumentException(Joiner.on(' ').join(allmsgs));
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
