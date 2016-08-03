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
package org.apache.brooklyn.entity.group;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.sensor.ssh.SshCommandSensor;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;

/**
 * Policy which tracks membership of a group, and executes SSH commands
 * on MEMBER{ADDED,REMOVED} events, as well as SERVICE_UP {true,false} for those members.
 */
public class SshCommandMembershipTrackingPolicy extends AbstractMembershipTrackingPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(SshCommandMembershipTrackingPolicy.class);

    public static final ConfigKey<String> EXECUTION_DIR = ConfigKeys.newStringConfigKey("executionDir", "Directory where the command should run; "
        + "if not supplied, executes in the entity's run dir (or home dir if no run dir is defined); "
        + "use '~' to always execute in the home dir, or 'custom-feed/' to execute in a custom-feed dir relative to the run dir");

    public static final MapConfigKey<Object> SHELL_ENVIRONMENT = BrooklynConfigKeys.SHELL_ENVIRONMENT;

    public static final ConfigKey<String> UPDATE_COMMAND = ConfigKeys.newStringConfigKey("update.command", "Command to run on membership change events");

    /**
     * Called when a member is updated or group membership changes.
     */
    @Override
    protected void onEntityEvent(EventType type, Entity entity) {
        LOG.trace("Event {} received for {} in {}", new Object[] { type, entity, getGroup() });
        String command = config().get(UPDATE_COMMAND);
        if (Strings.isNonBlank(command)) {
            execute(command);
        }
    }

    public void execute(String command) {
        Maybe<SshMachineLocation> machine = Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class);
        if (machine.isAbsentOrNull()) {
            throw new IllegalStateException("No machine available to execute command");
        }
        LOG.info("Executing command on {}: {}", machine.get(), command);
        String executionDir = config().get(EXECUTION_DIR);
        String sshCommand = SshCommandSensor.makeCommandExecutingInDirectory(command, executionDir, entity);

        // Set things from the entities defined shell environment, overriding with our config
        Map<String, Object> env = MutableMap.of();
        env.putAll(MutableMap.copyOf(entity.config().get(BrooklynConfigKeys.SHELL_ENVIRONMENT)));
        env.putAll(MutableMap.copyOf(config().get(BrooklynConfigKeys.SHELL_ENVIRONMENT)));

        // Try to resolve the configuration in the env Map
        try {
            env = (Map<String, Object>) Tasks.resolveDeepValue(env, Object.class, ((EntityInternal) entity).getExecutionContext());
        } catch (InterruptedException | ExecutionException e) {
            Exceptions.propagateIfFatal(e);
        }

        // Execute the command with the serialized environment strings
        ShellEnvironmentSerializer serializer = new ShellEnvironmentSerializer(getManagementContext());
        SshEffectorTasks.SshEffectorTaskFactory<String> task = SshEffectorTasks.ssh(sshCommand)
                .machine(machine.get())
                .requiringZeroAndReturningStdout()
                .summary("group-membership-updated")
                .environmentVariables(serializer.serialize(env));

        String output = DynamicTasks.submit(task.newTask(), entity).getUnchecked();
        LOG.debug("Command returned: {}", output);
    }
}
