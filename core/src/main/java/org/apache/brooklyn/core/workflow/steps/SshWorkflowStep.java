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
package org.apache.brooklyn.core.workflow.steps;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.ssh.SshTasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

import java.util.Map;

public class SshWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "[ \"to \" ${endpoint} ] ${command...}";

    public static final ConfigKey<String> ENDPOINT = ConfigKeys.newStringConfigKey("endpoint");
    public static final ConfigKey<String> COMMAND = ConfigKeys.newStringConfigKey("command");
    public static final ConfigKey<Map<String,Object>> ENV = new MapConfigKey.Builder(Object.class, "env").build();
    public static final ConfigKey<DslPredicates.DslPredicate<Integer>> EXIT_CODE = ConfigKeys.newConfigKey(new TypeToken<DslPredicates.DslPredicate<Integer>>() {}, "exit-code");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        String command = context.getInput(COMMAND);
        if (Strings.isBlank(command)) throw new IllegalStateException("'command' is required");

        SshMachineLocation machine;

        String endpoint = context.getInput(ENDPOINT);
        if (Strings.isNonBlank(endpoint)) {
            // TODO
            throw new IllegalStateException("Explicit endpoint not currently supported for ssh step");
        } else {
            machine = Locations.findUniqueSshMachineLocation(context.getEntity().getLocations()).orThrow("No SSH location available for workflow at "+context.getEntity()+" and no endpoint specified");
        }

        return DynamicTasks.queue(customizeProcessTaskFactory(context, SshTasks.newSshExecTaskFactory(machine, command)).newTask()).asTask().getUnchecked();
    }

    public static <U, T extends ProcessTaskFactory<U>> ProcessTaskFactory<Map<?,?>> customizeProcessTaskFactory(WorkflowStepInstanceExecutionContext context, T tf) {
        DslPredicates.DslPredicate<Integer> exitcode = context.getInput(EXIT_CODE);
        if (exitcode!=null) tf.allowingNonZeroExitCode();
        Map<String, Object> env = context.getInput(ENV);
        if (env!=null) tf.environmentVariables(new ShellEnvironmentSerializer(context.getWorkflowExectionContext().getManagementContext()).serialize(env));
        return tf.returning(ptw -> {
            context.setOutput(MutableMap.of("stdout", ptw.getStdout(),
                    "stderr", ptw.getStderr(),
                    "exit_code", ptw.getExitCode()));
            // make sure the output is set even if there is an error
            checkExitCode(ptw, exitcode);
            return (Map<?,?>) context.getOutput();
        });
    }

    protected static void checkExitCode(ProcessTaskWrapper<?> ptw, DslPredicates.DslPredicate<Integer> exitcode) {
        if (exitcode==null) {
            if (ptw.getExitCode()!=0) throw new IllegalStateException("Invalid exit code "+ptw.getExitCode());
            return;
        }

        if (exitcode instanceof DslPredicates.DslPredicateBase) {
            Object implicit = ((DslPredicates.DslPredicateBase) exitcode).implicitEquals;
            if (implicit!=null) {
                if ("any".equalsIgnoreCase(""+implicit)) {
                    // if any is supplied as the implicit value, we accept; e.g. user says "exit-code: any"
                    return;
                }
            }
            // no other implicit values need be treated specially; 0 or 1 or 255 will work.
            // ranges still require `exit-code: { range: [0, 4] }`, same with `exit-code: { less-than: 5 }`.
        }
        if (!exitcode.apply(ptw.getExitCode())) {
            throw new IllegalStateException("Invalid exit code "+ptw.getExitCode()+"; does not match explicit exit-code requirement");
        }
    }

}
