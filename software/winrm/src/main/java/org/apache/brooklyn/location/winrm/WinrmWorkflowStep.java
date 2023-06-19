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
package org.apache.brooklyn.location.winrm;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.steps.external.SshWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.ssh.ConnectionDefinition;
import org.apache.brooklyn.util.core.task.ssh.internal.RemoteExecTaskConfigHelper;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.text.Strings;

import java.util.Map;

public class WinrmWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "[ \"to \" ${endpoint} ] ${command}";

    public static final ConfigKey<String> ENDPOINT = ConfigKeys.newStringConfigKey("endpoint");
    public static final ConfigKey<String> COMMAND = ConfigKeys.newStringConfigKey("command");
    public static final ConfigKey<Map<String,Object>> ENV = new MapConfigKey.Builder(Object.class, "env").build();
    public static final ConfigKey<DslPredicates.DslPredicate<Integer>> EXIT_CODE = ConfigKeys.newConfigKey(new TypeToken<DslPredicates.DslPredicate<Integer>>() {}, "exit_code");
    public static final ConfigKey<Integer> OUTPUT_MAX_SIZE = ConfigKeys.newIntegerConfigKey("output_max_size", "Maximum size for stdout and stderr, or -1 for no limit", 100000);

    ConfigKey<SetVariableWorkflowStep.InterpolationMode> INTERPOLATION_MODE = ConfigKeys.newConfigKeyWithDefault(SetVariableWorkflowStep.INTERPOLATION_MODE, SetVariableWorkflowStep.InterpolationMode.FULL);
    ConfigKey<TemplateProcessor.InterpolationErrorMode> INTERPOLATION_ERRORS = ConfigKeys.newConfigKeyWithDefault(SetVariableWorkflowStep.INTERPOLATION_ERRORS, TemplateProcessor.InterpolationErrorMode.IGNORE);

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        String command = new SetVariableWorkflowStep.ConfigurableInterpolationEvaluation<>(context, TypeToken.of(String.class), getInput().get(COMMAND.getName()),
                context.getInputOrDefault(INTERPOLATION_MODE), context.getInputOrDefault(INTERPOLATION_ERRORS)).evaluate();
        if (Strings.isBlank(command)) throw new IllegalStateException("'command' is required");

        WinRmMachineLocation machine;

        String endpoint = context.getInput(ENDPOINT);
        RemoteExecTaskConfigHelper.RemoteExecCapability remoteExecCapability;
        if (Strings.isNonBlank(endpoint)) {
            // TODO
            throw new IllegalStateException("Explicit endpoint not currently supported for winrm step");
        } else {
            ConnectionDefinition cdef = BrooklynTags.findSingleKeyMapValue(ConnectionDefinition.CONNECTION, ConnectionDefinition.class, context.getEntity().tags().getTags());
            if (cdef != null) {
                remoteExecCapability =   RemoteExecTaskConfigHelper
                        .RemoteExecCapabilityFromDefinition.of(context.getEntity(), cdef);
            } else {
                machine = Machines.findUniqueMachineLocation(context.getEntity().getLocations(), WinRmMachineLocation.class)
                        .orThrow("No WinRm location available for workflow at " + context.getEntity() + " and no endpoint specified");
                remoteExecCapability = new RemoteExecTaskConfigHelper.RemoteExecCapabilityFromLocation(machine);
            }
        }

        DslPredicates.DslPredicate<Integer> exitcode = context.getInput(EXIT_CODE);
        ProcessTaskFactory<?> tf = WinRmTasks.newWinrmExecTaskFactory(remoteExecCapability, command);
        if (exitcode!=null) tf.allowingNonZeroExitCode();
        Map<String, Object> env = context.getInput(ENV);
        if (env!=null) tf.environmentVariables(new ShellEnvironmentSerializer(context.getWorkflowExectionContext().getManagementContext()).serialize(env));
        tf.returning(ptw -> {
            checkExitCode(ptw, exitcode);
            return MutableMap.of("stdout", SshWorkflowStep.truncate(ptw.getStdout(), context.getInput(OUTPUT_MAX_SIZE)),
                    "stderr", SshWorkflowStep.truncate(ptw.getStderr(), context.getInput(OUTPUT_MAX_SIZE)),
                    "exit_code", ptw.getExitCode());
        });
        return DynamicTasks.queue(tf.newTask()).asTask().getUnchecked();
    }

    @Override
    protected Boolean isDefaultIdempotent() {
        return false;
    }

    protected void checkExitCode(ProcessTaskWrapper<?> ptw, DslPredicates.DslPredicate<Integer> exitcode) {
        if (exitcode==null) return;
        if (exitcode instanceof DslPredicates.DslPredicateBase) {
            Object implicit = ((DslPredicates.DslPredicateBase) exitcode).implicitEqualsUnwrapped();
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
            throw new IllegalStateException("Invalid exit code '"+ptw.getExitCode()+"'");
        }
    }

}
