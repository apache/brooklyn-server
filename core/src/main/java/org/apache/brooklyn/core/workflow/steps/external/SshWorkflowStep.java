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
package org.apache.brooklyn.core.workflow.steps.external;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.ssh.ConnectionDefinition;
import org.apache.brooklyn.util.core.task.ssh.SshTasks;
import org.apache.brooklyn.util.core.task.ssh.internal.RemoteExecTaskConfigHelper;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

import java.util.Map;
import java.util.function.Supplier;

public class SshWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "[ \"to \" ${endpoint} ] ${command...}";

    public static final ConfigKey<String> ENDPOINT = ConfigKeys.newStringConfigKey("endpoint");
    public static final ConfigKey<String> COMMAND = ConfigKeys.newStringConfigKey("command");
    //TODO public static final ConfigKey<String> COMMAND_URL = ConfigKeys.newStringConfigKey("command_url");
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

        String endpoint = context.getInput(ENDPOINT);
        if (Strings.isNonBlank(endpoint)) {
            // TODO
            throw new IllegalStateException("Explicit endpoint not currently supported for ssh step");
        } else {
            // TODO better errors if multiple
            ConnectionDefinition cdef = BrooklynTags.findSingleKeyMapValue(ConnectionDefinition.CONNECTION, ConnectionDefinition.class, context.getEntity().tags().getTags());
            if (cdef != null) {
                RemoteExecTaskConfigHelper.RemoteExecCapability remoteExecConfig =  new RemoteExecTaskConfigHelper.RemoteExecCapabilityFromDefinition(context.getManagementContext(), context.getEntity(), cdef);
                return DynamicTasks.queue(customizeProcessTaskFactory(context, SshTasks.newSshExecTaskFactory(remoteExecConfig, command)).newTask()).asTask().getUnchecked();
            } else {
                SshMachineLocation machine = Locations.findUniqueSshMachineLocation(context.getEntity().getLocations()).orThrow("No SSH location available for workflow at " + context.getEntity() + " and no endpoint specified");
                return DynamicTasks.queue(customizeProcessTaskFactory(context, SshTasks.newSshExecTaskFactory(machine, command)).newTask()).asTask().getUnchecked();
            }
        }
    }

    public static <U, T extends ProcessTaskFactory<U>> ProcessTaskFactory<Map<?,?>> customizeProcessTaskFactory(WorkflowStepInstanceExecutionContext context, T tf) {
        DslPredicates.DslPredicate<Integer> exitcode = context.getInput(EXIT_CODE);
        if (exitcode!=null) tf.allowingNonZeroExitCode();
        Map<String, Object> env = context.getInput(ENV);
        if (env!=null) tf.environmentVariables(new ShellEnvironmentSerializer(context.getWorkflowExectionContext().getManagementContext()).serialize(env));
        Integer maxLength = context.getInput(OUTPUT_MAX_SIZE);
        return tf.returning(ptw -> {
            context.setOutput(MutableMap.of("stdout", truncate(ptw.getStdout(), maxLength),
                    "stderr", truncate(ptw.getStderr(), maxLength),
                    "exit_code", ptw.getExitCode()));
            // make sure the output is set even if there is an error
            checkExitCode(ptw, exitcode);
            return (Map<?,?>) context.getOutput();
        });
    }

    protected static void checkExitCode(ProcessTaskWrapper<?> ptw, DslPredicates.DslPredicate<Integer> exitcode) {
        Supplier<String> extraInfo = () -> {
            String err = ptw.getStderr();
            if (Strings.isBlank(err)) return "";
            err = err.trim();
            if (err.length()>80) err = "..." + err.substring(err.length()-80, err.length());
            err = Strings.replaceAll(err, "\n", " / ");
            return ". Stderr is: "+err;
        };
        if (exitcode==null) {
            if (ptw.getExitCode()!=0) throw new IllegalStateException("Invalid exit code "+ptw.getExitCode()+extraInfo.get());
            return;
        }

        if (exitcode instanceof DslPredicates.DslPredicateBase) {
            Object implicit = ((DslPredicates.DslPredicateBase) exitcode).implicitEquals;
            if (implicit!=null) {
                if ("any".equalsIgnoreCase(""+implicit)) {
                    // if any is supplied as the implicit value, we accept; e.g. user says "exit_code: any"
                    return;
                }
            }
            // no other implicit values need be treated specially; 0 or 1 or 255 will work.
            // ranges still require `exit_code: { range: [0, 4] }`, same with `exit_code: { less-than: 5 }`.
        }
        if (!exitcode.apply(ptw.getExitCode())) {
            throw new IllegalStateException("Invalid exit code "+ptw.getExitCode()+"; does not match explicit exit_code requirement"+extraInfo.get());
        }
    }

    public static String truncate(String input, Integer maxLength) {
        if (input==null || maxLength==null || maxLength<0 || input.length() < maxLength) return input;
        if (maxLength<=4) return input.substring(input.length()-maxLength);
        return "... " + input.substring(input.length()-maxLength+4);
    }

    @Override protected Boolean isDefaultIdempotent() { return false; }
}
