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
package org.apache.brooklyn.tasks.kubectl;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.steps.external.SshWorkflowStep;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.text.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

public class ContainerWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "${image} [ ${command} ]";

    public static final ConfigKey<String> IMAGE = ConfigKeys.newStringConfigKey("image");
    public static final ConfigKey<String> COMMAND = ConfigKeys.newStringConfigKey("command");
    public static final ConfigKey<List<String>> COMMANDS = ConfigKeys.newConfigKey(new TypeToken<List<String>>() {}, "commands");
    public static final ConfigKey<List<String>> RAW_COMMAND = ConfigKeys.newConfigKey(new TypeToken<List<String>>() {}, "raw_command");
    public static final ConfigKey<PullPolicy> PULL_POLICY = ConfigKeys.newConfigKey(PullPolicy.class, "pull_policy", ContainerCommons.CONTAINER_IMAGE_PULL_POLICY.getDescription(), ContainerCommons.CONTAINER_IMAGE_PULL_POLICY.getDefaultValue());
    public static final ConfigKey<PullPolicy> PULL_POLICY_ALT = ConfigKeys.newConfigKey(PullPolicy.class, "pull-policy", ContainerCommons.CONTAINER_IMAGE_PULL_POLICY.getDescription(), ContainerCommons.CONTAINER_IMAGE_PULL_POLICY.getDefaultValue());
    public static final ConfigKey<Map<String,Object>> ENV = new MapConfigKey.Builder(Object.class, "env").build();
    public static final ConfigKey<DslPredicates.DslPredicate<Integer>> EXIT_CODE = ConfigKeys.newConfigKey(new TypeToken<DslPredicates.DslPredicate<Integer>>() {}, "exit_code");
    public static final ConfigKey<Integer> OUTPUT_MAX_SIZE = ConfigKeys.newIntegerConfigKey("output_max_size", "Maximum size for stdout and stderr, or -1 for no limit", 100000);

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        String image = context.getInput(IMAGE);
        if (Strings.isBlank(image)) throw new IllegalStateException("'image' is required");

        DslPredicates.DslPredicate<Integer> exitcode = context.getInput(EXIT_CODE);

        ContainerTaskFactory.ConcreteContainerTaskFactory<ContainerTaskResult> tf = ContainerTaskFactory.newInstance()
                .summary(image + " container task for workflow")
                .jobIdentifier(context.getWorkflowStepReference())
                .imagePullPolicy(firstNonNull(context.getInput(PULL_POLICY), context.getInput(PULL_POLICY_ALT)))
                .image(image);

        List<String> commandTypesSet = MutableList.of();

        String command = context.getInput(COMMAND);
        if (Strings.isNonBlank(command)) {
            commandTypesSet.add(COMMAND.getName());
            tf.bashScriptCommands(Arrays.asList(command));
        }
        List<String> commands = context.getInput(COMMANDS);
        if (commands!=null && !commands.isEmpty()) {
            commandTypesSet.add(COMMANDS.getName());
            tf.bashScriptCommands(commands);
        }
        List<String> rawCommand = context.getInput(RAW_COMMAND);
        if (rawCommand!=null && !rawCommand.isEmpty()) {
            commandTypesSet.add(RAW_COMMAND.getName());
            tf.command(rawCommand);
        }

        if (commandTypesSet.size()>1) {
            throw new IllegalStateException("Incompatible command specification, max 1, received: "+commandTypesSet);
        }

        Map<String, Object> env = context.getInput(ENV);
        if (env != null)
            tf.environmentVariables(new ShellEnvironmentSerializer(context.getWorkflowExectionContext().getManagementContext()).serialize(env));

        if (exitcode != null) tf.allowingNonZeroExitCode();
        tf.returning(ptw -> {
            checkExitCode(ptw, exitcode);
            return MutableMap.of("stdout", SshWorkflowStep.truncate(ptw.getMainStdout(), context.getInput(OUTPUT_MAX_SIZE)),
                    "exit_code", ptw.getMainExitCode());
        });
        return DynamicTasks.queue(tf.newTask()).asTask().getUnchecked();
    }

    protected void checkExitCode(ContainerTaskResult ptw, DslPredicates.DslPredicate<Integer> exitcode) {
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
        if (!exitcode.apply(ptw.getMainExitCode())) {
            throw new IllegalStateException("Invalid exit code '"+ptw.getMainExitCode()+"'");
        }
    }

    @Override protected Boolean isDefaultIdempotent() { return false; }
}
