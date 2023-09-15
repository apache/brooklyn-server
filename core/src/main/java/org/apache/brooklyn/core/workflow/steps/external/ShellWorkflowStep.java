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

import java.util.Map;
import java.util.function.Supplier;

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
import org.apache.brooklyn.util.core.task.system.internal.SystemProcessTaskFactory;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.text.Strings;

public class ShellWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "${command...}";

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

        return DynamicTasks.queue(SshWorkflowStep.customizeProcessTaskFactory(context, new SystemProcessTaskFactory(command)).newTask()).asTask().getUnchecked();
    }

    @Override protected Boolean isDefaultIdempotent() { return false; }
}
