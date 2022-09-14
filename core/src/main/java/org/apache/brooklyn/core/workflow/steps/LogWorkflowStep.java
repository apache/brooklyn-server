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

import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(LogWorkflowStep.class);

    public static final ConfigKey<String> MESSAGE = ConfigKeys.newStringConfigKey("message");

    @Override
    public void setShorthand(String value) {
        setInput(MESSAGE, value);
    }

    @Override
    protected Task<?> newTask(String name, WorkflowExecutionContext context) {
        return Tasks.create(getDefaultTaskName(context), () -> {
            String message = getInput(context, MESSAGE);
            if (Strings.isBlank(message))  {
                throw new IllegalArgumentException("Log message is required");
            }
            LOG.info("{}: {}", name, message);
        });
    }
}
