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
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.util.core.task.Tasks;

public class NoOpWorkflowStep extends WorkflowStepDefinition {

    @Override
    protected void setShorthandValue(Object value) {
        // there is no point in having a no-op on its own; it is only useful if there is a next or condition or something else (i think)
        throw new IllegalStateException("Shorthand syntax not supported for no-op");
    }

    @Override
    protected Task<?> newTask(String name, WorkflowExecutionContext workflowExecutionContext) {
        return Tasks.create(getDefaultTaskName(workflowExecutionContext), () -> {});
    }

}
