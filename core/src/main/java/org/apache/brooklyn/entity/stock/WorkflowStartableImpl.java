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
package org.apache.brooklyn.entity.stock;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.StartableMethods;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class WorkflowStartableImpl extends AbstractStartableImpl implements WorkflowStartable {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStartableImpl.class);

    protected Maybe<Object> runWorkflow(ConfigKey<CustomWorkflowStep> key) {
        CustomWorkflowStep workflow = getConfig(key);
        if (workflow==null) return Maybe.absent("No workflow defined for: "+key.getName());

        WorkflowExecutionContext workflowContext = workflow.newWorkflowExecution(this, key.getName().toLowerCase(),
                null /* could getInput from workflow, and merge shell environment here */);

        return Maybe.of(DynamicTasks.queueIfPossible( workflowContext.getTask(true).get() ).orSubmitAsync(this).getTask().getUnchecked());
    }

    @Override
    protected void doStart(Collection<? extends Location> locations) {
        runWorkflow(START_WORKFLOW).get();
    }

    @Override
    protected void doStop() {
        runWorkflow(STOP_WORKFLOW).get();
    }

    @Override
    public void restart() {
        runWorkflow(RESTART_WORKFLOW).get();
    }

}
