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
package org.apache.brooklyn.core.workflow;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.NaturalOrderComparator;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class WorkflowExecutionContext implements TaskAdaptable<Object> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutionContext.class);

    String workflowInstanceId;

    String name;
    BrooklynObject entityOrAdjunctWhereRunning;
    Entity entity;

    // TODO for deserialization, and for sensibility, we might need to change these to be resolved maps
    ConfigBag paramsDefiningWorkflow;
    ConfigBag paramsPassedToWorkflow;

    String taskId;
    transient Task<Object> task;

    String currentStepId;

    // deserialization constructor
    private WorkflowExecutionContext() {}

    public WorkflowExecutionContext(String name, BrooklynObject entityOrAdjunctWhereRunning, org.apache.brooklyn.util.core.config.ConfigBag paramsDefiningWorkflow, ConfigBag paramsPassedToWorkflow) {
        workflowInstanceId = Identifiers.makeRandomId(12);

        this.name = name;
        this.entityOrAdjunctWhereRunning = entityOrAdjunctWhereRunning;
        this.entity = entityOrAdjunctWhereRunning instanceof Entity ? (Entity)entityOrAdjunctWhereRunning : ((EntityAdjuncts.EntityAdjunctProxyable)entityOrAdjunctWhereRunning).getEntity();
        this.paramsDefiningWorkflow = paramsDefiningWorkflow;
        this.paramsPassedToWorkflow = paramsPassedToWorkflow;

        task = Tasks.builder().dynamic(true).displayName(name).body(new Body()).build();
        taskId = task.getId();
    }

    @Override
    public String toString() {
        return "Workflow<" + name + " - " + workflowInstanceId + ">";
    }

    public String getCurrentStepId() {
        return currentStepId;
    }

    @Override
    public Task<Object> asTask() {
        if (task==null) {
            if (taskId!=null) {
                task = (Task<Object>) ((EntityInternal)entity).getManagementContext().getExecutionManager().getTask(taskId);
            }
            if (task==null) {
                throw new IllegalStateException("Task for "+this+" no longer available");
            }
        }
        return task;
    }

    public Entity getEntity() {
        return entity;
    }

    public TypeToken<?> lookupType(String typeName, Supplier<TypeToken<?>> ifUnset) {
        if (Strings.isBlank(typeName)) return ifUnset.get();
        BrooklynClassLoadingContext loader = getEntity() != null ? RegisteredTypes.getClassLoadingContext(getEntity()) : null;
        return new BrooklynTypeNameResolution.BrooklynTypeNameResolver("", loader, true, true).getTypeToken(typeName);
    }

    public String resolve(String expression) {
        // TODO expand interpolated strings deeply in the expression
        return expression;
    }

    public <T> T resolve(Object expression, TypeToken<T> type) {
        // TODO expand interpolated strings in the expression
        return TypeCoercions.coerce(expression, type);
    }

    protected class Body implements Callable<Object> {
        TreeMap<String, WorkflowStepDefinition> steps;

        @Override
        public Object call() throws Exception {
            steps = new TreeMap<>(NaturalOrderComparator.INSTANCE);
            steps.putAll(WorkflowStepResolution.resolveSteps( ((BrooklynObjectInternal)entityOrAdjunctWhereRunning).getManagementContext(), paramsDefiningWorkflow.get(WorkflowCommonConfig.STEPS) ));

            if (currentStepId==null) {
                currentStepId = steps.firstKey();
                while (currentStepId!=null) {
                    runCurrentStepIfPreconditions();
                }

                // TODO what to return
                return null;
            } else {
                // TODO
                throw new IllegalStateException("Cannot resume/retry (yet)");
            }
        }

        protected void runCurrentStepIfPreconditions() {
            WorkflowStepDefinition step = steps.get(currentStepId);
            if (step!=null) {
                if (step.condition!=null) {
                    // TODO evaluation
                    if (!step.condition.apply(this)) {
                        moveToNextSequentialStep("following step " + currentStepId + " where condition does not apply");
                        return;
                    }
                }

                // actually run the step
                DynamicTasks.queue(step.newTask(currentStepId, WorkflowExecutionContext.this)).getUnchecked();
                // TODO error handling

                moveToNextExplicitStep(step.next, "following step "+currentStepId);

            } else {
                moveToNextSequentialStep("following step "+currentStepId+" which is null");
            }
        }

        private void moveToNextSequentialStep(String notes) {
            moveToNextStep(currentStepId, false, notes);
        }

        private void moveToNextExplicitStep(String stepId, String notes) {
            if (stepId==null) moveToNextSequentialStep(notes);
            else moveToNextStep(stepId, true, notes + ", explicit next reference");
        }

        private void moveToNextStep(String stepId, boolean inclusive, String notes) {
            Map.Entry<String, WorkflowStepDefinition> nextStep = inclusive ? steps.ceilingEntry(stepId) : steps.higherEntry(stepId);
            if (nextStep !=null) {
                currentStepId = nextStep.getKey();
                log.debug("Workflow "+workflowInstanceId+" moving to step "+currentStepId+"; "+notes);

            } else {
                currentStepId = null;
                log.debug("Workflow "+workflowInstanceId+" completed; "+notes);
            }
        }
    }


}
