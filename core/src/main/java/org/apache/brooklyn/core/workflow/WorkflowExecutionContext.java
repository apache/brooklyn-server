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
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.NaturalOrderComparator;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class WorkflowExecutionContext {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutionContext.class);

    String workflowInstanceId;

    String name;
    BrooklynObject entityOrAdjunctWhereRunning;
    Entity entity;

    // TODO for deserialization, and for sensibility, we might need to change these to be resolved maps
    ConfigBag paramsDefiningWorkflow;
    ConfigBag input;
    Object output;

    String taskId;
    transient Task<Object> task;

    String currentStepId;
    String previousStepId;
    Map<String, WorkflowStepInstanceExecutionContext> lastInstanceOfEachStep = MutableMap.of();

    Map<String,Object> workflowScratchVariables = MutableMap.of();
    // TODO previousStepInstance
    // TODO currentStepInstance

    // deserialization constructor
    private WorkflowExecutionContext() {}

    public WorkflowExecutionContext(String name, BrooklynObject entityOrAdjunctWhereRunning, org.apache.brooklyn.util.core.config.ConfigBag paramsDefiningWorkflow, ConfigBag input) {
        workflowInstanceId = Identifiers.makeRandomId(12);

        this.name = name;
        this.entityOrAdjunctWhereRunning = entityOrAdjunctWhereRunning;
        this.entity = entityOrAdjunctWhereRunning instanceof Entity ? (Entity)entityOrAdjunctWhereRunning : ((EntityAdjuncts.EntityAdjunctProxyable)entityOrAdjunctWhereRunning).getEntity();
        this.paramsDefiningWorkflow = paramsDefiningWorkflow;
        this.input = input;

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

    public Map<String, Object> getWorkflowScratchVariables() {
        return workflowScratchVariables;
    }

    public String getTaskId() {
        return taskId;
    }

    public Maybe<Task<Object>> getOrCreateTask() {
        DslPredicates.DslPredicate condition = paramsDefiningWorkflow.get(WorkflowCommonConfig.CONDITION);
        if (condition!=null) {
            if (!condition.apply(entityOrAdjunctWhereRunning)) return Maybe.absent(new IllegalStateException("This workflow cannot be run at present: condition not satisfied"));
        }

        if (task==null) {
            if (taskId!=null) {
                task = (Task<Object>) ((EntityInternal)entity).getManagementContext().getExecutionManager().getTask(taskId);
            }
            if (task==null) {
                return Maybe.absent(new IllegalStateException("Task for "+this+" no longer available"));
            }
        }
        return Maybe.of(task);
    }

    public Entity getEntity() {
        return entity;
    }

    public TypeToken<?> lookupType(String typeName, Supplier<TypeToken<?>> ifUnset) {
        if (Strings.isBlank(typeName)) return ifUnset.get();
        BrooklynClassLoadingContext loader = getEntity() != null ? RegisteredTypes.getClassLoadingContext(getEntity()) : null;
        return new BrooklynTypeNameResolution.BrooklynTypeNameResolver("", loader, true, true).getTypeToken(typeName);
    }

    public Object resolve(String expression) {
        return resolve(expression, Object.class);
    }

    public <T> T resolve(Object expression, Class<T> type) {
        return resolve(expression, TypeToken.of(type));
    }

    public <T> T resolve(Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this).resolveWithTemplates(expression, type);
    }

    public String getPreviousStepId() {
        return previousStepId;
    }

    public Object getPreviousStepOutput() {
        WorkflowStepInstanceExecutionContext ps = lastInstanceOfEachStep.get(previousStepId);
        if (ps==null) return null;
        return ps.output;
    }

    public String getName() {
        return name;
    }

    public String getWorkflowInstanceId() {
        return workflowInstanceId;
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

                // TODO process any output set in the workflow definition
                // (default is the output of the last step)
                return output;

            } else {
                // TODO
                throw new IllegalStateException("Cannot resume/retry (yet)");
            }
        }

        protected void runCurrentStepIfPreconditions() {
            WorkflowStepDefinition step = steps.get(currentStepId);
            if (step!=null) {
                if (step.condition!=null) {
                    if (!step.condition.apply(this)) {
                        moveToNextSequentialStep("following step " + currentStepId + " where condition does not apply");
                        return;
                    }
                }

                // actually run the step
                WorkflowStepInstanceExecutionContext stepInstance = new WorkflowStepInstanceExecutionContext(currentStepId, step.getInput(), WorkflowExecutionContext.this);

                WorkflowStepInstanceExecutionContext old = lastInstanceOfEachStep.put(currentStepId, stepInstance);
                // put the previous output in output, so repeating steps can reference themselves
                if (old!=null) stepInstance.output = old.output;

                WorkflowExecutionContext.this.output = stepInstance.output = DynamicTasks.queue(step.newTask(stepInstance)).getUnchecked();
                // TODO error handling

                previousStepId = currentStepId;
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
