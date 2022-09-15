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
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.effector.Effectors;
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

import java.util.Collection;
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

    Map<String, Object> steps;
    DslPredicates.DslPredicate condition;

    Map<String,Object> input;
    Object outputDefinition;
    Object output;

    String taskId;
    transient Task<Object> task;

    String currentStepId;
    String previousStepId;
    // TODO currentStepInstance
    Map<String, WorkflowStepInstanceExecutionContext> lastInstanceOfEachStep = MutableMap.of();

    Map<String,Object> workflowScratchVariables = MutableMap.of();

    // deserialization constructor
    private WorkflowExecutionContext() {}

    public static WorkflowExecutionContext of(BrooklynObject entityOrAdjunctWhereRunning, String name, ConfigBag paramsDefiningWorkflow,
                                              Collection<ConfigKey<?>> extraConfigKeys, ConfigBag extraInputs) {

        // parameter defs
        Map<String,ConfigKey<?>> parameters = MutableMap.of();
        Effectors.parseParameters(paramsDefiningWorkflow.get(WorkflowCommonConfig.PARAMETER_DEFS)).forEach(p -> parameters.put(p.getName(), Effectors.asConfigKey(p)));
        if (extraConfigKeys!=null) extraConfigKeys.forEach(p -> parameters.put(p.getName(), p));

        // inputs, unresolved first
        ConfigBag inputRaw = ConfigBag.newInstance();
        inputRaw.putAll(paramsDefiningWorkflow.get(WorkflowCommonConfig.INPUT));
        if (extraInputs!=null) inputRaw.putAll(extraInputs.getAllConfig());
        parameters.values().forEach(p -> {
                    if (p.hasDefaultValue() && !inputRaw.containsKey(p.getName())) inputRaw.put((ConfigKey)p, p.getDefaultValue());
                });

        MutableMap<String,Object> input = MutableMap.of();
        inputRaw.forEach( (k,v) -> {
            ConfigKey<?> kc = parameters.get(k);
            // coerce, but don't freemarker resolve inputs because that's the job of the caller (e.g. in nested workflow, inputs resolved relative to calling workflow)
            Object v2 = kc == null ? v : inputRaw.get(kc);
            input.put(k, v2);
        });

        WorkflowExecutionContext w = new WorkflowExecutionContext(entityOrAdjunctWhereRunning, name,
                paramsDefiningWorkflow.get(WorkflowCommonConfig.STEPS),
                input,
                paramsDefiningWorkflow.get(WorkflowCommonConfig.OUTPUT));

        // some fields need to be resolved at setting time, in the context of the workflow
        w.setCondition(w.resolveConfig(paramsDefiningWorkflow, WorkflowCommonConfig.CONDITION));

        return w;
    }

    protected WorkflowExecutionContext(BrooklynObject entityOrAdjunctWhereRunning, String name, Map<String,Object> steps, Map<String,Object> input, Object output) {
        workflowInstanceId = Identifiers.makeRandomId(12);

        this.name = name;
        this.entityOrAdjunctWhereRunning = entityOrAdjunctWhereRunning;
        this.entity = entityOrAdjunctWhereRunning instanceof Entity ? (Entity)entityOrAdjunctWhereRunning : ((EntityAdjuncts.EntityAdjunctProxyable)entityOrAdjunctWhereRunning).getEntity();
        this.steps = steps;

        this.input = input;
        this.outputDefinition = output;

        task = Tasks.builder().dynamic(true).displayName(name).body(new Body()).build();
        taskId = task.getId();
    }

    public void setCondition(DslPredicates.DslPredicate condition) {
        this.condition = condition;
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

    /** as {@link #resolve(Object, TypeToken)} but without type coercion */
    public Object resolve(String expression) {
        return resolve(expression, Object.class);
    }

    /** as {@link #resolve(Object, TypeToken)} */
    public <T> T resolve(Object expression, Class<T> type) {
        return resolve(expression, TypeToken.of(type));
    }

    /** resolution of ${interpolation} and $brooklyn:dsl and deferred suppliers, followed by type coercion */
    public <T> T resolve(Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this, false).resolveWithTemplates(expression, type);
    }

    /** as {@link #resolve(Object, TypeToken)}, but returning DSL/supplier for values (so their "impure" status is preserved) */
    public <T> T resolveWrapped(Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this, true).resolveWithTemplates(expression, type);
    }

    /** resolution of ${interpolation} and $brooklyn:dsl and deferred suppliers, followed by type coercion */
    public <T> T resolveConfig(ConfigBag config, ConfigKey<T> key) {
        Object v = config.getStringKey(key.getName());
        if (v==null) return null;
        return resolve(v, key.getTypeToken());
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
            steps.putAll(WorkflowStepResolution.resolveSteps( ((BrooklynObjectInternal)entityOrAdjunctWhereRunning).getManagementContext(), WorkflowExecutionContext.this.steps));

            if (currentStepId==null) {
                currentStepId = steps.firstKey();
                while (currentStepId!=null) {
                    runCurrentStepIfPreconditions();
                }

                if (outputDefinition==null) {
                    // (default is the output of the last step)
                    output = getPreviousStepOutput();
                } else {
                    output = resolve(outputDefinition, Object.class);
                }

                return output;

            } else {
                // TODO
                throw new IllegalStateException("Cannot resume/retry (yet)");
            }
        }

        protected void runCurrentStepIfPreconditions() {
            WorkflowStepDefinition step = steps.get(currentStepId);
            if (step!=null) {
                WorkflowStepInstanceExecutionContext stepInstance = new WorkflowStepInstanceExecutionContext(currentStepId, step.getInput(), WorkflowExecutionContext.this);

                if (step.condition!=null) {
                    if (!step.getConditionResolved(stepInstance).apply(this)) {
                        moveToNextSequentialStep("following step " + currentStepId + " where condition does not apply");
                        return;
                    }
                }

                // no condition or condition met -- record and run the step
                WorkflowStepInstanceExecutionContext old = lastInstanceOfEachStep.put(currentStepId, stepInstance);
                // put the previous output in output, so repeating steps can reference themselves
                if (old!=null) stepInstance.output = old.output;

                stepInstance.output = DynamicTasks.queue(step.newTask(stepInstance)).getUnchecked();

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
