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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class WorkflowExecutionContext {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutionContext.class);

    String name;
    BrooklynObject entityOrAdjunctWhereRunning;
    Entity entity;

    List<Object> steps;
    DslPredicates.DslPredicate condition;

    Map<String,Object> input;
    Object outputDefinition;
    Object output;

    String taskId;
    transient Task<Object> task;

    Integer currentStepIndex;
    Integer previousStepIndex;
    WorkflowStepInstanceExecutionContext currentStepInstance;
    Map<Integer, WorkflowStepInstanceExecutionContext> lastInstanceOfEachStep = MutableMap.of();

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

    protected WorkflowExecutionContext(BrooklynObject entityOrAdjunctWhereRunning, String name, List<Object> steps, Map<String,Object> input, Object output) {
        this.name = name;
        this.entityOrAdjunctWhereRunning = entityOrAdjunctWhereRunning;
        this.entity = entityOrAdjunctWhereRunning instanceof Entity ? (Entity)entityOrAdjunctWhereRunning : ((EntityAdjuncts.EntityAdjunctProxyable)entityOrAdjunctWhereRunning).getEntity();
        this.steps = steps;

        this.input = input;
        this.outputDefinition = output;

        task = Tasks.builder().dynamic(true).displayName(name).body(new Body()).build();
        taskId = task.getId();
    }

    public static final Map<String, Consumer<WorkflowExecutionContext>> PREDEFINED_NEXT_TARGETS = MutableMap.<String, Consumer<WorkflowExecutionContext>>of(
            "start", c -> { c.currentStepIndex = 0; },
            "end", c -> { c.currentStepIndex = c.steps.size(); },
            "default", c -> { c.currentStepIndex++; }).asUnmodifiable();

    public static void validateSteps(ManagementContext mgmt, List<WorkflowStepDefinition> steps, boolean alreadyValidatedIndividualSteps) {
        if (!alreadyValidatedIndividualSteps) {
            steps.forEach(WorkflowStepDefinition::validateStep);
        }

        computeStepsWithExplicitIdById(steps);
    }

    static Map<String,Pair<Integer,WorkflowStepDefinition>> computeStepsWithExplicitIdById(List<WorkflowStepDefinition> steps) {
        Map<String,Pair<Integer,WorkflowStepDefinition>> stepsWithExplicitId = MutableMap.of();
        for (int i = 0; i<steps.size(); i++) {
            WorkflowStepDefinition s = steps.get(i);
            if (s.id != null) {
                if (PREDEFINED_NEXT_TARGETS.containsKey(s.id))
                    throw new IllegalStateException("Token '" + s + "' cannot be used as a step ID");
                Pair<Integer, WorkflowStepDefinition> old = stepsWithExplicitId.put(s.id, Pair.of(i, s));
                if (old != null) throw new IllegalStateException("Same step ID '" + s + "' used for multiple steps ("+(old.getLeft()+1)+" and "+(i+1)+")");
            }
        }
        return stepsWithExplicitId;
    }

    public void setCondition(DslPredicates.DslPredicate condition) {
        this.condition = condition;
    }

    @Override
    public String toString() {
        return "Workflow<" + name + " - " + taskId + ">";
    }

    public Map<String, Object> getWorkflowScratchVariables() {
        return workflowScratchVariables;
    }

    public Maybe<Task<Object>> getOrCreateTask() {
        return getOrCreateTask(true);
    }
    public Maybe<Task<Object>> getOrCreateTask(boolean checkCondition) {
        if (checkCondition && condition!=null) {
            if (!condition.apply(entityOrAdjunctWhereRunning)) return Maybe.absent(new IllegalStateException("This workflow cannot be run at present: condition not satisfied"));
        }

        if (task==null) {
            if (taskId!=null) {
                task = (Task<Object>) getManagementContext().getExecutionManager().getTask(taskId);
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

    public ManagementContext getManagementContext() {
        return ((EntityInternal)getEntity()).getManagementContext();
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
        return new WorkflowExpressionResolution(this, false, false).resolveWithTemplates(expression, type);
    }

    public <T> T resolveCoercingOnly(Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this, false, false).resolveCoercingOnly(expression, type);
    }

    /** as {@link #resolve(Object, TypeToken)}, but returning DSL/supplier for values (so the indication of their dynamic nature is preserved, even if the value returned by it is resolved;
     * this is needed e.g. for conditions which treat dynamic expressions differently to explicit values) */
    public <T> T resolveWrapped(Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this, false, true).resolveWithTemplates(expression, type);
    }

    /** as {@link #resolve(Object, TypeToken)}, but waiting on any expressions which aren't ready */
    public <T> T resolveWaiting(Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this, true, false).resolveWithTemplates(expression, type);
    }

    /** resolution of ${interpolation} and $brooklyn:dsl and deferred suppliers, followed by type coercion */
    public <T> T resolveConfig(ConfigBag config, ConfigKey<T> key) {
        Object v = config.getStringKey(key.getName());
        if (v==null) return null;
        return resolve(v, key.getTypeToken());
    }

    public int getCurrentStepIndex() {
        return currentStepIndex;
    }

    public WorkflowStepInstanceExecutionContext getCurrentStepInstance() {
        return currentStepInstance;
    }

    public int getPreviousStepIndex() {
        return previousStepIndex;
    }

    public Object getPreviousStepOutput() {
        WorkflowStepInstanceExecutionContext ps = lastInstanceOfEachStep.get(previousStepIndex);
        if (ps==null) return null;
        return ps.output;
    }

    public String getName() {
        return name;
    }

    public String getTaskId() {
        return taskId;
    }

    protected class Body implements Callable<Object> {
        List<WorkflowStepDefinition> steps;
        
        transient Map<String,Pair<Integer,WorkflowStepDefinition>> stepsWithExplicitId;
        @JsonIgnore
        public Map<String, Pair<Integer,WorkflowStepDefinition>> getStepsWithExplicitIdById() {
            if (stepsWithExplicitId==null) stepsWithExplicitId = computeStepsWithExplicitIdById(steps);
            return stepsWithExplicitId;
        }

        @Override
        public Object call() throws Exception {
            steps = MutableList.copyOf(WorkflowStepResolution.resolveSteps(getManagementContext(), WorkflowExecutionContext.this.steps));

            if (currentStepIndex==null) {
                currentStepIndex = 0;
                log.debug("Starting workflow '"+name+"', moving to first step "+ workflowStepReference(currentStepIndex));
                while (currentStepIndex < steps.size()) {
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
            WorkflowStepDefinition step = steps.get(currentStepIndex);
            if (step!=null) {
                currentStepInstance = new WorkflowStepInstanceExecutionContext(currentStepIndex, step, WorkflowExecutionContext.this);
                if (step.condition!=null) {
                    if (log.isTraceEnabled()) log.trace("Considering condition "+step.condition+" for "+ workflowStepReference(currentStepIndex));
                    boolean conditionMet = DslPredicates.evaluateDslPredicateWithBrooklynObjectContext(step.getConditionResolved(currentStepInstance), this, entityOrAdjunctWhereRunning);
                    if (log.isTraceEnabled()) log.trace("Considered condition "+step.condition+" for "+ workflowStepReference(currentStepIndex)+": "+conditionMet);
                    if (!conditionMet) {
                        moveToNextStep(null, "Skipping step "+ workflowStepReference(currentStepIndex));
                        return;
                    }
                }

                // no condition or condition met -- record and run the step

                // previously we did this before running, but seems better to delay that,
                // so allow explicit own-ID reference to access previous-instance-of-same-step output,
                // but unqualified access to var does NOT look up previous invocation of same step.
//                WorkflowStepInstanceExecutionContext old = lastInstanceOfEachStep.put(currentStepIndex, currentStepInstance);
//                // put the previous output in output, so repeating steps can reference themselves
//                if (old!=null) currentStepInstance.output = old.output;

                Task<?> t = step.newTask(currentStepInstance);
                try {
                    currentStepInstance.output = DynamicTasks.queue(t).getUnchecked();
                } catch (Exception e) {
                    log.warn("Error in step '"+t.getDisplayName()+"' (rethrowing): "+ Exceptions.collapseText(e));
                    throw Exceptions.propagate(e);
                }

                lastInstanceOfEachStep.put(currentStepIndex, currentStepInstance);
                if (step.output!=null) {
                    // allow output to be customized / overridden
                    currentStepInstance.output = resolve(step.output, Object.class);
                }

                // TODO error handling

                previousStepIndex = currentStepIndex;
                moveToNextStep(step.getNext(), "Completed step "+ workflowStepReference(currentStepIndex));

            } else {
                // moving to floor/ceiling in treemap made sense when numero-ordered IDs are used, but not with list
                throw new IllegalStateException("Cannot find step "+currentStepIndex);
            }
        }

        private void moveToNextStep(String optionalRequestedNextStep, String prefix) {
            prefix = prefix + "; ";
            if (optionalRequestedNextStep==null) {
                currentStepIndex++;
                if (currentStepIndex<steps.size()) {
                    log.debug(prefix + "moving to sequential next step " + workflowStepReference(currentStepIndex));
                } else {
                    log.debug(prefix + "no further steps: Workflow completed");
                }
            } else {
                Consumer<WorkflowExecutionContext> predefined = PREDEFINED_NEXT_TARGETS.get(optionalRequestedNextStep);
                if (predefined!=null) {
                    predefined.accept(WorkflowExecutionContext.this);
                    if (currentStepIndex<steps.size()) {
                        log.debug(prefix + "moving to explicit next step " + workflowStepReference(currentStepIndex) + " for '" + optionalRequestedNextStep + "'");
                    } else {
                        log.debug(prefix + "explicit next '"+optionalRequestedNextStep+"': Workflow completed");
                    }
                } else {
                    Pair<Integer, WorkflowStepDefinition> next = getStepsWithExplicitIdById().get(optionalRequestedNextStep);
                    if (next==null) {
                        log.warn(prefix + "explicit next step '"+optionalRequestedNextStep+"' not found (failing)");
                        throw new NoSuchElementException("Step with ID '"+optionalRequestedNextStep+"' not found");
                    }
                    currentStepIndex = next.getLeft();
                    log.debug(prefix + "moving to explicit next step " + workflowStepReference(currentStepIndex) + " for id '" + optionalRequestedNextStep + "'; ");
                }
            }
        }

        @JsonIgnore
        String workflowStepReference(int index) {
            return getWorkflowStepReference(index, steps.get(index));
        }
    }

    @JsonIgnore
    public String getWorkflowStepReference(int index, WorkflowStepDefinition step) {
        return getWorkflowStepReference(index, step!=null ? step.id : null);
    }
    @JsonIgnore
    String getWorkflowStepReference(int index, String optionalStepId) {
        return taskId+"-"+(index+1)+
                (Strings.isNonBlank(optionalStepId) ? "-"+optionalStepId : "");
    }

}
