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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import java.util.function.Supplier;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkflowExecutionContext {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutionContext.class);

    String name;
    @Nullable BrooklynObject adjunct;
    Entity entity;

    public enum WorkflowStatus {
        STAGED(false, false, false, true),
        RUNNING(true, false, false, true),
        SUCCESS(true, true, false, true),
        /** useful information, usually cannot persisted by the time we've set this the first time, but could set on rebind */ ERROR_SHUTDOWN(true, true, true, false),
        /** task cancelled or other interrupt, usually recursively */ ERROR_CANCELLED(true, true, true, true),
        /** task cancelled or other interrupt, usually recursively */ ERROR_ENTITY_DESTROYED(true, true, true, false),
        /** any other error, e.g. workflow step failed or data not immediately available (the interrupt used internally is not relevant) */ ERROR(true, true, true, true);

        public final boolean started;
        public final boolean ended;
        public final boolean error;
        public final boolean persistable;

        WorkflowStatus(boolean started, boolean ended, boolean error, boolean persistable) { this.started = started; this.ended = ended; this.error = error; this.persistable = persistable; }
    }

    WorkflowStatus status;

    transient WorkflowExecutionContext parent;
    String parentId;

    List<Object> stepsDefinition;
    DslPredicates.DslPredicate condition;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<String,Object> input = MutableMap.of();
    Object outputDefinition;
    Object output;

    String workflowId;
    /** current or most recent executing task created for this workflow, corresponding to task */
    String taskId;
    transient Task<Object> task;

    /** all tasks created for this workflow */
    Set<WorkflowReplayUtils.WorkflowReplayRecord> replays = MutableSet.of();
    transient WorkflowReplayUtils.WorkflowReplayRecord replayCurrent = null;
    /** null if not replayable; number if a step is the last so marked; -1 if should replay from start, -2 if completed successfully */
    Integer replayableLastStep;
    boolean replayableFromStart = true;  // starts off true, cleared when task starts to run
    WorkflowReplayUtils.ReplayableOption replayableCurrentSetting;

    Integer currentStepIndex;
    Integer previousStepIndex;
    String previousStepTaskId;

    WorkflowStepInstanceExecutionContext currentStepInstance;

    Map<Integer, OldStepRecord> oldStepInfo = MutableMap.of();

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class OldStepRecord {
        /** count of runs started */
        int countStarted = 0;
        /** count of runs completed */
        int countCompleted = 0;

        /** context for last _completed_ instance of step */
        WorkflowStepInstanceExecutionContext context;
        /** is step replayable */
        WorkflowReplayUtils.ReplayableOption replayableCurrentSetting;
        boolean replayableFromHere;
        /** scratch for last _started_ instance of step */
        Map<String,Object> workflowScratch;
        /** steps that immediately preceded this, updated when _this_ step started */
        Set<Integer> previous;
        /** steps that immediately followed this, updated when _next_ step started */
        Set<Integer> next;

        String previousTaskId;
        String nextTaskId;
    }

    Map<String,Object> workflowScratchVariables = MutableMap.of();

    // deserialization constructor
    private WorkflowExecutionContext() {}

    public static WorkflowExecutionContext newInstancePersisted(BrooklynObject entityOrAdjunctWhereRunning, String name, ConfigBag paramsDefiningWorkflow,
                                                                Collection<ConfigKey<?>> extraConfigKeys, ConfigBag extraInputs, Map<String, Object> optionalTaskFlags) {
        WorkflowExecutionContext w = newInstanceUnpersistedWithParent(entityOrAdjunctWhereRunning, null, name, paramsDefiningWorkflow, extraConfigKeys, extraInputs, optionalTaskFlags);
        w.persist();
        return w;
    }

    public static WorkflowExecutionContext newInstanceUnpersistedWithParent(BrooklynObject entityOrAdjunctWhereRunning, WorkflowExecutionContext parent, String name, ConfigBag paramsDefiningWorkflow,
                                              Collection<ConfigKey<?>> extraConfigKeys, ConfigBag extraInputs, Map<String, Object> optionalTaskFlags) {

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

        WorkflowExecutionContext w = new WorkflowExecutionContext(entityOrAdjunctWhereRunning, parent, name,
                paramsDefiningWorkflow.get(WorkflowCommonConfig.STEPS),
                input,
                paramsDefiningWorkflow.get(WorkflowCommonConfig.OUTPUT),
                paramsDefiningWorkflow.get(WorkflowCommonConfig.REPLAYABLE),
                optionalTaskFlags);

        // some fields need to be resolved at setting time, in the context of the workflow
        w.setCondition(w.resolveConfig(paramsDefiningWorkflow, WorkflowCommonConfig.CONDITION));

        // finished -- checkpoint noting this has been created but not yet started
        w.status = WorkflowStatus.STAGED;
        return w;
    }

    protected WorkflowExecutionContext(BrooklynObject entityOrAdjunctWhereRunning, WorkflowExecutionContext parent, String name,
                                       List<Object> stepsDefinition, Map<String,Object> input, Object output,
                                       WorkflowReplayUtils.ReplayableOption replayableWorkflowSetting, Map<String, Object> optionalTaskFlags) {
        initParent(parent);
        this.name = name;
        this.adjunct = entityOrAdjunctWhereRunning instanceof Entity ? null : entityOrAdjunctWhereRunning;
        this.entity = entityOrAdjunctWhereRunning instanceof Entity ? (Entity)entityOrAdjunctWhereRunning : ((EntityAdjuncts.EntityAdjunctProxyable)entityOrAdjunctWhereRunning).getEntity();
        this.stepsDefinition = stepsDefinition;

        this.input = input;
        this.outputDefinition = output;
        this.replayableCurrentSetting = replayableWorkflowSetting;

        TaskBuilder<Object> tb = Tasks.builder().dynamic(true);
        if (optionalTaskFlags!=null) tb.flags(optionalTaskFlags);
        else tb.displayName(name);
        task = tb.body(new Body()).build();
        WorkflowReplayUtils.updateOnWorkflowStartOrReplay(this, task, "initial run", false);
        workflowId = taskId = task.getId();
        TaskTags.addTagDynamically(task, BrooklynTaskTags.WORKFLOW_TAG);
        TaskTags.addTagDynamically(task, BrooklynTaskTags.tagForWorkflow(this));

        // currently workflow ID is the same as the task ID assigned initially. (but if replayed they will be different.)
        // there is no deep reason or need for this, it is just convenient, and used for tests.
        //this.workflowId = Identifiers.makeRandomId(8);
    }

    public void initParent(WorkflowExecutionContext parent) {
        this.parent = parent;
        this.parentId = parent ==null ? null : parent.workflowId;
    }

    public static final Map<String, Consumer<WorkflowExecutionContext>> PREDEFINED_NEXT_TARGETS = MutableMap.<String, Consumer<WorkflowExecutionContext>>of(
            "start", c -> { c.currentStepIndex = 0; },
            "end", c -> { c.currentStepIndex = c.stepsDefinition.size(); },
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
        return "Workflow<" + name + " - " + workflowId + ">";
    }

    @JsonIgnore
    public BrooklynObject getEntityOrAdjunctWhereRunning() {
        if (adjunct!=null) return adjunct;
        return entity;
    }

    public String getParentId() {
        return parentId;
    }

    public Map<String, Object> getWorkflowScratchVariables() {
        return workflowScratchVariables;
    }

    @JsonIgnore
    public Maybe<Task<Object>> getTask(boolean checkCondition) {
        if (checkCondition && condition!=null) {
            if (!condition.apply(getEntityOrAdjunctWhereRunning())) return Maybe.absent(new IllegalStateException("This workflow cannot be run at present: condition not satisfied"));
        }

        if (task==null) {
            if (taskId !=null) {
                task = (Task<Object>) getManagementContext().getExecutionManager().getTask(taskId);
            }
            if (task==null) {
                return Maybe.absent(new IllegalStateException("Task for "+this+" no longer available"));
            }
        }
        return Maybe.of(task);
    }

    public synchronized Task<Object> createTaskReplayingFromStep(int stepIndex, String reason, boolean forced) {
        if (task!=null && !task.isDone()) {
            throw new IllegalStateException("Cannot replay ongoing workflow");
        }

        if (!forced) {
            Set<Integer> considered = MutableSet.of();
            Set<Integer> possibleOthers = MutableSet.of();
            while (true) {
                if (WorkflowReplayUtils.isReplayable(this, stepIndex) || stepIndex==-1) {
                    break;
                }

                // look at the previous step
                OldStepRecord osi = oldStepInfo.get(stepIndex);
                if (osi==null) {
                    log.warn("Unable to backtrack from step "+(stepIndex)+"; no step information. Replaying overall workflow.");
                    stepIndex = -1;
                    break;
                }

                Set<Integer> prev = osi.previous;
                if (prev==null || prev.isEmpty()) {
                    log.warn("Unable to backtrack from step "+(stepIndex)+"; no previous step recorded. Replaying overall workflow.");
                    stepIndex = -1;
                    break;
                }

                boolean repeating = !considered.add(stepIndex);
                if (repeating) {
                    if (possibleOthers.size()!=1) {
                        log.warn("Unable to backtrack from step " + (stepIndex) + "; ambiguous precedents " + prev + " / " + possibleOthers + ". Replaying overall workflow.");
                        stepIndex = -1;
                        break;
                    } else {
                        stepIndex = possibleOthers.iterator().next();
                        continue;
                    }
                }

                Iterator<Integer> prevI = prev.iterator();
                stepIndex = prevI.next();
                while (prevI.hasNext()) {
                    possibleOthers.add(prevI.next());
                }
            }
        }

        if (!forced && !WorkflowReplayUtils.isReplayable(this, stepIndex)) {
            throw new IllegalStateException("Workflow is not replayable");
        }

        return createTaskReplayingWithCustom(new WorkflowStepDefinition.ReplayContinuationInstructions(stepIndex, reason, null, forced));
    }

    public synchronized Task<Object> createTaskReplayingFromStart(String reason, boolean forced) {
        return createTaskReplayingFromStep(-1, reason, forced);
    }

    public synchronized Task<Object> createTaskReplayingLast(String reason, boolean forced) {
        return createTaskReplayingLast(reason, forced, null);
    }

    public synchronized Task<Object> createTaskReplayingLastForcedWithCustom(String reason, Runnable code) {
        return createTaskReplayingLast(reason, true, code);
    }

    protected synchronized Task<Object> createTaskReplayingLast(String reason, boolean forced, Runnable code) {
        if (task!=null && !task.isDone()) {
            throw new IllegalStateException("Cannot replay ongoing workflow");
        }

        Integer replayFromStep = null;
        if (currentStepIndex == null) {
            // not yet started
            replayFromStep = -1;
        } else if (currentStepInstance == null || currentStepInstance.stepIndex != currentStepIndex) {
            // replaying from a different step, or current step which has either not run or completed but didn't save
            log.debug("Replaying workflow '" + name + "', cannot replay within step " + currentStepIndex + " because step instance not known; will reinitialize then replay that step");
            replayFromStep = currentStepIndex;
        }

        if (!forced) {
            int replayFromStepActual = replayFromStep!=null ? replayFromStep : currentStepIndex;
            if (!WorkflowReplayUtils.isReplayable(this, replayFromStepActual)) {
                if (code!=null) throw new IllegalArgumentException("Cannot supply code without forcing");
                return createTaskReplayingFromStep(replayFromStepActual, reason, forced);
            }
        }

        return createTaskReplayingWithCustom(new WorkflowStepDefinition.ReplayContinuationInstructions(replayFromStep, reason, code, forced));
    }

    public void markShutdown() {
        log.debug(this+" was "+this.status+" but now marking as "+WorkflowStatus.ERROR_SHUTDOWN+"; compensating workflow should be triggered shortly");
        this.status = WorkflowStatus.ERROR_SHUTDOWN;
        // don't persist; that will happen when workflows are kicked off
    }

    public synchronized Task<Object> createTaskReplayingWithCustom(WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions) {
        String explanation = continuationInstructions!=null ? continuationInstructions.customBehaviourExplanation!=null ? continuationInstructions.customBehaviourExplanation : "no explanation" : "no continuation";
        if (task!=null && !task.isDone()) {
            throw new IllegalStateException("Cannot replay ongoing workflow with custom behavior ("+explanation+")");
        }
        task = Tasks.builder().dynamic(true).displayName(name + " (" + explanation + ")")
                .tag(BrooklynTaskTags.tagForWorkflow(this))
                .tag(BrooklynTaskTags.WORKFLOW_TAG)
                .body(new Body(continuationInstructions)).build();
        WorkflowReplayUtils.updateOnWorkflowStartOrReplay(this, task, continuationInstructions.customBehaviourExplanation, continuationInstructions.stepToReplayFrom!=null && continuationInstructions.stepToReplayFrom!=-1);

        taskId = task.getId();

        return task;
    }

    public Entity getEntity() {
        return entity;
    }

    @JsonIgnore
    public ManagementContext getManagementContext() {
        return ((EntityInternal)getEntity()).getManagementContext();
    }

    @JsonIgnore
    protected WorkflowStatePersistenceViaSensors getPersister() {
        return new WorkflowStatePersistenceViaSensors(getManagementContext());
    }

    public void persist() {
        getPersister().checkpoint(this);
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

    public Integer getCurrentStepIndex() {
        return currentStepIndex;
    }

    public WorkflowStepInstanceExecutionContext getCurrentStepInstance() {
        return currentStepInstance;
    }

    public Integer getPreviousStepIndex() {
        return previousStepIndex;
    }

    @JsonIgnore
    public Object getPreviousStepOutput() {
        OldStepRecord last = oldStepInfo.get(previousStepIndex);
        if (last!=null && last.context!=null) return last.context.output;
        return null;
    }

    public Object getOutput() {
        return output;
    }

    public String getName() {
        return name;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public String getTaskId() {
        return taskId;
    }

    public Set<WorkflowReplayUtils.WorkflowReplayRecord> getReplays() {
        return replays;
    }

    public WorkflowStatus getStatus() {
        return status;
    }

    protected class Body implements Callable<Object> {
        /** step to start with, if replaying. if null when replaying, starts at the current step but skipping the reinitialization of that step (all inputs, conditions, etc recomputed) */
        private Integer replayFromStep;
        private boolean replaying;
        private WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions;

        List<WorkflowStepDefinition> stepsResolved;
        
        transient Map<String,Pair<Integer,WorkflowStepDefinition>> stepsWithExplicitId;

        public Body() {
            replayFromStep = null;
            replaying = false;
        }

        public Body(WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions) {
            this.replayFromStep = continuationInstructions.stepToReplayFrom;
            this.continuationInstructions = continuationInstructions;
            replaying = true;
        }

        @JsonIgnore
        public Map<String, Pair<Integer,WorkflowStepDefinition>> getStepsWithExplicitIdById() {
            if (stepsWithExplicitId==null) stepsWithExplicitId = computeStepsWithExplicitIdById(stepsResolved);
            return stepsWithExplicitId;
        }

        @Override
        public Object call() throws Exception {
            try {
                stepsResolved = MutableList.copyOf(WorkflowStepResolution.resolveSteps(getManagementContext(), WorkflowExecutionContext.this.stepsDefinition));

                WorkflowReplayUtils.updateOnWorkflowTaskStartup(WorkflowExecutionContext.this, task, stepsResolved, !replaying, replayFromStep);

                // show task running
                status = WorkflowStatus.RUNNING;

                if (replaying) {
                    if (replayFromStep!=null && replayFromStep==-1) {
                        log.debug("Replaying workflow '" + name + "', from start " +
                                " (was at "+(currentStepIndex==null ? "<UNSTARTED>" : workflowStepReference(currentStepIndex))+")");
                        currentStepIndex = 0;
                        workflowScratchVariables = MutableMap.of();

                    } else {

                        // replaying workflow
                        log.debug("Replaying workflow '" + name + "', from step " + (replayFromStep == null ? "<CURRENT>" : workflowStepReference(replayFromStep)) +
                                " (was at " + (currentStepIndex == null ? "<UNSTARTED>" : workflowStepReference(currentStepIndex)) + ")");
                        if (replayFromStep == null) {
                            // replayFromLast should correct the cases below
                            if (currentStepIndex == null) {
                                throw new IllegalStateException("Invalid instructions to continue from last bypassing convenience method, and there is no last");
                            } else if (currentStepInstance == null || currentStepInstance.stepIndex != currentStepIndex) {
                                throw new IllegalStateException("Invalid instructions to continue from last step which is unknown, bypassing convenience method");
                            }
                        }
                        if (replayFromStep != null) {
                            OldStepRecord last = oldStepInfo.get(replayFromStep);
                            if (last != null) workflowScratchVariables = last.workflowScratch;
                            if (workflowScratchVariables == null) workflowScratchVariables = MutableMap.of();
                            currentStepIndex = replayFromStep;
                        }
                    }
                } else {
                    if (replayFromStep == null && currentStepIndex == null) {
                        currentStepIndex = 0;
                        log.debug("Starting workflow '" + name + "', moving to first step " + workflowStepReference(currentStepIndex));

                    } else {
                        // shouldn't come here
                        throw new IllegalStateException("Should either be replaying or unstarted, but not invoked as replaying, and current="+currentStepIndex+" replay="+replayFromStep);
                    }
                }

                if (taskId != Tasks.current().getId()) throw new IllegalStateException("Running workflow in unexpected task, "+taskId+" does not match "+task);

                if (replaying && replayFromStep==null) {
                    // clear output before re-running
                    currentStepInstance.output = null;
                    currentStepInstance.injectContext(WorkflowExecutionContext.this);
                    log.debug("Replaying workflow '" + name + "', reusing instance "+currentStepInstance+" for step " + workflowStepReference(currentStepIndex) + ")");
                    runCurrentStepInstanceApproved(stepsResolved.get(currentStepIndex));
                }

                while (currentStepIndex < stepsResolved.size()) {
                    runCurrentStepIfPreconditions();
                }

                if (outputDefinition == null) {
                    // (default is the output of the last step)
                    output = getPreviousStepOutput();
                } else {
                    output = resolve(outputDefinition, Object.class);
                }

                // finished -- checkpoint noting previous step and null for current because finished
                status = WorkflowStatus.SUCCESS;
                // record how it ended
                oldStepInfo.compute(previousStepIndex==null ? -1 : previousStepIndex, (index, old) -> {
                    if (old==null) old = new OldStepRecord();
                    old.next = MutableSet.<Integer>of(-1).putAll(old.next);
                    old.nextTaskId = null;
                    return old;
                });
                oldStepInfo.compute(-1, (index, old) -> {
                    if (old==null) old = new OldStepRecord();
                    old.previous = MutableSet.<Integer>of(previousStepIndex).putAll(old.previous);
                    old.previousTaskId = previousStepTaskId;
                    return old;
                });

                WorkflowReplayUtils.updateOnWorkflowSuccess(WorkflowExecutionContext.this, task, output);

                persist();

                return output;

            } catch (Throwable e) {
                WorkflowReplayUtils.updateOnWorkflowError(WorkflowExecutionContext.this, task, e);

                try {
                    // do not propagateIfFatal, we need to handle most throwables
                    if (Exceptions.isCausedByInterruptInAnyThread(e) || Exceptions.getFirstThrowableMatching(e, t -> t instanceof CancellationException)!=null) {
                        WorkflowStatus provisionalStatus;
                        if (!Thread.currentThread().isInterrupted()) {
                            // might be a data model error
                            if (Exceptions.getFirstThrowableOfType(e, TemplateProcessor.TemplateModelDataUnavailableException.class) != null) {
                                provisionalStatus = WorkflowStatus.ERROR;
                            } else {
                                // cancelled or a subtask interrupted
                                provisionalStatus = WorkflowStatus.ERROR_CANCELLED;
                            }
                        } else {
                            provisionalStatus = WorkflowStatus.ERROR_CANCELLED;
                        }
                        if (provisionalStatus == WorkflowStatus.ERROR_CANCELLED) {
                            if (!getManagementContext().isRunning()) {
                                // if mgmt is shutting down we should record that. maybe enough if thread is interrupted we note that.
                                provisionalStatus = WorkflowStatus.ERROR_SHUTDOWN;
                            } else if (Entities.isUnmanagingOrNoLongerManaged(entity)) {
                                provisionalStatus = WorkflowStatus.ERROR_ENTITY_DESTROYED;
                            }
                        }
                        status = provisionalStatus;
                    } else {
                        status = WorkflowStatus.ERROR;
                    }

                    if (status.persistable) {
                        log.debug("Error running workflow " + this + "; will persist then rethrow: " + e);
                        log.trace("Error running workflow " + this + "; will persist then rethrow (details): " + e, e);
                        persist();
                    } else {
                        log.debug("Error running workflow " + this + ", unsurprising because "+status);
                    }

                } catch (Throwable e2) {
                    log.error("Error persisting workflow " + this + " after error in workflow; persistence error: "+e2);
                    log.debug("Error persisting workflow " + this + " after error in workflow; persistence error (details): "+e2, e2);
                    log.warn("Error running workflow " + this + ", rethrowing without persisting because of persistence error (above): "+e);
                    log.trace("Error running workflow " + this + ", rethrowing without persisting because of persistence error (above): "+e, e);
                }

                throw Exceptions.propagate(e);
            }
        }

        protected void runCurrentStepIfPreconditions() {
            WorkflowStepDefinition step = stepsResolved.get(currentStepIndex);
            if (step!=null) {
                currentStepInstance = new WorkflowStepInstanceExecutionContext(currentStepIndex, step, WorkflowExecutionContext.this);
                if (step.condition!=null) {
                    if (log.isTraceEnabled()) log.trace("Considering condition "+step.condition+" for "+ workflowStepReference(currentStepIndex));
                    boolean conditionMet = DslPredicates.evaluateDslPredicateWithBrooklynObjectContext(step.getConditionResolved(currentStepInstance), this, getEntityOrAdjunctWhereRunning());
                    if (log.isTraceEnabled()) log.trace("Considered condition "+step.condition+" for "+ workflowStepReference(currentStepIndex)+": "+conditionMet);
                    if (!conditionMet) {
                        moveToNextStep(null, "Skipping step "+ workflowStepReference(currentStepIndex));
                        return;
                    }
                }

                // no condition or condition met -- record and run the step

                runCurrentStepInstanceApproved(step);

            } else {
                // moving to floor/ceiling in treemap made sense when numero-ordered IDs are used, but not with list
                throw new IllegalStateException("Cannot find step "+currentStepIndex);
            }
        }

        private void runCurrentStepInstanceApproved(WorkflowStepDefinition step) {
            Task<?> t;
            if (replaying) {
                t = step.newTaskContinuing(currentStepInstance, continuationInstructions);
                continuationInstructions = null;
                replaying = false;
            } else {
                t = step.newTask(currentStepInstance);
            }

            // about to run -- checkpoint noting current and previous steps
            OldStepRecord currentStepRecord = oldStepInfo.compute(currentStepIndex, (index, old) -> {
                if (old == null) old = new OldStepRecord();
                old.countStarted++;
                if (!workflowScratchVariables.isEmpty())
                    old.workflowScratch = MutableMap.copyOf(workflowScratchVariables);
                else old.workflowScratch = null;
                old.previous = MutableSet.<Integer>of(previousStepIndex == null ? -1 : previousStepIndex).putAll(old.previous);
                old.previousTaskId = previousStepTaskId;
                old.nextTaskId = null;
                return old;
            });
            oldStepInfo.compute(previousStepIndex==null ? -1 : previousStepIndex, (index, old) -> {
                if (old==null) old = new OldStepRecord();
                old.next = MutableSet.<Integer>of(currentStepIndex).putAll(old.next);
                old.nextTaskId = t.getId();
                return old;
            });

            // and update replayable info
            WorkflowReplayUtils.updateOnWorkflowStepChange(currentStepRecord, currentStepInstance, step);

            persist();

            Runnable next = () -> moveToNextStep(step.getNext(), "Completed step "+ workflowStepReference(currentStepIndex));

            try {
                currentStepInstance.output = DynamicTasks.queue(t).getUnchecked();
                if (step.output!=null) {
                    // allow output to be customized / overridden
                    currentStepInstance.output = resolve(step.output, Object.class);
                }

            } catch (Exception e) {

                // TODO custom error handling (also needed at workflow level)
                // if onError
                // log debug
                // try
                // next = () -> moveToNextStep(step.getNext(), "Completed step "+ workflowStepReference(currentStepIndex));
                // catch
                // log warn throw propagate
                // else

                log.warn("Error in step '"+t.getDisplayName()+"' (rethrowing): "+ Exceptions.collapseText(e));
                throw Exceptions.propagate(e);
            }

            oldStepInfo.compute(currentStepIndex, (index, old) -> {
                if (old==null) {
                    log.warn("Lost old step info for "+this+", step "+index);
                    old = new OldStepRecord();
                }
                old.countCompleted++;
                // okay if this gets picked up by accident because we will check the stepIndex it records against the currentStepIndex,
                // and ignore it if different
                old.context = currentStepInstance;
                return old;
            });

            previousStepTaskId = currentStepInstance.taskId;
            previousStepIndex = currentStepIndex;
            next.run();
        }

        private void moveToNextStep(String optionalRequestedNextStep, String prefix) {
            prefix = prefix + "; ";
            if (optionalRequestedNextStep==null) {
                currentStepIndex++;
                if (currentStepIndex< stepsResolved.size()) {
                    log.debug(prefix + "moving to sequential next step " + workflowStepReference(currentStepIndex));
                } else {
                    log.debug(prefix + "no further steps: Workflow completed");
                }
            } else {
                Consumer<WorkflowExecutionContext> predefined = PREDEFINED_NEXT_TARGETS.get(optionalRequestedNextStep);
                if (predefined!=null) {
                    predefined.accept(WorkflowExecutionContext.this);
                    if (currentStepIndex< stepsResolved.size()) {
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

        String workflowStepReference(int index) {
            if (index>=stepsResolved.size()) return getWorkflowStepReference(index, "<END>");
            return getWorkflowStepReference(index, stepsResolved.get(index));
        }
    }

    @JsonIgnore
    public String getWorkflowStepReference(int index, WorkflowStepDefinition step) {
        return getWorkflowStepReference(index, step!=null ? step.id : null);
    }
    @JsonIgnore
    String getWorkflowStepReference(int index, String optionalStepId) {
        return workflowId+"-"+(index+1)+
                (Strings.isNonBlank(optionalStepId) ? "-"+optionalStepId : "");
    }

}
