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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.Iterables;
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
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.*;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkflowExecutionContext {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutionContext.class);

    public static final int STEP_INDEX_FOR_START = -1;
    public static final int STEP_INDEX_FOR_END = -2;
    public static final int STEP_INDEX_FOR_ERROR_HANDLER = -3;


    String name;
    @Nullable BrooklynObject adjunct;
    Entity entity;

    public enum WorkflowStatus {
        STAGED(false, false, false, true),
        RUNNING(true, false, false, true),
        SUCCESS(true, true, false, true),
        /** useful information, usually cannot persisted by the time we've set this the first time, but could set on rebind */ ERROR_SHUTDOWN(true, true, true, false),
        /** task failed because entity destroyed */ ERROR_ENTITY_DESTROYED(true, true, true, false),
        /** task cancelled, timeout, or other interrupt, usually recursively (but not entity destroyed or server shutdown) */ ERROR_CANCELLED(true, true, true, true),
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

    // should be treated as raw json
    @JsonDeserialize(contentUsing = JsonPassThroughDeserializer.class)
    List<Object> stepsDefinition;

    DslPredicates.DslPredicate condition;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<String,Object> input = MutableMap.of();

    Object outputDefinition;
    Object output;

    Duration timeout;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    List<Object> onError;

    String workflowId;
    /** current or most recent executing task created for this workflow, corresponding to task */
    String taskId;
    transient Task<Object> task;

    @JsonProperty("expiryKey")
    String expiryKey;

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

    /** set if an error handler is the last thing which ran */
    String errorHandlerTaskId;
    /** set for the last _step_ inside the error handler */
    WorkflowStepInstanceExecutionContext errorHandlerContext;

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

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<String,List<Instant>> retryRecords = MutableMap.of();

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

        w.timeout = paramsDefiningWorkflow.get(WorkflowCommonConfig.TIMEOUT);
        w.onError = paramsDefiningWorkflow.get(WorkflowCommonConfig.ON_ERROR);
        // fail fast if error steps not resolveable
        WorkflowStepResolution.resolveOnErrorSteps(w.getManagementContext(), w.onError);

        // some fields need to be resolved at setting time, in the context of the workflow
        w.setCondition(w.resolveWrapped(paramsDefiningWorkflow.getStringKey(WorkflowCommonConfig.CONDITION.getName()), WorkflowCommonConfig.CONDITION.getTypeToken()));

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
        if (Strings.isBlank(tb.getDisplayName())) tb.displayName(name);
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

    public static final Map<String, Function<WorkflowExecutionContext,Integer>> PREDEFINED_NEXT_TARGETS = MutableMap.<String, Function<WorkflowExecutionContext,Integer>>of(
            "start", c -> 0,
            "end", c -> c.stepsDefinition.size(),
            "default", c -> c.currentStepIndex+1).asUnmodifiable();

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
                if (PREDEFINED_NEXT_TARGETS.containsKey(s.id.toLowerCase()))
                    throw new IllegalStateException("Token '" + s + "' is a reserved word and cannot be used as a step ID");

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

    public Map<String, List<Instant>> getRetryRecords() {
        return retryRecords;
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

    public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayingFromStep(int stepIndex, String reason, boolean forced) {
        if (!forced) {
            Set<Integer> considered = MutableSet.of();
            Set<Integer> possibleOthers = MutableSet.of();
            while (true) {
                if (WorkflowReplayUtils.isReplayable(this, stepIndex) || stepIndex == STEP_INDEX_FOR_START) {
                    break;
                }

                // look at the previous step
                OldStepRecord osi = oldStepInfo.get(stepIndex);
                if (osi == null) {
                    log.warn("Unable to backtrack from step " + (stepIndex) + "; no step information. Replaying overall workflow.");
                    stepIndex = STEP_INDEX_FOR_START;
                    break;
                }

                Set<Integer> prev = osi.previous;
                if (prev == null || prev.isEmpty()) {
                    log.warn("Unable to backtrack from step " + (stepIndex) + "; no previous step recorded. Replaying overall workflow.");
                    stepIndex = STEP_INDEX_FOR_START;
                    break;
                }

                boolean repeating = !considered.add(stepIndex);
                if (repeating) {
                    if (possibleOthers.size() != 1) {
                        log.warn("Unable to backtrack from step " + (stepIndex) + "; ambiguous precedents " + prev + " / " + possibleOthers + ". Replaying overall workflow.");
                        stepIndex = STEP_INDEX_FOR_START;
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

        return new WorkflowStepDefinition.ReplayContinuationInstructions(stepIndex, reason, null, forced);
    }

    public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayingFromStart(String reason, boolean forced) {
        return makeInstructionsForReplayingFromStep(STEP_INDEX_FOR_START, reason, forced);
    }

    public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayingLast(String reason, boolean forced) {
        return makeInstructionsForReplayingLast(reason, forced, null);
    }

    public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayingLastForcedWithCustom(String reason, Runnable code) {
        return makeInstructionsForReplayingLast(reason, true, code);
    }

    protected WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayingLast(String reason, boolean forced, Runnable code) {
        Integer replayFromStep = null;
        if (currentStepIndex == null) {
            // not yet started
            replayFromStep = STEP_INDEX_FOR_START;
        } else if (currentStepInstance == null || currentStepInstance.stepIndex != currentStepIndex) {
            // replaying from a different step, or current step which has either not run or completed but didn't save
            log.debug("Replaying workflow '" + name + "', cannot replay within step " + currentStepIndex + " because step instance not known; will reinitialize then replay that step");
            replayFromStep = currentStepIndex;
        }

        if (!forced) {
            int replayFromStepActual = replayFromStep!=null ? replayFromStep : currentStepIndex;
            if (!WorkflowReplayUtils.isReplayable(this, replayFromStepActual)) {
                if (code!=null) throw new IllegalArgumentException("Cannot supply code without forcing");
                return makeInstructionsForReplayingFromStep(replayFromStepActual, reason, forced);
            }
        }

        return new WorkflowStepDefinition.ReplayContinuationInstructions(replayFromStep, reason, code, forced);
    }

    public void markShutdown() {
        log.debug(this+" was "+this.status+" but now marking as "+WorkflowStatus.ERROR_SHUTDOWN+"; compensating workflow should be triggered shortly");
        this.status = WorkflowStatus.ERROR_SHUTDOWN;
        // don't persist; that will happen when workflows are kicked off
    }

    public Task<Object> createTaskReplaying(WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions) {
        if (task != null && !task.isDone()) {
            if (!task.isSubmitted()) {
                log.warn("Abandoning workflow task that was never submitted: " + task + " for " + this);
            } else {
                throw new IllegalStateException("Cannot replay ongoing workflow, given "+continuationInstructions);
            }
        }

        String explanation = continuationInstructions.customBehaviourExplanation!=null ? continuationInstructions.customBehaviourExplanation : "no explanation";
        task = Tasks.builder().dynamic(true).displayName(name + " (" + explanation + ")")
                .tag(BrooklynTaskTags.tagForWorkflow(this))
                .tag(BrooklynTaskTags.WORKFLOW_TAG)
                .body(new Body(continuationInstructions)).build();
        WorkflowReplayUtils.updateOnWorkflowStartOrReplay(this, task, continuationInstructions.customBehaviourExplanation, continuationInstructions.stepToReplayFrom!=null && continuationInstructions.stepToReplayFrom!=STEP_INDEX_FOR_START);

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

    @JsonIgnore
    /** Error handlers _could_ launch sub-workflow, but they typically don't */
    protected boolean isInErrorHandlerSubWorkflow() {
        if (parent!=null) {
            if (parent.getErrorHandlerContext()!=null) {
                return true;
            }
            return parent.isInErrorHandlerSubWorkflow();
        }
        return false;
    }

    public void persist() {
        if (isInErrorHandlerSubWorkflow()) {
            // currently don't persist error handler sub-workflows
            return;
        }
        getPersister().checkpoint(this);
    }

    /** Get the value of the input. Supports Brooklyn DSL resolution but NOT Freemarker resolution. */
    public Object getInput(String key) {
        return getInputMaybe(key, TypeToken.of(Object.class), Maybe.ofAllowingNull(null)).get();
    }
    public <T> Maybe<T> getInputMaybe(String key, TypeToken<T> type, Maybe<T> valueIfUndefined) {
        if (!input.containsKey(key)) return valueIfUndefined;

        Object v = input.get(key);
        // DSL resolution/coercion only, not workflow syntax here (as no workflow scope)
        Maybe<T> vm = Tasks.resolving(v).as(type).context(getEntity()).immediately(true).deep().getMaybe();
        if (vm.isPresent()) {
            if (WorkflowStepInstanceExecutionContext.REMEMBER_RESOLVED_INPUT) {
                // this will keep spending time resolving, but will resolve the resolved value
                input.put(key, vm.get());
            }
        }
        return vm;
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

    public WorkflowStepInstanceExecutionContext getErrorHandlerContext() {
        return errorHandlerContext;
    }

    @JsonIgnore
    public String getExpiryKey() {
        if (Strings.isNonBlank(expiryKey)) return expiryKey;
        if (Strings.isNonBlank(getName())) return getName();
        return "anonymous-workflow";
    }

    /** look in tasks, steps, and replays to find most recent activity */
    public long getMostRecentActivityTime() {
        AtomicLong result = new AtomicLong(-1);

        Consumer<Long> consider = l -> {
            if (l!=null && l>result.get()) result.set(l);
        };
        Consumer<Task> considerTask = task -> {
            if (task!=null) {
                consider.accept(task.getEndTimeUtc());
                consider.accept(task.getStartTimeUtc());
                consider.accept(task.getSubmitTimeUtc());
            }
        };
        considerTask.accept(getTask(false).orNull());

        Consumer<WorkflowReplayUtils.WorkflowReplayRecord> considerReplay = replay -> {
            if (replay!=null) {
                consider.accept(replay.endTimeUtc);
                consider.accept(replay.startTimeUtc);
                consider.accept(replay.submitTimeUtc);
            }
        };
        if (replayCurrent!=null) {
            considerReplay.accept(replayCurrent);
        } else if (!replays.isEmpty()) {
            considerReplay.accept(Iterables.getLast(replays));
        }

        if (currentStepInstance!=null) {
            considerTask.accept(getManagementContext().getExecutionManager().getTask(currentStepInstance.getTaskId()));
        }

        return result.get();
    }

    public List<Object> getStepsDefinition() {
        return MutableList.copyOf(stepsDefinition).asUnmodifiable();
    }

    transient Map<String,Pair<Integer,WorkflowStepDefinition>> stepsWithExplicitId;
    transient List<WorkflowStepDefinition> stepsResolved;
    @JsonIgnore
    List<WorkflowStepDefinition> getStepsResolved() {
        if (stepsResolved ==null) {
            stepsResolved = MutableList.copyOf(WorkflowStepResolution.resolveSteps(getManagementContext(), WorkflowExecutionContext.this.stepsDefinition));
        }
        return stepsResolved;
    }

    @JsonIgnore
    public Map<String, Pair<Integer,WorkflowStepDefinition>> getStepsWithExplicitIdById() {
        if (stepsWithExplicitId ==null) stepsWithExplicitId = computeStepsWithExplicitIdById(getStepsResolved());
        return stepsWithExplicitId;
    }


    protected class Body implements Callable<Object> {
        private WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions;

        public Body() {
        }

        public Body(WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions) {
            this.continuationInstructions = continuationInstructions;
        }

        @Override
        public Object call() throws Exception {
            boolean continueOnErrorHandledOrNextReplay;

            Task<?> timerTask = null;
            AtomicReference<Boolean> timerCancelled = new AtomicReference<>(false);
            try {
                if (timeout != null) {
                    Task<?> otherTask = Tasks.current();
                    timerTask = Entities.submit(getEntity(), Tasks.builder().displayName("Timer for " + WorkflowExecutionContext.this.toString() + ":" + taskId)
                            .body(() -> {
                                try {
                                    Time.sleep(timeout);
                                    if (!otherTask.isDone()) {
                                        timerCancelled.set(true);
                                        log.debug("Cancelling " + otherTask + " after timeout of " + timeout);
                                        otherTask.cancel(true);
                                    }
                                } catch (Throwable e) {
                                    if (Exceptions.isRootCauseIsInterruption(e)) {
                                        // normal, just exit
                                    } else {
                                        throw Exceptions.propagate(e);
                                    }
                                }
                            }).dynamic(false)
                            .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                            .tag(BrooklynTaskTags.INESSENTIAL_TASK)
                            .build());
                }

                RecoveryAndReplay: do {
                    continueOnErrorHandledOrNextReplay = false;

                    try {
                        boolean replaying = continuationInstructions!=null;
                        Integer replayFromStep = replaying ? continuationInstructions.stepToReplayFrom : null;
                        WorkflowReplayUtils.updateOnWorkflowTaskStartupOrReplay(WorkflowExecutionContext.this, task, getStepsResolved(), !replaying, replayFromStep);

                        // show task running
                        status = WorkflowStatus.RUNNING;

                        if (replaying) {
                            if (replayFromStep != null && replayFromStep == STEP_INDEX_FOR_START) {
                                log.debug("Replaying workflow '" + name + "', from start " +
                                        " (was at " + (currentStepIndex == null ? "<UNSTARTED>" : workflowStepReference(currentStepIndex)) + ")");
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
                                throw new IllegalStateException("Should either be replaying or unstarted, but not invoked as replaying, and current=" + currentStepIndex + " replay=" + replayFromStep);
                            }
                        }

                        if (!Objects.equals(taskId, Tasks.current().getId()))
                            throw new IllegalStateException("Running workflow in unexpected task, " + taskId + " does not match " + task);

                        while (currentStepIndex < getStepsResolved().size()) {
                            if (replaying && replayFromStep == null) {
                                // clear output before re-running
                                currentStepInstance.output = null;
                                currentStepInstance.injectContext(WorkflowExecutionContext.this);
                                log.debug("Replaying workflow '" + name + "', reusing instance " + currentStepInstance + " for step " + workflowStepReference(currentStepIndex) + ")");
                                runCurrentStepInstanceApproved(getStepsResolved().get(currentStepIndex));
                            } else {
                                runCurrentStepIfPreconditions();
                            }
                            replaying = false;

                            if (continuationInstructions!=null) {
                                continueOnErrorHandledOrNextReplay = true;
                                continue RecoveryAndReplay;
                            }
                        }

                        if (outputDefinition != null) {
                            output = resolve(outputDefinition, Object.class);
                        } else {
                            // (default is the output of the last step)
                            // ((unlike steps, workflow output is not available as a default value, but previous step always is, so there is no need to do this
                            // before the above; slight chance if onError is triggered by a failure to resolve something in outputDefinition, but that can be ignored))
                            output = getPreviousStepOutput();
                        }

                        // finished -- checkpoint noting previous step and null for current because finished
                        status = WorkflowStatus.SUCCESS;
                        // record how it ended
                        oldStepInfo.compute(previousStepIndex == null ? STEP_INDEX_FOR_START : previousStepIndex, (index, old) -> {
                            if (old == null) old = new OldStepRecord();
                            old.next = MutableSet.<Integer>of(STEP_INDEX_FOR_START).putAll(old.next);
                            old.nextTaskId = null;
                            return old;
                        });
                        oldStepInfo.compute(STEP_INDEX_FOR_START, (index, old) -> {
                            if (old == null) old = new OldStepRecord();
                            old.previous = MutableSet.<Integer>of(previousStepIndex).putAll(old.previous);
                            old.previousTaskId = previousStepTaskId;
                            return old;
                        });

                    } catch (Throwable e) {
                        // do not propagateIfFatal, we need to handle most throwables

                        boolean isTimeout = false;

                        if (timerCancelled.get()) {
                            if (Exceptions.getCausalChain(e).stream().anyMatch(cause -> cause instanceof TimeoutException || cause instanceof InterruptedException || cause instanceof CancellationException || cause instanceof RuntimeInterruptedException)) {
                                // timed out, and a cause is related to cancellation

                                // NOTE: this just gets used for logging, it is not ever stored by the calling task or returned to a user;
                                // when the task is cancelled, the only information available to callers is that it was cancelled
                                // (we could add indications of why but that is a lot of work, and not clear we need it)
                                TimeoutException timeoutException = new TimeoutException("Timeout after " + timeout + ": " + getName());
                                timeoutException.initCause(e);
                                e = timeoutException;
                                isTimeout = true;
                            }
                        }

                        WorkflowStatus provisionalStatus;

                        if (Exceptions.isCausedByInterruptInAnyThread(e) || Exceptions.getFirstThrowableMatching(e,
                                // could do with a more precise check that workflow is cancelled or has timed out; exceptions could come from other unrelated causes (but unlikely)
                                t -> t instanceof CancellationException || t instanceof TimeoutException) != null) {
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
                        } else {
                            provisionalStatus = WorkflowStatus.ERROR;
                        }

                        boolean errorHandled = false;
                        if (isTimeout) {
                            // don't run error handler
                            log.debug("Timeout in workflow '" + getName() + "' around step " + workflowStepReference(currentStepIndex) + ", throwing: " + Exceptions.collapseText(e));

                        } else if (onError != null && !onError.isEmpty() && provisionalStatus.persistable) {
                            WorkflowErrorHandling.WorkflowErrorHandlingResult result = null;
                            try {
                                log.debug("Error in workflow '" + getName() + "' around step " + workflowStepReference(currentStepIndex) + ", running error handler");
                                Task<WorkflowErrorHandling.WorkflowErrorHandlingResult> workflowErrorHandlerTask = WorkflowErrorHandling.createWorkflowErrorHandlerTask(WorkflowExecutionContext.this, task, e);
                                errorHandlerTaskId = workflowErrorHandlerTask.getId();
                                result = DynamicTasks.queue(workflowErrorHandlerTask).getUnchecked();
                                if (result != null) {
                                    errorHandled = true;

                                    if (result.output != null) output = resolve(result.output, Object.class);

                                    String next = Strings.isNonBlank(result.next) ? result.next : "end";
                                    moveToNextStep(next, "Handled error in workflow around step " + workflowStepReference(currentStepIndex));

                                    if (continuationInstructions!=null || currentStepIndex < getStepsResolved().size()) {
                                        continueOnErrorHandledOrNextReplay = true;
                                        continue RecoveryAndReplay;
                                    }
                                }

                            } catch (Exception e2) {
                                Throwable e0 = e;
                                if (Exceptions.getCausalChain(e2).stream().anyMatch(e3 -> e3==e0)) {
                                    // wraps/rethrows original, don't need to log
                                } else {
                                    log.warn("Error in workflow '" + getName() + "' around step " + workflowStepReference(currentStepIndex) + " error handler for -- " + Exceptions.collapseText(e) + " -- threw another error (rethrowing): " + Exceptions.collapseText(e2));
                                    log.debug("Full trace of original error was: " + e, e);
                                    e = e2;
                                }
                            }

                        } else {
                            log.debug("Error in workflow '" + getName() + "' around step " + workflowStepReference(currentStepIndex) + ", no error handler so rethrowing: " + Exceptions.collapseText(e));
                        }

                        if (!errorHandled) {
                            status = provisionalStatus;
                            WorkflowReplayUtils.updateOnWorkflowError(WorkflowExecutionContext.this, task, e);

                            try {
                                if (status.persistable) {
                                    log.debug("Error running workflow " + this + "; will persist then rethrow: " + e);
                                    log.trace("Error running workflow " + this + "; will persist then rethrow (details): " + e, e);
                                    persist();
                                } else {
                                    log.debug("Error running workflow " + this + ", unsurprising because " + status);
                                }

                            } catch (Throwable e2) {
                                log.error("Error persisting workflow " + this + " after error in workflow; persistence error: " + e2);
                                log.debug("Error persisting workflow " + this + " after error in workflow; persistence error (details): " + e2, e2);
                                log.warn("Error running workflow " + this + ", rethrowing without persisting because of persistence error (above): " + e);
                                log.trace("Error running workflow " + this + ", rethrowing without persisting because of persistence error (above): " + e, e);
                            }

                            throw Exceptions.propagate(e);
                        }
                    }

                } while (continueOnErrorHandledOrNextReplay);

            } finally {
                if (timerTask != null && !timerTask.isDone() && !timerCancelled.get()) {
                    log.debug("Cancelling " + timerTask + " on completion of this task");
                    timerTask.cancel(true);
                }
            }

            WorkflowReplayUtils.updateOnWorkflowSuccess(WorkflowExecutionContext.this, task, output);
            persist();
            return output;
        }

        private WorkflowStepDefinition.ReplayContinuationInstructions getSpecialNextReplay() {
            if (errorHandlerContext!=null && errorHandlerContext.nextReplay!=null) return errorHandlerContext.nextReplay;
            if (currentStepInstance!=null && currentStepInstance.nextReplay!=null) return currentStepInstance.nextReplay;
            return null;
        }

        protected void runCurrentStepIfPreconditions() {
            WorkflowStepDefinition step = getStepsResolved().get(currentStepIndex);
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
            if (continuationInstructions!=null) {
                t = step.newTaskContinuing(currentStepInstance, continuationInstructions);
                continuationInstructions = null;
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
                old.previous = MutableSet.<Integer>of(previousStepIndex == null ? STEP_INDEX_FOR_START : previousStepIndex).putAll(old.previous);
                old.previousTaskId = previousStepTaskId;
                old.nextTaskId = null;
                return old;
            });
            oldStepInfo.compute(previousStepIndex==null ? STEP_INDEX_FOR_START : previousStepIndex, (index, old) -> {
                if (old==null) old = new OldStepRecord();
                old.next = MutableSet.<Integer>of(currentStepIndex).putAll(old.next);
                old.nextTaskId = t.getId();
                return old;
            });

            // and update replayable info
            WorkflowReplayUtils.updateOnWorkflowStepChange(currentStepRecord, currentStepInstance, step);
            errorHandlerContext = null;
            errorHandlerTaskId = null;

            persist();

            AtomicReference<String> customNext = new AtomicReference<>();
            Runnable next = () -> moveToNextStep(Strings.isNonBlank(customNext.get()) ? customNext.get() : step.getNext(), "Completed step "+ workflowStepReference(currentStepIndex));
            Consumer<Object> saveOutput = output -> {
                if (output!=null) currentStepInstance.output = resolve(output, Object.class);
            };

            try {
                Duration duration = step.getTimeout();
                if (duration!=null) {
                    boolean isEnded = DynamicTasks.queue(t).blockUntilEnded(duration);
                    if (isEnded) {
                        currentStepInstance.output = t.getUnchecked();
                    } else {
                        t.cancel(true);
                        throw new TimeoutException("Timeout after "+duration+": "+t.getDisplayName());
                    }
                } else {
                    currentStepInstance.output = DynamicTasks.queue(t).getUnchecked();
                }

                // allow output to be customized / overridden
                saveOutput.accept(step.output);

            } catch (Exception e) {
                if (!step.onError.isEmpty()) {
                    WorkflowErrorHandling.WorkflowErrorHandlingResult result;
                    try {
                        Task<WorkflowErrorHandling.WorkflowErrorHandlingResult> stepErrorHandlerTask = WorkflowErrorHandling.createStepErrorHandlerTask(step, currentStepInstance, t, e);
                        currentStepInstance.errorHandlerTaskId = stepErrorHandlerTask.getId();
                        result = DynamicTasks.queue(stepErrorHandlerTask).getUnchecked();
                        if (result!=null) {
                            if (Strings.isNonBlank(result.next)) customNext.set(result.next);
                            saveOutput.accept(result.output);
                        }

                    } catch (Exception e2) {
                        if (Exceptions.getCausalChain(e2).stream().anyMatch(e3 -> e3==e)) {
                            // is, or wraps, original error, don't need to log
                        } else {
                            log.warn("Error in step '" + t.getDisplayName() + "' error handler for -- " + Exceptions.collapseText(e) + " -- threw another error (rethrowing): " + Exceptions.collapseText(e2));
                            log.debug("Full trace of original error was: " + e, e);
                        }
                        throw Exceptions.propagate(e2);
                    }
                    if (result==null) {
                        log.warn("Error in step '" + t.getDisplayName() + "', error handler did not apply so rethrowing: " + Exceptions.collapseText(e));
                    }

                } else {
                    log.warn("Error in step '" + t.getDisplayName() + "', no error handler so rethrowing: " + Exceptions.collapseText(e));
                    throw Exceptions.propagate(e);
                }
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
            continuationInstructions = getSpecialNextReplay();
            if (continuationInstructions!=null) {
                log.debug(prefix + "proceeding to custom replay: "+currentStepInstance.nextReplay);
                return;
            }

            prefix = prefix + "; ";
            if (optionalRequestedNextStep==null) {
                currentStepIndex++;
                if (currentStepIndex < getStepsResolved().size()) {
                    log.debug(prefix + "moving to sequential next step " + workflowStepReference(currentStepIndex));
                } else {
                    log.debug(prefix + "no further steps: Workflow completed");
                }
            } else {
                Maybe<Pair<Integer, Boolean>> nextResolved = getIndexOfStepId(optionalRequestedNextStep);
                if (nextResolved.isAbsent()) {
                    log.warn(prefix + "explicit next step '"+optionalRequestedNextStep+"' not found (failing)");
                    nextResolved.get();
                }

                currentStepIndex = nextResolved.get().getLeft();
                if (nextResolved.get().getRight()) {
                    if (currentStepIndex < getStepsResolved().size()) {
                        log.debug(prefix + "moving to explicit next step " + workflowStepReference(currentStepIndex) + " for token '" + optionalRequestedNextStep + "'");
                    } else {
                        log.debug(prefix + "explicit next '"+optionalRequestedNextStep+"': Workflow completed");
                    }
                } else {
                    log.debug(prefix + "moving to explicit next step " + workflowStepReference(currentStepIndex) + " for id '" + optionalRequestedNextStep + "'");
                }
            }
        }

        String workflowStepReference(Integer index) {
            if (index==null) return workflowId+"-<no-step>";

            if (index>=getStepsResolved().size()) return getWorkflowStepReference(index, "<END>", false);
            return getWorkflowStepReference(index, getStepsResolved().get(index));
        }
    }

    public Maybe<Pair<Integer,Boolean>> getIndexOfStepId(String next) {
        if (next==null) return Maybe.absent("Null step ID supplied");
        Function<WorkflowExecutionContext, Integer> predefined = PREDEFINED_NEXT_TARGETS.get(next.toLowerCase());
        if (predefined!=null) return Maybe.of(Pair.of(predefined.apply(this), true));
        Pair<Integer, WorkflowStepDefinition> explicit = getStepsWithExplicitIdById().get(next);
        if (explicit!=null) return Maybe.of(Pair.of(explicit.getLeft(), false));
        return Maybe.absent(new NoSuchElementException("Step with ID '"+next+"' not found"));
    }

    String getWorkflowStepReference(int index, WorkflowStepDefinition step) {
        return getWorkflowStepReference(index, step!=null ? step.id : null, false);
    }
    String getWorkflowStepReference(int index, String optionalStepId, boolean isError) {
        // error handler step number not always available here,
        // and risk of ID ambiguity with error-handler phrase;
        // only for logging so this is okay;
        // for canonical usage, prefer the Task-based method below
        return workflowId+"-"+(index>=0 ? index+1 : indexCode(index))
                +(Strings.isNonBlank(optionalStepId) ? "-"+optionalStepId : "")
                +(isError ? "-error-handler" : "");
    }
    public String getWorkflowStepReference(Task<?> t) {
        BrooklynTaskTags.WorkflowTaskTag wt = BrooklynTaskTags.getWorkflowTaskTag(t, false);
        return wt.getWorkflowId()
                +(wt.getStepIndex()!=null && wt.getStepIndex()>=0 && wt.getStepIndex()<stepsDefinition.size() ? "-"+(wt.getStepIndex()+1) : "")
                +(wt.getErrorHandlerIndex()!=null ? "-error-handler-"+(wt.getErrorHandlerIndex()+1) : "")
                ;
    }

    private String indexCode(int index) {
        // these numbers shouldn't be used for much, but they are used in a few places :(
        if (index==STEP_INDEX_FOR_START) return "start";
        if (index==STEP_INDEX_FOR_END) return "end";
        if (index==STEP_INDEX_FOR_ERROR_HANDLER) return "error-handler";
        return "neg-"+(index); // unknown
    }

}
