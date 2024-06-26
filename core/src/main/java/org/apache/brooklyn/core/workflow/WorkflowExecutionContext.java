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

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext.SubworkflowLocality;
import org.apache.brooklyn.core.workflow.store.WorkflowRetentionAndExpiration;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.core.workflow.utils.WorkflowRetentionParser;
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
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Threads;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.brooklyn.core.workflow.WorkflowReplayUtils.ReplayResumeDepthCheck.RESUMABLE_WHENEVER_NESTED_WORKFLOWS_PRESENT;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(converter = WorkflowExecutionContext.Converter.class)
public class WorkflowExecutionContext {
    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutionContext.class);

    public static final String LABEL_FOR_ERROR_HANDLER = "error-handler";

    public static final int STEP_INDEX_FOR_START = -1;
    public static final int STEP_INDEX_FOR_END = -2;
    public static final int STEP_INDEX_FOR_ERROR_HANDLER = -3;

    public static final String STEP_TARGET_NAME_FOR_START = "start";
    public static final String STEP_TARGET_NAME_FOR_END = "end";
    public static final String STEP_TARGET_NAME_FOR_LAST = "last";
    public static final String STEP_TARGET_NAME_FOR_HERE = "here";
    public static final String STEP_TARGET_NAME_FOR_EXIT = "exit";
    public static final String STEP_TARGET_NAME_FOR_DEFAULT = "default";

    public static final Map<String, Function<WorkflowExecutionContext,Integer>> PREDEFINED_NEXT_TARGETS = MutableMap.<String, Function<WorkflowExecutionContext,Integer>>of(
            STEP_TARGET_NAME_FOR_START, c -> c==null? STEP_INDEX_FOR_START : 0,
            STEP_TARGET_NAME_FOR_END, c -> c==null ? STEP_INDEX_FOR_END : c.stepsDefinition.size(),
            STEP_TARGET_NAME_FOR_LAST, c -> c==null ? null : c.replayableLastStep,
            STEP_TARGET_NAME_FOR_HERE, c -> c==null ? null : null,
            STEP_TARGET_NAME_FOR_EXIT, c -> c==null ? null : null,
            STEP_TARGET_NAME_FOR_DEFAULT, c -> c==null ? null : c.currentStepIndex+1).asUnmodifiable();

    String name;
    @Nullable BrooklynObject adjunct;
    Entity entity;

    public enum WorkflowStatus {
        STAGED(false, false, false, false),
        RUNNING(true, false, false, false),
        SUCCESS(true, true, false, true),
        /** useful information, usually cannot persisted by the time we've set this the first time, but could set on rebind */ ERROR_SHUTDOWN(true, true, true, false),
        /** task failed because entity destroyed */ ERROR_ENTITY_DESTROYED(true, true, true, true),
        /** task cancelled, timeout, or other interrupt, usually recursively (but not entity destroyed or server shutdown) */ ERROR_CANCELLED(true, true, true, true),
        /** any other error, e.g. workflow step failed or data not immediately available (the interrupt used internally is not relevant) */ ERROR(true, true, true, true);

        public final boolean started;
        public final boolean ended;
        public final boolean error;
        public final boolean expirable;

        WorkflowStatus(boolean started, boolean ended, boolean error, boolean expirable) { this.started = started; this.ended = ended; this.error = error; this.expirable = expirable; }
    }

    WorkflowStatus status;
    Instant lastStatusChangeTime;

    @JsonIgnore private transient WorkflowExecutionContext parent;
    private BrooklynTaskTags.WorkflowTaskTag parentTag;

    // should be treated as raw json
    @JsonDeserialize(contentUsing = JsonPassThroughDeserializer.class)
    List<Object> stepsDefinition;

    Object condition;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<String,Object> input = MutableMap.of();
    @JsonIgnore  // persist as sensor but not via REST in case it has secrets resolved
    Map<String,Object> inputResolved = MutableMap.of();

    public boolean hasInput(ConfigKey<?> key) {
        return hasInput(key.getName());
    }
    public boolean hasInput(String key) {
        return input.containsKey(key);
    }
    @JsonIgnore
    public Map<String, Object> getAllInput() {
        return input;
    }
    @JsonIgnore
    public Map<String, Object> getAllInputResolved() {
        return inputResolved;
    }
    public void noteInputResolved(String k, Object v) {
        inputResolved.put(k, v);
    }

    Object outputDefinition;
    /** final output of the workflow, set at end */
    Object output;

    /*
     * Tricks to keep size of persisted/serialized data down:
     *
     * * this.output set at the end (as a copy)
     *   NEW: only set if different, on lookup if null, if finished, get from last step
     * * step.output set on completion of each step (copying previous if no explicit output)
//     *   - if never run
//     *     - if different to previous step, set
//     *     - if same as previous step, set null
//     *   - if run before
//     *     - if old value equals new value, do nothing
//     *     - if old value different
     *       - if new value is same as previous step, set null, else set new value
     *       - & if last run's next step output was null, set it to the old value here
     *   NEW: only set if different to previous step or not recoverable
     *   - if different to previous step, set
     *   - if there was a previous instance of this step: if we are different to previous instance of this step, set
     *   - if same. has this step
     *
     * * this.workflowScratch - currently set on context dynamically, copied to oldStepInfo
     *   NEW 1: set on context and oldStepInfo dynamically but null if same as previous, retrieved looking up previous
     *   NEW 2: add up all the previous until it repeats
     */

    Object lock;

    Duration timeout;
    Object onError;

    String workflowId;
    /** current or most recent executing task created for this workflow, corresponding to task */
    String taskId;
    transient Task<Object> task;

    @JsonProperty("retention")
    WorkflowRetentionAndExpiration.WorkflowRetentionSettings retention;

    /** all tasks created for this workflow */
    Set<WorkflowReplayUtils.WorkflowReplayRecord> replays = MutableSet.of();
    transient WorkflowReplayUtils.WorkflowReplayRecord replayCurrent = null;
    /** null if no replay point, otherwise step number of the last completed replay point, or -1 if should replay from start and -2 if completed successfully */
    Integer replayableLastStep;
    Boolean replayableFromStart;
    Boolean replayableAutomatically;
    Boolean replayableDisabled;
    Boolean idempotentAll;

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
        /** count of runs completed (without error) */
        int countCompleted = 0;

        /** context for last _completed_ instance of step */
        WorkflowStepInstanceExecutionContext context;
        /** is step replayable */
        Boolean replayableFromHere;
        /** scratch vars as at start of last invocation of set _if_ they could not be derived from updates */
        Map<String,Object> workflowScratch;
        /** updates to scratch vars made by the last run of this step */
        Map<String,Object> workflowScratchUpdates;
        /** steps that immediately preceded this, updated when _this_ step started, with most recent first */
        Set<Integer> previous;
        /** steps that immediately followed this, updated when _next_ step started, with most recent first */
        Set<Integer> next;

        String previousTaskId;
        String nextTaskId;
    }

    // when persisted, this is omitted and restored from the oldStepInfo map on read
    transient Map<String,Object> workflowScratchVariables;
    transient Map<String,Object> workflowScratchVariablesUpdatedThisStep;

    @JsonSetter("workflowScratchVariables") //only used to read in old state which stored this
    public void setWorkflowScratchVariablesToDeserializeOld(Map<String, Object> workflowScratchVariables) {
        this.workflowScratchVariables = workflowScratchVariables;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<String,List<Instant>> retryRecords = MutableMap.of();

    // deserialization constructor
    private WorkflowExecutionContext() {}

    public static WorkflowExecutionContext newInstancePersisted(BrooklynObject entityOrAdjunctWhereRunning, WorkflowContextType wcType, String name, ConfigBag paramsDefiningWorkflow,
                                                                Collection<ConfigKey<?>> extraConfigKeys, ConfigBag extraInputs, Map<String, Object> optionalTaskFlags) {
        WorkflowExecutionContext w = newInstanceUnpersistedWithParent(entityOrAdjunctWhereRunning, null, wcType, name, paramsDefiningWorkflow, extraConfigKeys, extraInputs, optionalTaskFlags);
        w.persist();
        return w;
    }

    public enum WorkflowContextType {
        SENSOR, EFFECTOR, POLICY, NESTED_WORKFLOW, OTHER
    }

    public static WorkflowExecutionContext newInstanceUnpersistedWithParent(BrooklynObject entityOrAdjunctWhereRunning, WorkflowExecutionContext parent,
                                              WorkflowContextType wcType, String name, ConfigBag paramsDefiningWorkflow,
                                              Collection<ConfigKey<?>> extraConfigKeys, ConfigBag extraInputs, Map<String, Object> optionalTaskFlags) {
        return newInstanceUnpersistedWithParent(entityOrAdjunctWhereRunning, parent, wcType, name, paramsDefiningWorkflow, extraConfigKeys, extraInputs, optionalTaskFlags, null);
    }

    public static WorkflowExecutionContext newInstanceUnpersistedWithParent(BrooklynObject entityOrAdjunctWhereRunning, WorkflowExecutionContext parent,
                                            WorkflowContextType wcType, String name, ConfigBag paramsDefiningWorkflow,
                                            Collection<ConfigKey<?>> extraConfigKeys, ConfigBag extraInputs, Map<String, Object> optionalTaskFlags, String optionalTaskName) {

        // parameter defs
        Map<String,ConfigKey<?>> parameters = MutableMap.of();
        Maybe<BrooklynClassLoadingContext> loader = RegisteredTypes.getClassLoadingContextMaybe(entityOrAdjunctWhereRunning);
        Effectors.parseParameters(paramsDefiningWorkflow.get(WorkflowCommonConfig.PARAMETER_DEFS), loader.orNull()).forEach(p -> parameters.put(p.getName(), Effectors.asConfigKey(p)));
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
                WorkflowReplayUtils.updaterForReplayableAtWorkflow(paramsDefiningWorkflow, wcType == WorkflowContextType.NESTED_WORKFLOW),
                optionalTaskFlags, optionalTaskName);

        w.getStepsResolved();  // ensure steps resolve at this point; should be true even if condition doesn't apply (though input might not be valid without condition)

        w.retention = WorkflowRetentionParser.parse(paramsDefiningWorkflow.get(WorkflowCommonConfig.RETENTION), w).init(w);
        w.lock = paramsDefiningWorkflow.get(WorkflowCommonConfig.LOCK);
        w.timeout = paramsDefiningWorkflow.get(WorkflowCommonConfig.TIMEOUT);
        w.onError = paramsDefiningWorkflow.get(WorkflowCommonConfig.ON_ERROR);
        // fail fast if error steps not resolveable
        new WorkflowStepResolution(w).resolveSubSteps("error handling", WorkflowErrorHandling.wrappedInListIfNecessaryOrNullIfEmpty(w.onError));

        // some fields need to be resolved at setting time, in the context of the workflow
        w.setCondition(paramsDefiningWorkflow.getStringKey(WorkflowCommonConfig.CONDITION.getName()));

        // finished -- checkpoint noting this has been created but not yet started
        w.updateStatus(WorkflowStatus.STAGED);
        return w;
    }

    protected WorkflowExecutionContext(BrooklynObject entityOrAdjunctWhereRunning, WorkflowExecutionContext parent, String name,
                                       List<Object> stepsDefinition, Map<String,Object> input, Object output,
                                       Consumer<WorkflowExecutionContext> replayableInitializer, Map<String, Object> optionalTaskFlags) {
        this(entityOrAdjunctWhereRunning, parent, name, stepsDefinition, input, output, replayableInitializer, optionalTaskFlags, null);
    }
    protected WorkflowExecutionContext(BrooklynObject entityOrAdjunctWhereRunning, WorkflowExecutionContext parent, String name,
                                       List<Object> stepsDefinition, Map<String,Object> input, Object output,
                                       Consumer<WorkflowExecutionContext> replayableInitializer, Map<String, Object> optionalTaskFlags, String optionalTaskName) {
        initParent(parent);
        this.name = name;
        this.adjunct = entityOrAdjunctWhereRunning instanceof Entity ? null : entityOrAdjunctWhereRunning;
        this.entity = entityOrAdjunctWhereRunning instanceof Entity ? (Entity)entityOrAdjunctWhereRunning : ((EntityAdjuncts.EntityAdjunctProxyable)entityOrAdjunctWhereRunning).getEntity();
        this.stepsDefinition = stepsDefinition;

        this.input = input;
        this.outputDefinition = output;
        if (replayableInitializer!=null) replayableInitializer.accept(this);

        TaskBuilder<Object> tb = Tasks.builder().displayName(optionalTaskName).dynamic(true);
        if (optionalTaskFlags!=null) tb.flags(optionalTaskFlags);
        if (Strings.isBlank(tb.getDisplayName())) tb.displayName(name);
        task = tb.body(new Body()).build();
        WorkflowReplayUtils.updateOnWorkflowStartOrReplay(this, task, "initial run", null);
        workflowId = taskId = task.getId();
        TaskTags.addTagDynamically(task, BrooklynTaskTags.WORKFLOW_TAG);
        TaskTags.addTagDynamically(task, BrooklynTaskTags.tagForWorkflow(this));

        // currently workflow ID is the same as the task ID assigned initially. (but if replayed they will be different.)
        // there is no deep reason or need for this, it is just convenient, and used for tests.
        //this.workflowId = Identifiers.makeRandomId(8);
    }

    public void initParent(WorkflowExecutionContext parent) {
        this.parent = parent;
        this.parentTag = parent==null ? null : BrooklynTaskTags.tagForWorkflow(parent);
    }

    @JsonIgnore
    public WorkflowExecutionContext getParent() {
        if (parent==null && parentTag!=null) {
            Entity entity = getManagementContext().getEntityManager().getEntity(parentTag.getEntityId());
            if (entity==null) {
                log.warn("Parent workflow "+parentTag+" for "+this+" is on an entity no longer known; unparenting this workflow");
                parentTag = null;
            } else {
                parent = new WorkflowStatePersistenceViaSensors(getManagementContext()).getWorkflows(entity).get(parentTag.getWorkflowId());
                if (parent==null) {
                    log.warn("Parent workflow "+parentTag+" for "+this+" is no longer known; unparenting this workflow");
                    parentTag = null;
                }
            }
        }
        return parent;
    }

    public static void validateSteps(WorkflowStepResolution workflowStepResolution, List<WorkflowStepDefinition> steps, boolean alreadyValidatedIndividualSteps) {
        if (!alreadyValidatedIndividualSteps) {
            steps.forEach(w -> w.validateStep(workflowStepResolution));
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

    public void setCondition(Object condition) {
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

    public BrooklynTaskTags.WorkflowTaskTag getParentTag() {
        return parentTag;
    }

    @JsonIgnore
    public Map<String, Object> getWorkflowScratchVariables() {
        if (workflowScratchVariables == null) {
            Pair<Map<String, Object>, Set<Integer>> prev = getStepWorkflowScratchAndBacktrackedSteps(null);
            workflowScratchVariables = prev.getLeft();
        }
        return MutableMap.copyOf(workflowScratchVariables).asUnmodifiable();
    }

    public Object clearWorkflowScratchVariable(String s) {
        if (workflowScratchVariables == null) getWorkflowScratchVariables();
        Object old = workflowScratchVariables.remove(s);
        if (workflowScratchVariablesUpdatedThisStep==null) workflowScratchVariablesUpdatedThisStep = MutableMap.of();
        workflowScratchVariablesUpdatedThisStep.put(s, Entities.REMOVE);
        return old;
    }

    public Object updateWorkflowScratchVariable(String s, Object v) {
        if (workflowScratchVariables == null) getWorkflowScratchVariables();
        Object old = workflowScratchVariables.put(s, v);
        if (Entities.REMOVE.equals(v)) workflowScratchVariables.remove(s);
        if (workflowScratchVariablesUpdatedThisStep==null) workflowScratchVariablesUpdatedThisStep = MutableMap.of();
        workflowScratchVariablesUpdatedThisStep.put(s, v);
        return old;
    }

    public void updateWorkflowScratchVariables(Map<String,Object> newValues) {
        if (newValues!=null && !newValues.isEmpty()) {
            if (workflowScratchVariables == null) getWorkflowScratchVariables();
            workflowScratchVariables.putAll(newValues);
            newValues.forEach((k,v)->{ if (Entities.REMOVE.equals(v)) workflowScratchVariables.remove(k); });
            if (workflowScratchVariablesUpdatedThisStep==null) workflowScratchVariablesUpdatedThisStep = MutableMap.of();
            workflowScratchVariablesUpdatedThisStep.putAll(newValues);
        }
    }

    public Map<String, List<Instant>> getRetryRecords() {
        return retryRecords;
    }

    public Maybe<Task<Object>> getTask(boolean checkCondition) {
        if (checkCondition) return getTaskCheckingConditionWithTarget(getEntityOrAdjunctWhereRunning());
        else return getTaskSkippingCondition();
    }
    DslPredicates.DslPredicate resolveCondition(Object condition) {
        if (condition==null) return null;
        // condition is resolved wrapped for two reasons:
        // - target cannot be a fully resolved string unless it is something like 'children', and that constant should be
        //   different to a var ${x} (even if x evaluates to children)
        // - some tests allow things to throw errors and check for error, so e.g. an expression that doesn't resolve isn't necessarily a problem
        return resolveWrapped(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, condition, TypeToken.of(DslPredicates.DslPredicate.class),
                WorkflowExpressionResolution.WrappingMode.WRAPPED_RESULT_DEFER_THROWING_ERROR_BUT_NO_RETRY);
    }
    public Maybe<Task<Object>> getTaskCheckingConditionWithTarget(Object conditionTarget) {
        DslPredicates.DslPredicate conditionResolved = resolveCondition(condition);
        if (conditionResolved != null) {
            if (!conditionResolved.apply(conditionTarget))
                return Maybe.absent(new IllegalStateException("This workflow cannot be run at present: condition not satisfied"));
        }
        return getTaskSkippingCondition();
    }
    @JsonIgnore
    public Maybe<Task<Object>> getTaskSkippingCondition() {
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

    public Factory factory(boolean allowInternallyEvenIfDisabled) {
        return new Factory(allowInternallyEvenIfDisabled);
    }

    public class Factory {
        private final boolean allowInternallyEvenIfDisabled;

        protected Factory(boolean allowInternallyEvenIfDisabled) {
            this.allowInternallyEvenIfDisabled = allowInternallyEvenIfDisabled;
        }

        public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayingFromStep(int stepIndex0, String reason, boolean forced) {
            if (!forced) checkNotDisabled();
            int stepIndex = stepIndex0;
            if (!forced) {
                stepIndex = Maybe.ofDisallowingNull(WorkflowReplayUtils.findNearestReplayPoint(WorkflowExecutionContext.this, stepIndex0))
                        .orThrow(() -> new IllegalStateException("Workflow is not replayable: no replay points found backtracking from " + stepIndex0));
                log.debug("Request to replay from step " + stepIndex0 + ", nearest replay point is " + stepIndex);
            }
            return new WorkflowStepDefinition.ReplayContinuationInstructions(stepIndex, reason, null, forced);
        }

        public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayingFromLastReplayable(String reason, boolean forced) {
            return makeInstructionsForReplayingFromStep(replayableLastStep != null ? replayableLastStep : STEP_INDEX_FOR_START, reason, forced);
        }

        public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayingFromStart(String reason, boolean forced) {
            return makeInstructionsForReplayingFromStep(STEP_INDEX_FOR_START, reason, forced);
        }

        public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayResuming(String reason, boolean forced) {
            return makeInstructionsForReplayResuming(reason, forced, null);
        }

        public WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayResumingForcedWithCustom(String reason, Runnable code) {
            return makeInstructionsForReplayResuming(reason, true, code);
        }

        protected WorkflowStepDefinition.ReplayContinuationInstructions makeInstructionsForReplayResuming(String reason, boolean forced, Runnable code) {
            if (!forced) checkNotDisabled();
            Integer replayFromStep = null;
            if (currentStepIndex == null) {
                // not yet started
                replayFromStep = STEP_INDEX_FOR_START;
            } else if (currentStepInstance == null || currentStepInstance.stepIndex != currentStepIndex) {
                // replaying from a different step, or current step which has either not run or completed but didn't save
                log.debug("Replaying workflow '" + name + "', cannot replay within step " + currentStepIndex + " because step instance not known; will reinitialize then replay that step");
                replayFromStep = currentStepIndex;
            }

            if (!forced && replayFromStep == null) {
                // instructions should be made even if subworkflows might reject them; that's the intention of "resume" without force, vs replay from last
                if (!WorkflowReplayUtils.isReplayResumable(WorkflowExecutionContext.this, RESUMABLE_WHENEVER_NESTED_WORKFLOWS_PRESENT, allowInternallyEvenIfDisabled)) {
                    if (code != null) {
                        // we could allow this, but we don't need it
                        throw new IllegalArgumentException("Cannot supply code to here without forcing as workflow does not support replay resuming at this point");
                    }
                    log.debug("Request to replay resuming " + WorkflowExecutionContext.this + " at non-idempotent step; rolling back to " + replayableLastStep);
                    if (replayableLastStep == null) {
                        throw new IllegalArgumentException("Cannot replay resuming as there are no replay points and last step " + currentStepIndex + " is not idempotent; " +
                                "should that step or a previous one declare 'idempotent: true' or 'replayable: from here' ?");
                    }
                    return makeInstructionsForReplayingFromStep(replayableLastStep, reason, false);
                }
            }

            return new WorkflowStepDefinition.ReplayContinuationInstructions(replayFromStep, reason, code, forced);
        }

        public Task<Object> createTaskReplaying(WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions) {
            return createTaskReplaying(null, continuationInstructions);
        }

        public Task<Object> createTaskReplaying(Runnable intro, WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions) {
            if (continuationInstructions==null || !continuationInstructions.forced) checkNotDisabled();
            if (task != null && !task.isDone()) {
                if (!task.isSubmitted()) {
                    if (parent!=null && parent.getReplays().size()>1) {
                        log.debug("Abandoning sub-workflow task that was never submitted, not unusual as parent seems to be replaying: " + task + " for " + WorkflowExecutionContext.this);
                    } else {
                        log.warn("Abandoning workflow task that was never submitted: " + task + " for " + WorkflowExecutionContext.this);
                    }
                } else {
                    if (isSubmitterAncestor(Tasks.current(), task)) {
                        // not sure we need this check
                        log.debug("Replaying containing workflow " + WorkflowExecutionContext.this + " in task " + task + " which is an ancestor of " + Tasks.current());
                    } else {
                        log.warn("Unable to replay workflow " + WorkflowExecutionContext.this + " from " + Tasks.current() + " because workflow task " + task + " is ongoing; will delay up to 1s then retry");
                        // there can be a slight race between tasks ending and the workflow reporting a failure and replaying;
                        // esp in tests, but also in real world, forgive such a situation by delaying the replay for a short time
                        if (!task.blockUntilEnded(Duration.ONE_SECOND)) {
                            log.warn("Unable to replay workflow " + WorkflowExecutionContext.this + " from " + Tasks.current() + " because workflow task " + task + " is ongoing (waited 1s, still ongoing; so rethrowing)");
                            throw new IllegalStateException("Cannot replay ongoing workflow, given " + continuationInstructions);
                        }
                    }
                }
            }

            String explanation = continuationInstructions.customBehaviorExplanation != null ? continuationInstructions.customBehaviorExplanation : "no explanation";
            task = Tasks.builder().dynamic(true).displayName(name + " (" + explanation + ")")
                    .tag(BrooklynTaskTags.tagForWorkflow(WorkflowExecutionContext.this))
                    .tag(BrooklynTaskTags.WORKFLOW_TAG)
                    .body(new Body(continuationInstructions).withIntro(intro)).build();
            WorkflowReplayUtils.updateOnWorkflowStartOrReplay(WorkflowExecutionContext.this, task, continuationInstructions.customBehaviorExplanation, continuationInstructions.stepToReplayFrom);

            taskId = task.getId();

            return task;
        }

        public boolean isDisabled() {
            if (allowInternallyEvenIfDisabled) return false;
            if (Boolean.TRUE.equals(replayableDisabled)) return true;
            return false;
        }

        public void checkNotDisabled() {
            if (isDisabled()) throw new IllegalStateException("Replays disabled on "+WorkflowExecutionContext.this);
        }
    }

    private boolean isSubmitterAncestor(Task current, Task<Object> possibleAncestor) {
        if (current==null) return false;
        if (current.equals(possibleAncestor)) return true;
        return isSubmitterAncestor(current.getSubmittedByTask(), possibleAncestor);
    }

    public Entity getEntity() {
        return entity;
    }

    @JsonIgnore
    public ManagementContext getManagementContext() {
        return ((EntityInternal)getEntity()).getManagementContext();
    }

    public void persist() {
        if (isInErrorHandlerSubWorkflow()) {
            // currently don't persist error handler sub-workflows
            return;
        }
        WorkflowRetentionAndExpiration.checkpoint(getManagementContext(), this);
    }

    /** Get the value of the input. Supports Brooklyn DSL resolution but NOT Freemarker resolution. */
    public Object getInput(String key) {
        return getInputMaybe(key, TypeToken.of(Object.class), Maybe.ofAllowingNull(null)).get();
    }
    public <T> Maybe<T> getInputMaybe(String key, TypeToken<T> type, Maybe<T> valueIfUndefined) {
        if (!input.containsKey(key)) return valueIfUndefined;

        if (inputResolved.containsKey(key)) return Maybe.ofAllowingNull((T)inputResolved.get(key));

        Object v = input.get(key);
        // normally do DSL resolution/coercion only, not workflow syntax here (as no workflow scope);
        // except if we are in a nested workflow, we allow resolving from the parent.
        // (alternatively we could resolve when starting the custom workflow; that might be better.)
        Maybe<T> vm = null;
        if (v instanceof String && parent!=null && parent.getCurrentStepInstance()!=null) {
            try {
                vm = Maybe.of(parent.getCurrentStepInstance().resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, (String) v, type));
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                vm = Maybe.absent(e);
            }
        }
        if (vm==null || vm.isAbsent()) {
            Maybe<T> vm2 = Tasks.resolving(v).as(type).context(getEntity()).immediately(true).deep().getMaybe();
            if (vm2.isPresent() || vm==null) vm = vm2;  // if errors in both, prefer error in first
        }
        if (vm.isPresent()) {
            if (WorkflowStepInstanceExecutionContext.REMEMBER_RESOLVED_INPUT) {
                // this will keep spending time resolving, but will resolve the resolved value
                noteInputResolved(key, vm.get());
            }
        }
        return vm;
    }

    public TypeToken<?> lookupType(String typeName, Supplier<TypeToken<?>> ifUnset) {
        if (Strings.isBlank(typeName)) return ifUnset.get();
        BrooklynClassLoadingContext loader = getEntity() != null ? RegisteredTypes.getClassLoadingContext(getEntity()) : null;
        return new BrooklynTypeNameResolution.BrooklynTypeNameResolver("workflow", loader, true, true).getTypeToken(typeName);
    }

    /** as {@link #resolve(WorkflowExpressionResolution.WorkflowExpressionStage, Object, TypeToken)} but without type coercion */
    public Object resolve(WorkflowExpressionResolution.WorkflowExpressionStage stage, String expression) {
        return resolve(stage, expression, Object.class);
    }

    /** as {@link #resolve(WorkflowExpressionResolution.WorkflowExpressionStage, Object, TypeToken)} */
    public <T> T resolve(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, Class<T> type) {
        return resolve(stage, expression, TypeToken.of(type));
    }

    /** resolution of ${interpolation} and $brooklyn:dsl and deferred suppliers, followed by type coercion.
     * if the type is a string, null is not permitted, otherwise it is. */
    public <T> T resolve(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this, stage, false, WorkflowExpressionResolution.WrappingMode.NONE).resolveWithTemplates(expression, type);
    }

    public <T> T resolveCoercingOnly(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this, stage, false, WorkflowExpressionResolution.WrappingMode.NONE).resolveCoercingOnly(expression, type);
    }

    /** as {@link #resolve(WorkflowExpressionResolution.WorkflowExpressionStage, Object, TypeToken)},
     * but returning DSL/supplier for dynamic values (so the indication of their dynamic nature is preserved, even if the value returned by it is resolved;
     * this is needed e.g. for conditions which treat dynamic expressions differently to explicit values) */
    public <T> T resolveWrapped(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, TypeToken<T> type, WorkflowExpressionResolution.WrappingMode wrappingMode) {
        return new WorkflowExpressionResolution(this, stage, false, wrappingMode).resolveWithTemplates(expression, type);
    }

    /** as {@link #resolve(WorkflowExpressionResolution.WorkflowExpressionStage, Object, TypeToken)}, but waiting on any expressions which aren't ready */
    public <T> T resolveWaiting(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, TypeToken<T> type) {
        return new WorkflowExpressionResolution(this, stage, true, WorkflowExpressionResolution.WrappingMode.NONE).resolveWithTemplates(expression, type);
    }

    /** resolution of ${interpolation} and $brooklyn:dsl and deferred suppliers, followed by type coercion */
    public <T> T resolveConfig(WorkflowExpressionResolution.WorkflowExpressionStage stage, ConfigBag config, ConfigKey<T> key) {
        Object v = config.getStringKey(key.getName());
        if (v==null) return null;
        return resolve(stage, v, key.getTypeToken());
    }

    public WorkflowStatus getStatus() {
        return status;
    }

    void updateStatus(WorkflowStatus newStatus) {
        status = newStatus;
        lastStatusChangeTime = Instant.now();
    }

    @JsonIgnore
    public Instant getLastStatusChangeTime() {
        return lastStatusChangeTime;
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

    // clear this when error handler is not running
    transient Object lastErrorHandlerOutput = null;

    @JsonIgnore
    public Object getPreviousStepOutput() {
        Pair<Object, Set<Integer>> p = getStepOutputAndBacktrackedSteps(null);
        if (p==null) return null;
        return p.getLeft();
    }
    @JsonIgnore
    Pair<Object,Set<Integer>> getStepOutputAndBacktrackedSteps(Integer stepOrNullForPrevious) {
        if (stepOrNullForPrevious==null && lastErrorHandlerOutput!=null) return Pair.of(lastErrorHandlerOutput,null);

        Integer prevSI = stepOrNullForPrevious==null ? previousStepIndex : stepOrNullForPrevious;
        Set<Integer> previousSteps = MutableSet.of();
        while (prevSI!=null && previousSteps.add(prevSI)) {
            OldStepRecord last = oldStepInfo.get(prevSI);
            if (last==null || last.context==null) break;
            if (last.context.getOutput() !=null) return Pair.of(last.context.getOutput(), previousSteps);
            if (last.previous.isEmpty()) break;
            prevSI = last.previous.iterator().next();
        }
        return null;
    }

    @JsonIgnore
    public Pair<Map<String,Object>,Set<Integer>> getStepWorkflowScratchAndBacktrackedSteps(Integer stepOrNullForPrevious) {
        Integer prevSI = stepOrNullForPrevious==null ? previousStepIndex : stepOrNullForPrevious;
        Set<Integer> previousSteps = MutableSet.of();
        Map<String,Object> result = MutableMap.of();
        boolean includeUpdates = stepOrNullForPrevious==null;  // exclude first update if getting at an explicit step
        while (prevSI!=null && previousSteps.add(prevSI)) {
            OldStepRecord last = oldStepInfo.get(prevSI);
            if (last==null) break;
            if (includeUpdates && last.workflowScratchUpdates !=null) {
                result = MutableMap.copyOf(last.workflowScratchUpdates).add(result);
            }
            includeUpdates = true;
            if (last.workflowScratch !=null) {
                result = MutableMap.copyOf(last.workflowScratch).add(result);
                result.entrySet().stream().filter(e -> Entities.REMOVE.equals(e.getValue())).map(Map.Entry::getKey).forEach(result::remove);
                break;
            }
            if (last.previous==null || last.previous.isEmpty()) break;
            prevSI = last.previous.iterator().next();
        }
        return Pair.of(result, previousSteps);
    }

    public Object getOutput() {
        return output;
    }

    public static void checkEqual(Object o1, Object o2) {
        if (!Objects.equals(o1, o2)) {
            log.warn("Objects different: " + o1 + " / " + o2);
            throw new IllegalStateException("Objects different: " + o1 + " / " + o2);
        }
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

    public Integer getReplayableLastStep() {
        return replayableLastStep;
    }

    public WorkflowStepInstanceExecutionContext getErrorHandlerContext() {
        return errorHandlerContext;
    }

    @JsonIgnore
    public String getRetentionHash() {
        if (retention!=null && Strings.isNonBlank(retention.hash)) return retention.hash;
        if (Strings.isNonBlank(getName())) return getName();
        return "anonymous-workflow-"+Math.abs(getStepsDefinition().hashCode());
    }

    public void updateRetentionFrom(WorkflowRetentionAndExpiration.WorkflowRetentionSettings other) {
        WorkflowRetentionAndExpiration.WorkflowRetentionSettings r = getRetentionSettings();
        r.updateFrom(other);
        retention = r;
        retentionDefault = null;
    }

    @JsonIgnore private transient WorkflowRetentionAndExpiration.WorkflowRetentionSettings retentionDefault;
    @JsonIgnore
    public WorkflowRetentionAndExpiration.WorkflowRetentionSettings getRetentionSettings() {
        if (retention==null) {
            if (retentionDefault==null) {
                retentionDefault = new WorkflowRetentionAndExpiration.WorkflowRetentionSettings().init(this);
            }
            return retentionDefault;
        }
        return retention;
    }

    public void markShutdown() {
        log.debug(this+" was "+this.status+" but now marking as "+WorkflowStatus.ERROR_SHUTDOWN+"; compensating workflow should be triggered shortly");
        this.updateStatus(WorkflowStatus.ERROR_SHUTDOWN);
        // don't persist; that will happen when workflows are kicked off
    }

    @JsonIgnore
    /** Error handlers _could_ launch sub-workflow, but they typically don't */
    protected boolean isInErrorHandlerSubWorkflow() {
        if (getParent()!=null) {
            if (getParent().getErrorHandlerContext()!=null) {
                return true;
            }
            return getParent().isInErrorHandlerSubWorkflow();
        }
        return false;
    }

    // for json
    private void setMostRecentActivityTime(Object ignored) {}

    /** look in tasks, steps, and replays to find most recent activity */
    // keep on jackson serialization for api?
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
            Task<?> lastTask = null;
            try {
                lastTask = getManagementContext().getExecutionManager().getTask(currentStepInstance.getTaskId());
            } catch (Exception e) {
                // probably not being managed; ignore
                Exceptions.propagateIfFatal(e);
            }
            considerTask.accept(lastTask);
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
            stepsResolved = MutableList.copyOf(new WorkflowStepResolution(this).resolveSteps(WorkflowExecutionContext.this.stepsDefinition, outputDefinition));
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
        private Runnable intro = null;
        private int stepsRun = 0;

        public Body() {}

        public Body(WorkflowStepDefinition.ReplayContinuationInstructions continuationInstructions) {
            this.continuationInstructions = continuationInstructions;
        }

        @Override
        public String toString() {
            return "WorkflowExecutionContext.Body["+workflowId+"; " + continuationInstructions + "]";
        }

        @Override
        public Object call() throws Exception {
            if (intro!=null) {
                // intro needed to make dangling resumption wait
                intro.run();
            }
            return callWithLock(this::callSteps);
        }

        protected Object callWithLock(Callable<Callable<Object>> handler) throws Exception {
            AttributeSensor<String> lockSensor0 = null;
            Entity lockEntity0 = null;
            if (lock != null) {
                String lockName = null;
                if (lock instanceof String) lockName = resolve(WorkflowExpressionResolution.WorkflowExpressionStage.WORKFLOW_INPUT, lock, TypeToken.of(String.class));
                else if (lock instanceof Map) {
                    lockName = resolve(WorkflowExpressionResolution.WorkflowExpressionStage.WORKFLOW_INPUT, ((Map)lock).get("name"), TypeToken.of(String.class));

                    Object lockEntity00 = ((Map)lock).get("entity");
                    if (lockEntity00!=null) {
                        lockEntity0 = resolve(WorkflowExpressionResolution.WorkflowExpressionStage.WORKFLOW_INPUT, lockEntity00, TypeToken.of(Entity.class));
                        log.debug(WorkflowExecutionContext.this + " using lock " + lockName + " on entity " + lockEntity0);
                    }
                }
                if (lockName==null) throw new IllegalArgumentException("Invalid lock object, should be a string or a map indicating a name and optional entity");
                lockSensor0 = Sensors.newStringSensor("lock-for-" + lockName);
                if (lockEntity0==null) lockEntity0 = getEntity();
            }
            AttributeSensor<String> lockSensor = lockSensor0;
            Entity lockEntity = lockEntity0;

            AtomicBoolean mustClearLock = new AtomicBoolean(false);
            if (lockSensor!=null) {
                // acquire lock
//          - step: set-sensor lock-for-count = ${workflow.id}
//            require:
//            any:
//            - when: absent
//            - equals: ${workflow.id}
//            on-error:
//            - retry backoff 50ms increasing 2x up to 5s
                String wid = getWorkflowId();
                Duration delay = null;
                String lastHolder = null;
                while (true) {
                    AtomicReference<String> holder = new AtomicReference<>();
                    lockEntity.sensors().modify(lockSensor, old -> {
                        if (old == null || old.equals(wid)) {
                            if (old != null) {
                                if (continuationInstructions != null) {
                                    log.debug(WorkflowExecutionContext.this+" reasserting lock on " + lockSensor.getName() + " during replay");
                                } else {
                                    // i don't think this should be possible
                                    log.warn("Entering block with lock on " + lockSensor.getName() + " when this workflow already holds the lock");
                                }
                            } else {
                                log.debug(WorkflowExecutionContext.this+" acquired lock on " + lockSensor.getName());
                            }
                            mustClearLock.set(true);
                            return Maybe.of(wid);
                        }
                        log.debug("Blocked by lock on " + lockSensor.getName() + ", currently held by " + old);
                        holder.set(old);
                        return Maybe.absent();
                    });
                    if (mustClearLock.get()) break;
                    // didn't get lock; probably do a retry
                    try {
                        if (delay==null || !Objects.equals(lastHolder, holder.get())) {
                            // reset initially, and if we observe the lock holder to change (highly competitive environment)
                            delay = Duration.millis(5);
                        }
                        Duration ddelay = delay;
                        Tasks.withBlockingDetails("Waiting for lock on " + lockSensor.getName() +" (held by workflow "+holder.get()+")", () -> { Time.sleep(ddelay); return null; });
                        // increment delay for next time
                        delay = Duration.max(delay.multiply(1 + Math.random()), Duration.seconds(5));
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    }
                }
            }

            Callable<Object> endHandler;
            try {
                endHandler = handler.call(); //super.doTaskBodyPossiblyWithLock(context, handler, isReplaying, continuationInstructions);

            } finally {
                // clear if we set it, whether initially, on real replay, or on injected dangling failure
                if (mustClearLock.get()) {
                    try {
                        DynamicTasks.waitForLast();
                    } finally {
                        if (Entities.isUnmanagingOrNoLongerManaged(getEntity())) {
                            log.debug("Skipping clearance of lock on "+lockSensor.getName()+" in "+WorkflowExecutionContext.this+" because entity unmanaging here; expect auto-replay on resumption to pick up");
                        } else {
                            Threads.runTemporarilyUninterrupted(() -> {
                                log.debug(WorkflowExecutionContext.this + " releasing lock on " + lockSensor.getName());
                                ((EntityInternal.SensorSupportInternal) lockEntity.sensors()).remove(lockSensor);
                            });
                        }
                    }
                }
            }
            return endHandler.call();
        }

        boolean continueOnErrorHandledOrNextReplay;
        AtomicReference<Boolean> timerCancelled;

        public Callable<Object> callSteps() throws Exception {
            DynamicTasks.swallowChildrenFailures();

            Task<?> timerTask = null;
            timerCancelled = new AtomicReference<>(false);
            try {
                if (timeout != null) {
                    timerTask = initializeTimerFromWorkflowTimeout(timerTask);
                }

                RecoveryAndReplay: do {
                    try {
                        boolean replaying = continuationInstructions!=null;
                        Integer replayFromStep = replaying ? continuationInstructions.stepToReplayFrom : null;

                        if (!replaying) initializeWithoutContinuationInstructions(replayFromStep);

                        continueOnErrorHandledOrNextReplay = false;
                        lastErrorHandlerOutput = null;

                        WorkflowReplayUtils.updateOnWorkflowTaskStartupOrReplay(WorkflowExecutionContext.this, task, getStepsResolved(), !replaying, replayFromStep);

                        // show task running
                        updateStatus(WorkflowStatus.RUNNING);

                        if (replaying) initializeFromContinuationInstructions(replayFromStep);

                        if (!Objects.equals(taskId, Tasks.current().getId()))
                            throw new IllegalStateException("Running workflow in unexpected task, " + taskId + " does not match " + task);

                        int stepsConsidered = 0;
                        while (currentStepIndex >= 0 && currentStepIndex < getStepsResolved().size()) {
                            stepsConsidered++;
                            if (replaying && replayFromStep == null) {
                                // check step number and clear output before re-running
                                if (currentStepInstance==null || currentStepInstance.getStepIndex()!=currentStepIndex) {
                                    throw new IllegalStateException("Running workflow at unexpected step, "+currentStepIndex+" v "+currentStepInstance);
                                }
                                currentStepInstance.setOutput(null);
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

                        log.debug("Completed workflow "+workflowId+" successfully; step count: "+stepsConsidered+" considered, "+stepsRun+" executed");

                        if (outputDefinition != null) {
                            output = resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_FINISHING_POST_OUTPUT, outputDefinition, Object.class);
                        } else {
                            if (stepsRun>0) {
                                // (default is the output of the last step, if there is one, otherwise nothing)
                                // ((unlike steps, workflow output is not available as a default value, but previous step always is, so there is no need to do this
                                // before the above; slight chance if onError is triggered by a failure to resolve something in outputDefinition, but that can be ignored))
                                output = getPreviousStepOutput();
                            }
                        }

                        updateOnSuccessfulCompletion();

                    } catch (Throwable e) {
                        try {
                            Pair<Throwable, WorkflowStatus> unhandledError = handleErrorAtWorkflow(e);

                            if (unhandledError != null) {
                                return () -> endWithError(unhandledError.getLeft(), unhandledError.getRight());
                            }
                            if (!continueOnErrorHandledOrNextReplay) {
                                updateOnSuccessfulCompletion();
                            }
                        } catch (Throwable e2) {
                            // do not propagateIfFatal, we need to handle most throwables
                            log.debug("Uncaught error in workflow exception handler: "+ e2, e2);
                            return () -> endWithError(e2, WorkflowStatus.ERROR);
                        }
                    }

                } while (continueOnErrorHandledOrNextReplay);

            } finally {
                if (timerTask != null && !timerTask.isDone() && !timerCancelled.get()) {
                    log.debug("Cancelling " + timerTask + " on completion of this task");
                    timerTask.cancel(true);
                }
            }

            return this::endWithSuccess;
        }

        private Pair<Throwable, WorkflowStatus> handleErrorAtWorkflow(Throwable e) {
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

            } else if (Thread.currentThread().isInterrupted()) {
                // don't run error handler
                log.debug("Interrupt in workflow '" + getName() + "' around step " + workflowStepReference(currentStepIndex) + ", throwing: " + Exceptions.collapseText(e));

            } else if (onError != null && (!(onError instanceof Collection) || !((Collection)onError).isEmpty())) {
                try {
                    if (currentStepInstance.getError()==null) {
                        log.warn("Error in workflow '" + getName() + "' around step " + workflowStepReference(currentStepIndex) + ", running error handler but likely this should be corrected in code -- " + Exceptions.collapseText(e));
                        log.debug("Trace of error:", e);
                    } else {
                        log.debug("Error in workflow '" + getName() + "' around step " + workflowStepReference(currentStepIndex) + ", running error handler");
                    }
                    Task<WorkflowErrorHandling.WorkflowErrorHandlingResult> workflowErrorHandlerTask = WorkflowErrorHandling.createWorkflowErrorHandlerTask(WorkflowExecutionContext.this, task, e);
                    errorHandlerTaskId = workflowErrorHandlerTask.getId();
                    WorkflowErrorHandling.WorkflowErrorHandlingResult result = DynamicTasks.queue(workflowErrorHandlerTask).getUnchecked();
                    if (result != null) {
                        errorHandled = true;

                        currentStepInstance.next = WorkflowReplayUtils.getNext(result.next, STEP_TARGET_NAME_FOR_END);
                        if (result.output != null) {
                            output = resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_FINISHING_POST_OUTPUT, result.output, Object.class);
                        }

                        moveToNextStep("Handled error in workflow around step " + workflowStepReference(currentStepIndex), result.next==null);

                        if (continuationInstructions!=null || currentStepIndex < getStepsResolved().size()) {
                            continueOnErrorHandledOrNextReplay = true;
                            return null;
                        }
                    } // else errorHandled remains false and will fail below

                } catch (Exception e2) {
                    WorkflowErrorHandling.logExceptionWhileHandlingException(() -> "in '" + getName() + "' around step " + workflowStepReference(currentStepIndex), entity, e2, e);
                    e = e2;
                }

            } else {
                // provide the error in case useful; if too wasteful, attach an error handler to fail rethrow
                log.debug("Error in workflow '" + getName() + "' around step " + workflowStepReference(currentStepIndex) + ", no error handler so rethrowing: " + Exceptions.collapseText(e), e);
            }

            if (errorHandled) return null;

            if (replayAutomaticallyIfAppropriate(e)) return null;

            return Pair.of(e, provisionalStatus);
        }

        private boolean replayAutomaticallyIfAppropriate(Throwable e) {
            if (Boolean.TRUE.equals(replayableAutomatically) && Exceptions.getFirstThrowableOfType(e, DanglingWorkflowException.class)!=null) {
                log.info("Automatic replay indicated for "+WorkflowExecutionContext.this+" when detected as dangling on server startup");

                currentStepInstance.next = factory(true).makeInstructionsForReplayResuming("Replay resuming on dangling", false);
                continueOnErrorHandledOrNextReplay = true;
                return true;
            }

            return false;
        }

        private void updateOnSuccessfulCompletion() {
            // finished -- checkpoint noting previous step and null for current because finished
            updateStatus(WorkflowStatus.SUCCESS);
            replayableLastStep = STEP_INDEX_FOR_END;
            // record how it ended
            oldStepInfo.compute(previousStepIndex == null ? STEP_INDEX_FOR_START : previousStepIndex, (index, old) -> {
                if (old == null) old = new OldStepRecord();
                old.next = MutableSet.of(STEP_INDEX_FOR_END).putAll(old.next);
                old.nextTaskId = null;
                return old;
            });
            oldStepInfo.compute(STEP_INDEX_FOR_END, (index, old) -> {
                if (old == null) old = new OldStepRecord();
                old.previous = MutableSet.of(previousStepIndex == null ? STEP_INDEX_FOR_START : previousStepIndex).putAll(old.previous);
                old.previousTaskId = previousStepTaskId;
                return old;
            });
        }

        private void resetWorkflowContextPreviousAndScratchVarsToStep(Integer step, boolean requireLastStep) {
            if (step==null) {
                // when resuming, keep them as they were set
                return;
            }
            OldStepRecord last = oldStepInfo.get(step);
            if (last != null) {
                workflowScratchVariables = getStepWorkflowScratchAndBacktrackedSteps(step).getLeft();
                previousStepIndex = last.previous==null ? null : last.previous.stream().findFirst().orElse(null);

            } else {
                if (requireLastStep) {
                    throw new IllegalStateException("Last step record required for step "+step+" to replay from there");
                } else {
                    // no such step; probably starting something which has never been run (not uncommon), or at a step which was never run (not sure if possible);
                    // in any case just keep the scratch vars as they were
                }
            }
            // and ensure not null
            if (workflowScratchVariables == null) workflowScratchVariables = MutableMap.of();
        }

        private void initializeFromContinuationInstructions(Integer replayFromStep) {
            if (replayFromStep != null && replayFromStep == STEP_INDEX_FOR_START) {
                log.debug("Replaying workflow '" + name + "', from start " +
                        "(was at " + (currentStepIndex == null ? "<UNSTARTED>" : workflowStepReference(currentStepIndex)) + ")");
                resetWorkflowContextPreviousAndScratchVarsToStep(replayFromStep, false);
                currentStepIndex = 0;

            } else if (replayFromStep != null && replayFromStep == STEP_INDEX_FOR_END) {
                log.debug("Replaying workflow '" + name + "', from end " +
                        "(was at " + (currentStepIndex == null ? "<UNSTARTED>" : workflowStepReference(currentStepIndex)) + ")");
                currentStepIndex = STEP_INDEX_FOR_END;
                resetWorkflowContextPreviousAndScratchVarsToStep(replayFromStep, false);
                currentStepInstance = null;

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
                } else {
                    currentStepIndex = replayFromStep;
                }
                // must reset, but okay if null, in that case we are continuing to a step which hasn't been initialized yet; use scratch vars
                resetWorkflowContextPreviousAndScratchVarsToStep(currentStepIndex, false);
            }
            if (continuationInstructions.customWorkflowScratchVariables!=null) {
                updateWorkflowScratchVariables(continuationInstructions.customWorkflowScratchVariables);
            }

        }

        private void initializeWithoutContinuationInstructions(Integer replayFromStep) {
            if (replayFromStep == null && currentStepIndex == null) {
                currentStepIndex = 0;
                log.debug("Starting workflow '" + name + "', moving to first step " + workflowStepReference(currentStepIndex));

            } else if (replayFromStep==null && continueOnErrorHandledOrNextReplay) {
                // workflow error handler indicated a next step to run

            } else {
                // shouldn't come here
                continueOnErrorHandledOrNextReplay = false;
                throw new IllegalStateException("Should either be replaying or unstarted, but not invoked as replaying, and current=" + currentStepIndex + " replay=" + replayFromStep);
            }
        }

        private Task<?> initializeTimerFromWorkflowTimeout(Task<?> timerTask) {
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
            return timerTask;
        }

        private Object endWithSuccess() {
            WorkflowReplayUtils.updateOnWorkflowSuccess(WorkflowExecutionContext.this, task, getOutput());
            persist();
            return output;
        }

        private Object endWithError(Throwable e, WorkflowStatus provisionalStatus) {
            updateStatus(provisionalStatus);
            WorkflowReplayUtils.updateOnWorkflowError(WorkflowExecutionContext.this, task, e);
            persist();

            try {
                log.debug("Error running workflow " + this + "; will persist then rethrow: " + e);
                log.trace("Error running workflow " + this + "; will persist then rethrow (details): " + e, e);

            } catch (Throwable e2) {
                if (Entities.isUnmanagingOrNoLongerManaged(getEntity())) {
                    log.trace("Error persisting workflow (entity ending) " + this + " after error in workflow; persistence error (details): " + e2, e2);
                } else {
                    log.error("Error persisting workflow " + this + " after error in workflow; persistence error: " + e2);
                    log.debug("Error persisting workflow " + this + " after error in workflow; persistence error (details): " + e2, e2);
                    log.warn("Error running workflow " + this + ", rethrowing without persisting because of persistence error (above): " + e);
                }
                log.trace("Error running workflow " + this + ", rethrowing without persisting because of persistence error (above): " + e, e);
            }

            throw Exceptions.propagate(e);
        }

        protected void runCurrentStepIfPreconditions() {
            WorkflowStepDefinition step = getStepsResolved().get(currentStepIndex);
            if (step!=null) {
                currentStepInstance = new WorkflowStepInstanceExecutionContext(currentStepIndex, step, WorkflowExecutionContext.this);
                DslPredicates.DslPredicate conditionResolved = step.getConditionResolved(currentStepInstance);
                if (conditionResolved!=null) {
                    if (log.isTraceEnabled()) log.trace("Considering condition "+step.condition+" for "+ workflowStepReference(currentStepIndex));
                    boolean conditionMet = DslPredicates.evaluateDslPredicateWithBrooklynObjectContext(conditionResolved, WorkflowExecutionContext.this, getEntityOrAdjunctWhereRunning());
                    if (log.isTraceEnabled()) log.trace("Considered condition "+step.condition+" for "+ workflowStepReference(currentStepIndex)+": "+conditionMet);
                    if (!conditionMet) {
                        moveToNextStep("Skipping step "+ workflowStepReference(currentStepIndex), false);
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
            stepsRun++;

            Task<?> t;
            if (continuationInstructions!=null) {
                t = step.newTaskContinuing(currentStepInstance, continuationInstructions);
                continuationInstructions = null;
            } else {
                t = step.newTask(currentStepInstance);
            }

            updateOldNextStepOnThisStepStarting();

            // about to run -- checkpoint noting current and previous steps, and updating replayable from info
            OldStepRecord currentStepRecord = oldStepInfo.compute(currentStepIndex, (index, old) -> {
                if (old == null) old = new OldStepRecord();
                old.countStarted++;

                old.workflowScratchUpdates = null;

                old.previous = MutableSet.<Integer>of(previousStepIndex == null ? STEP_INDEX_FOR_START : previousStepIndex).putAll(old.previous);
                old.previousTaskId = previousStepTaskId;
                old.nextTaskId = null;
                return old;
            });
            WorkflowReplayUtils.updateReplayableFromStep(WorkflowExecutionContext.this, step);
            oldStepInfo.compute(previousStepIndex==null ? STEP_INDEX_FOR_START : previousStepIndex, (index, old) -> {
                if (old==null) old = new OldStepRecord();
                if (previousStepIndex==null && workflowScratchVariables!=null && !workflowScratchVariables.isEmpty()) {
                    // if workflow scratch vars were initialized prior to run, we nee to save those
                    old.workflowScratch = MutableMap.copyOf(workflowScratchVariables);
                }
                old.next = MutableSet.<Integer>of(currentStepIndex).putAll(old.next);
                old.nextTaskId = t.getId();
                return old;
            });

            errorHandlerContext = null;
            errorHandlerTaskId = null;
            currentStepInstance.next = null;  // clear, eg if was set from a previous run; will be reset from step definition
            // but don't clear output, in case a step is returning to itself and wants to reference previous_step.output

            persist();

            BiConsumer<Object,Object> onFinish = (stepOutputDefinition,overrideNext) -> {
                currentStepInstance.next = WorkflowReplayUtils.getNext(overrideNext, currentStepInstance, step);
                if (stepOutputDefinition!=null) {
                    Object outputResolved = resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_FINISHING_POST_OUTPUT, stepOutputDefinition, Object.class);
                    currentStepInstance.setOutput(outputResolved);
                }
                if (currentStepInstance.output != null) {
                    Pair<Object, Set<Integer>> prev = getStepOutputAndBacktrackedSteps(null);
                    if (prev != null && Objects.equals(prev.getLeft(), currentStepInstance.output) && lastErrorHandlerOutput==null) {
                        // optimization, clear the value here if we can simply take it from the previous step
                        currentStepInstance.output = null;
                    }
                }
                if (workflowScratchVariablesUpdatedThisStep!=null && !workflowScratchVariablesUpdatedThisStep.isEmpty()) {
                    currentStepRecord.workflowScratchUpdates = workflowScratchVariablesUpdatedThisStep;
                }
                if (currentStepRecord.workflowScratch != null) {
                    // if we are repeating, check if we need to keep what we were repeating
                    Pair<Map<String, Object>, Set<Integer>> prev = getStepWorkflowScratchAndBacktrackedSteps(null);
                    if (prev!=null && !prev.getRight().contains(currentStepIndex) && Objects.equals(prev.getLeft(), currentStepRecord.workflowScratch)){
                        currentStepRecord.workflowScratch = null;
                    }
                }

                workflowScratchVariablesUpdatedThisStep = null;
            };

            // now run the step
            try {
                Duration duration = step.getTimeout();
                Object newOutput;
                if (duration!=null) {
                    boolean isEnded = DynamicTasks.queue(t).blockUntilEnded(duration);
                    if (isEnded) {
                        newOutput = t.getUnchecked();
                    } else {
                        t.cancel(true);
                        throw new TimeoutException("Timeout after "+duration+": "+t.getDisplayName());
                    }
                } else {
                    newOutput = DynamicTasks.queue(t).getUnchecked();
                }
                currentStepInstance.setOutput(newOutput);

                // allow output to be customized / overridden
                onFinish.accept(step.output, null);

            } catch (Exception e) {
                try {
                    handleErrorAtStep(step, t, onFinish, e);
                } catch (Exception e2) {
                    currentStepInstance.setError(e2);
                    throw e2;
                }
            } finally {
                // do this whether or not error
                oldStepInfo.compute(currentStepIndex, (index, old) -> {
                    if (old == null) {
                        log.warn("Lost old step info for " + this + ", step " + index);
                        old = new OldStepRecord();
                    }
                    if (currentStepInstance.getError()==null) old.countCompleted++;
                    // okay if this gets picked up by accident because we will check the stepIndex it records against the currentStepIndex,
                    // and ignore it if different
                    old.context = currentStepInstance;
                    return old;
                });
            }

            previousStepTaskId = currentStepInstance.taskId;
            previousStepIndex = currentStepIndex;
            moveToNextStep("Completed step "+ workflowStepReference(currentStepIndex), false);
        }

        private void handleErrorAtStep(WorkflowStepDefinition step, Task<?> stepTaskThrowingError, BiConsumer<Object, Object> onFinish, Exception error) {
            WorkflowErrorHandling.handleErrorAtStep(getEntity(), step, currentStepInstance, stepTaskThrowingError, onFinish, error, null);
        }

        private void moveToNextStep(String prefix, boolean inferredNext) {
            prefix = prefix + "; ";

            Object specialNext = WorkflowReplayUtils.getNext(currentStepInstance);

            continuationInstructions = specialNext instanceof WorkflowStepDefinition.ReplayContinuationInstructions ? (WorkflowStepDefinition.ReplayContinuationInstructions) specialNext : null;
            if (continuationInstructions!=null) {
                log.debug(prefix + "proceeding to custom replay: "+continuationInstructions);
                return;
            }

            if (specialNext==null) {
                currentStepIndex++;
                if (currentStepIndex < getStepsResolved().size()) {
                    log.debug(prefix + "moving to sequential next step " + workflowStepReference(currentStepIndex));
                } else {
                    log.debug(prefix + "no further steps: Workflow completed");
                }
            } else if (specialNext instanceof String) {
                SubworkflowLocality subworkflowLocality = getParent() != null && getParent().currentStepInstance != null ? getParent().currentStepInstance.subworkflowLocality : null;
                boolean isInLocalSubworkflow = subworkflowLocality!=null && subworkflowLocality.ordinal()>=SubworkflowLocality.LOCAL_STEPS_SHARED_CONTEXT.ordinal();
                if (isInLocalSubworkflow && STEP_TARGET_NAME_FOR_END.equals(specialNext)) {
                    // parent of local subworkflow should return also
                    if (Boolean.TRUE.equals(currentStepInstance.nextIsReturn)) {
                        getParent().currentStepInstance.next = specialNext;
                        getParent().currentStepInstance.nextIsReturn = true;
                    } else if (SubworkflowLocality.INLINE_SHARED_CONTEXT.equals(subworkflowLocality)) {
                        // if statement as shorthand should go to end of calling workflow
                        getParent().currentStepInstance.next = specialNext;
                    }
                }

                String explicitNext = (String)specialNext;
                Maybe<Pair<Integer, Boolean>> nextResolved = getIndexOfStepId(explicitNext);
                if (nextResolved.isAbsent() && isInLocalSubworkflow) {
                    // if in subworkflow, you can goto something in the outer workflow, if not found here
                    getParent().currentStepInstance.next = specialNext;
                    nextResolved = getIndexOfStepId(STEP_TARGET_NAME_FOR_END);
                }
                if (nextResolved.isAbsent()) {
                    log.warn(prefix +  (inferredNext ? "inferred" : "explicit") + " next step '"+explicitNext+"' not found (failing)");
                    // throw
                    nextResolved.get();
                }
                if (nextResolved.get().getLeft()==null) {
                    throw new IllegalArgumentException("Next step '"+explicitNext+"' not supported here");
                }

                currentStepIndex = nextResolved.get().getLeft();
                if (nextResolved.get().getRight()) {
                    if (currentStepIndex < getStepsResolved().size()) {
                        log.debug(prefix + "moving to "+(inferredNext ? "inferred" : "explicit")+" next step " + workflowStepReference(currentStepIndex) + " for token '" + explicitNext + "'");
                    } else {
                        log.debug(prefix + (inferredNext ? "inferred" : "explicit") + " next step '"+explicitNext+"': Workflow completed");
                    }
                } else {
                    log.debug(prefix + "moving to "+(inferredNext ? "inferred" : "explicit")+" next step " + workflowStepReference(currentStepIndex) + " for id '" + explicitNext + "'");
                }
            } else {
                throw new IllegalStateException("Illegal next definition: "+specialNext+" (type "+specialNext.getClass()+")");
            }
        }

        String workflowStepReference(Integer index) {
            if (index==null) return workflowId+"-<no-step>";

            if (index>=getStepsResolved().size()) return getWorkflowStepReference(index, "<END>", false);
            return getWorkflowStepReference(index, getStepsResolved().get(index));
        }

        public Body withIntro(Runnable intro) {
            this.intro = intro;
            return this;
        }
    }

    private void updateStepOutput(WorkflowStepInstanceExecutionContext step, Object newOutput) {
        step.output = step.outputOld = newOutput;
    }
    private void updateOldNextStepOnThisStepStarting() {
        // at step start, we update the _next_ record to have a copy of our old output and workflow vars
        OldStepRecord old = oldStepInfo.get(currentStepInstance.stepIndex);
        if (old!=null && old.next!=null && !old.next.isEmpty()) {
            Integer lastNext = old.next.iterator().next();
            OldStepRecord oldNext = oldStepInfo.get(lastNext);
            if (oldNext!=null && oldNext.context!=null) {
                // if oldNext has no context then we never ran it, so we aren't repeating, we're replaying
                if (oldNext.context.output ==null) {
                    // below will access the _previous_ StepInstanceExecutionContext, as oldStepRecord.context is update at end of step
                    // thus below gets the _previous_ output known at this step, saving it in the next
                    oldNext.context.output = old.context.output;
                }

                oldNext.workflowScratch = getWorkflowScratchVariables();
            }
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
        return workflowId+(index>=0 ? "-"+(index+1) : (index==STEP_INDEX_FOR_ERROR_HANDLER && isError) ? "" : "-"+indexCode(index))
                +(Strings.isNonBlank(optionalStepId) ? "-"+optionalStepId : "")
                +(isError ? "-"+LABEL_FOR_ERROR_HANDLER : "");
    }
    public String getWorkflowStepReference(Task<?> t) {
        BrooklynTaskTags.WorkflowTaskTag wt = BrooklynTaskTags.getWorkflowTaskTag(t, false);
        if (wt.getErrorHandlerIndex()!=null) {
            // formula below not suitable for error tasks, but the name should be good
            return t.getDisplayName();
        }
        return wt.getWorkflowId()
                +(wt.getStepIndex()!=null && wt.getStepIndex()>=0 && wt.getStepIndex()<stepsDefinition.size() ? "-"+(wt.getStepIndex()+1) : "")
                +(wt.getErrorHandlerIndex()!=null ? "-error-handler-"+(wt.getErrorHandlerIndex()+1) : "")
                ;
    }

    private String indexCode(int index) {
        // these numbers shouldn't be used for much, but they are used in a few places :(
        if (index==STEP_INDEX_FOR_START) return STEP_TARGET_NAME_FOR_START;
        if (index==STEP_INDEX_FOR_END) return STEP_TARGET_NAME_FOR_END;
        if (index==STEP_INDEX_FOR_ERROR_HANDLER) return LABEL_FOR_ERROR_HANDLER;
        return "neg-"+(index); // unknown
    }

    public static class Converter implements com.fasterxml.jackson.databind.util.Converter<WorkflowExecutionContext,WorkflowExecutionContext> {
        @Override
        public WorkflowExecutionContext convert(WorkflowExecutionContext value) {
            if (value.workflowScratchVariables ==null || value.workflowScratchVariables.isEmpty()) {
                value.workflowScratchVariables = value.getStepWorkflowScratchAndBacktrackedSteps(null).getLeft();
            }
            // note: no special handling needed for output; it is derived from the last non-null step output
            return value;
        }

        @Override
        public JavaType getInputType(TypeFactory typeFactory) {
            return typeFactory.constructType(WorkflowExecutionContext.class);
        }

        @Override
        public JavaType getOutputType(TypeFactory typeFactory) {
            return typeFactory.constructType(WorkflowExecutionContext.class);
        }
    }
}
