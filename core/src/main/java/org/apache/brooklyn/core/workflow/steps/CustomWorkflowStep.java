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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.*;
import org.apache.brooklyn.core.workflow.utils.WorkflowConcurrencyParser;
import org.apache.brooklyn.core.workflow.utils.WorkflowRetentionParser;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CustomWorkflowStep extends WorkflowStepDefinition implements WorkflowStepDefinition.WorkflowStepDefinitionWithSpecialDeserialization, WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow {

    private static final Logger LOG = LoggerFactory.getLogger(CustomWorkflowStep.class);

    // Name of the scratch variable used to store the target of a nested workflow when being run as a subworkflow over a target collection
    public static final String SHORTHAND_TYPE_NAME_DEFAULT = "workflow";

    public static final String TARGET_VAR_NAME_DEFAULT = "target";
    // Name of the scratch variable used to store the index of a nested workflow when being run as a subworkflow over a target collection
    public static final String TARGET_INDEX_VAR_NAME_DEFAULT = "target_index";

    private static final String WORKFLOW_SETTING_SHORTHAND = "[ \"replayable\" ${replayable...} ] [ \"retention\" ${retention...} ] ";

    public CustomWorkflowStep() {}
    public CustomWorkflowStep(String name, List<Object> steps) {
        this.name = name;
        this.steps = steps;
    }

    public CustomWorkflowStep(CustomWorkflowStep base) {
        this.retention = base.retention;
        this.target = base.target;
        this.target_var_name = base.target_var_name;
        this.target_index_var_name = base.target_index_var_name;
        this.lock = base.lock;
        this.concurrency = base.concurrency;
        this.parameters = base.parameters;
        this.steps = base.steps;
        this.workflowOutput = base.workflowOutput;
        this.reducing = base.reducing;
        this.id = base.id;
        this.name = base.name;
        this.metadata = base.metadata;
        this.userSuppliedShorthand = base.userSuppliedShorthand;
        this.shorthandTypeName = base.shorthandTypeName;
        this.input = base.input;
        this.next = base.next;
        this.condition = base.condition;
        this.output = base.output;
        this.replayable = base.replayable;
        this.idempotent = base.idempotent;
        this.timeout = base.timeout;
        this.onError = base.onError;
    }

    @JsonCreator
    /** special creator for when a list is supplied as the workflow; treat those as steps, without requiring a superfluous `steps` entry in a map.
     *  this is useful especially for config key / parameters of type `workflow` ({@link CustomWorkflowStep)}. */
    public CustomWorkflowStep(List<Object> steps) {
        this.steps = steps;
    }

    String shorthand;
    @Override
    public void populateFromShorthand(String value) {
        if (shorthand==null) {
            if (SHORTHAND_TYPE_NAME_DEFAULT.equals(shorthandTypeName)) {
                shorthand = WORKFLOW_SETTING_SHORTHAND;
            } else {
                throw new IllegalStateException("Shorthand not supported for " + getNameOrDefault());
            }
        }
        if (input==null) input = MutableMap.of();

        populateFromShorthandTemplate(shorthand, value);

        replayable = (String) input.remove("replayable");
        retention = (String) input.remove("retention");
    }

    protected String retention;

    /** What to run this set of steps against, either an entity to run in that context, 'children' or 'members' to run over those, a range eg 1..10,  or a list (often in a variable) to run over elements of the list */
    protected Object target;

    protected Object target_var_name;
    protected Object target_index_var_name;

    // see WorkflowCommonConfig.LOCK
    protected Object lock;

    // usually a string; see utils/WorkflowConcurrency
    protected Object concurrency;

    protected Map<String,Object> parameters;

    // should be treated as raw json
    @JsonDeserialize(contentUsing = JsonPassThroughDeserializer.class)
    protected List<Object> steps;

    // output transform to be applied to the result of each sub-workflow (if there are multiple ones, and/or if it is saved as a type)
    // inferred from `output` where a workflow is saved as a new registered type, and can be extended (once) by setting `output` on the referring workflow
    protected Object workflowOutput;

    protected Map<String,Object> reducing;

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        if (steps instanceof List) WorkflowStepResolution.resolveSteps(mgmt, (List<Object>) steps);
        else if (steps!=null) throw new IllegalArgumentException("Workflow `steps` must be a list");
        else if (target!=null) throw new IllegalArgumentException("Workflow cannot take a `target` without `steps`");
    }

    @Override
    protected boolean isOutputHandledByTask() { return true; }

    // error handler applies to each run
    @Override
    public List<Object> getOnError() {
        return null;
    }

    @JsonIgnore
    public Object getConditionRaw() {
        if (target!=null) {
            // condition applies to each run
            return null;
        }
        return super.getConditionRaw();
    }

    protected Pair<WorkflowReplayUtils.ReplayableAtStepOption, Boolean> validateReplayableAndIdempotent() {
        return WorkflowReplayUtils.validateReplayableAndIdempotentAtStep(replayable, idempotent, true);
    }

    @Override @JsonIgnore
    public SubWorkflowsForReplay getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, boolean forced, boolean peekingOnly, boolean allowInternallyEvenIfDisabled) {
        return WorkflowReplayUtils.getSubWorkflowsForReplay(context, forced, peekingOnly, allowInternallyEvenIfDisabled, sw -> sw.isResumableOnlyAtParent = true);
    }

    @Override
    public Object doTaskBodyWithSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, @Nonnull List<WorkflowExecutionContext> subworkflows, ReplayContinuationInstructions instructions) {
        boolean wasList = Boolean.TRUE.equals(getStepState(context).wasList);
        return runSubworkflowsWithConcurrency(context, subworkflows, getStepState(context).reducing, wasList, true, instructions);
    }

    static class StepState {
        Boolean wasList;
        Map reducing;
    }
    void setStepState(WorkflowStepInstanceExecutionContext context, StepState state, boolean persist) {
        context.setStepState(state, persist);
    }
    @Override protected StepState getStepState(WorkflowStepInstanceExecutionContext context) {
        StepState result = (StepState) super.getStepState(context);
        if (result==null) {
            result = new StepState();
            setStepState(context, result, false);
        }
        return result;
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        // 'replayable from here' configured elsewhere

        // 'retention <value>'
        if (retention!=null) {
            context.getWorkflowExectionContext().updateRetentionFrom(WorkflowRetentionParser.parse(retention, context.getWorkflowExectionContext()).init(context.getWorkflowExectionContext()));
        }

        if (steps==null) {
            return context.getPreviousStepOutput();
        }

        Object targetR = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, target, Object.class);

        if (targetR instanceof String) {
            targetR = getTargetFromString(context, (String) targetR);
        }

        List<WorkflowExecutionContext> nestedWorkflowContext = MutableList.of();

        boolean wasList = targetR instanceof Iterable;

        targetR = checkTarget(targetR);

        Map reducingV = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, reducing, Map.class);

        AtomicInteger index = new AtomicInteger(0);
        ((Iterable<?>) targetR).forEach(t -> {
            WorkflowExecutionContext nw = newWorkflow(context, t, wasList ? index.getAndIncrement() : null);
            Maybe<Task<Object>> mt = nw.getTask(true);

            String targetS = wasList || t !=null ? " for target '"+t+"'" : "";
            if (mt.isAbsent()) {
                LOG.debug("Step " + context.getWorkflowStepReference() + " skipping nested workflow " + nw.getWorkflowId() + targetS + "; condition not met");
            } else {
                LOG.debug("Step " + context.getWorkflowStepReference() + " launching nested workflow " + nw.getWorkflowId() + targetS + " in task " + nw.getTaskId());
                nestedWorkflowContext.add(nw);
            }
        });

        StepState state = getStepState(context);
        state.wasList = wasList;
        state.reducing = reducingV;
        setStepState(context, state, false); // persist in next line
        WorkflowReplayUtils.setNewSubWorkflows(context, nestedWorkflowContext.stream().map(BrooklynTaskTags::tagForWorkflow).collect(Collectors.toList()), Tasks.current().getId());
        // persist children now in case they aren't run right away, so that they are known in case of replay, we can incrementally resume (but after parent list)
        nestedWorkflowContext.forEach(n -> n.persist());

        return runSubworkflowsWithConcurrency(context, nestedWorkflowContext, reducingV, wasList, false, null);
    }

    protected Iterable checkTarget(Object targetR) {
        if (targetR instanceof Iterable) return (Iterable)targetR;

        if (targetR == null) { /* fine if no target supplied */ }
        else if (targetR instanceof Entity) { /* entity is also supported */ }

        else {
            throw new IllegalArgumentException("Target of workflow must be an entity, a list, or an expression that resolves to a list");
        }

        return MutableList.of(targetR);
    }

    private Object runSubworkflowsWithConcurrency(WorkflowStepInstanceExecutionContext context, List<WorkflowExecutionContext> nestedWorkflowContexts, Map reducingV, boolean wasList, boolean isReplaying, ReplayContinuationInstructions instructionsIfReplaying) {
        LOG.debug("Running sub-workflows "+nestedWorkflowContexts);
        if (nestedWorkflowContexts.isEmpty()) return reducingV!=null ? reducingV : MutableList.of();

        long ci = 1;
        Object c = concurrency;
        if (c != null && wasList) {
            if (reducingV!=null) {
                // if reducing, force concurrency 1, and also disallow dynamic concurrency (to prevent things that work in tests with dynamic value resolving to 1 but fail in real life when not 1)
                Maybe<Integer> cm = TypeCoercions.tryCoerce(c, Integer.class);
                if (cm.isAbsent() || cm.get()!=1)
                    throw new IllegalArgumentException("Concurrency cannot be used unless static value 1 when reducing");
                ci = 1;
            } else {
                c = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, c, Object.class);
                if (c instanceof Number) {
                    // okay
                } else if (c instanceof String) {
                    c = WorkflowConcurrencyParser.parse((String) c).apply((double) nestedWorkflowContexts.size());
                } else {
                    throw new IllegalArgumentException("Unsupported concurrency object: '" + c + "'");
                }
                ci = (long) Math.floor(0.000001 + ((Number) c).doubleValue());
                if (ci <= 0)
                    throw new IllegalArgumentException("Invalid concurrency value: " + ci + " (concurrency " + c + ", target size " + nestedWorkflowContexts.size() + ")");
            }
        }

        AtomicInteger availableThreads = ci == 1 ? null : new AtomicInteger((int) ci);
        List<Task<?>> submitted = MutableList.of();
        List<Pair<WorkflowExecutionContext,Task<?>>> delayedBecauseReducing = MutableList.of();
        WorkflowExecutionContext lastWorkflowRunBeforeReplay = null;
        List<Throwable> errors = MutableList.of();
        for (int i = 0; i < nestedWorkflowContexts.size(); i++) {
            if (availableThreads != null) while (availableThreads.get() <= 0) {
                try {
                    Tasks.withBlockingDetails("Waiting before running remaining " + (nestedWorkflowContexts.size() - i) + " instances because " + ci + " are currently running",
                            () -> {
                                synchronized (availableThreads) {
                                    availableThreads.wait(500);
                                }
                                return null;
                            });
                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }
            }

            Task<Object> task;
            if (!isReplaying) {
                task = nestedWorkflowContexts.get(i).getTask(false).get();
            } else {
                try {
                    Pair<Boolean, Object> check = WorkflowReplayUtils.checkReplayResumingInSubWorkflowAlsoReturningTaskOrResult("nested workflow " + (i + 1), context, nestedWorkflowContexts.get(i), instructionsIfReplaying,
                            (w, e) -> {
                                throw new IllegalStateException("Sub workflow " + w + " is not replayable", e);
                            }, false);
                    if (check.getLeft()) {
                        task = (Task<Object>) check.getRight();
                    } else {
                        // completed, skip run; workflow output will be set and used by caller (so can ignore check.getRight() here)
                        task = null;
                        // unless we are reducing, in which case we need to track which one ran last
                        lastWorkflowRunBeforeReplay = nestedWorkflowContexts.get(i);
                    }
                } catch (Exception e) {
                    errors.add(e);
                    task = null;
                }
            }
            if (task != null) {
                if (Entities.isUnmanagingOrNoLongerManaged(context.getEntity())) {
                    // on shutdown don't keep running tasks
                    task.cancel(false);
                } else {
                    if (reducingV!=null) {
                        delayedBecauseReducing.add(Pair.of(nestedWorkflowContexts.get(i), task));
                    } else {
                        if (availableThreads != null) {
                            availableThreads.decrementAndGet();
                            ((EntityInternal) context.getEntity()).getExecutionContext().submit(MutableMap.of("newTaskEndCallback", (Runnable) () -> {
                                        availableThreads.incrementAndGet();
                                        synchronized (availableThreads) {
                                            availableThreads.notifyAll();
                                        }
                                    }),
                                    task);
                        }
                        DynamicTasks.queue(task);
                        submitted.add(task);
                    }
                }
            }
        }

        if (reducingV==null) {
            assert delayedBecauseReducing.isEmpty();
            submitted.forEach(t -> {
                try {
                    if (!t.isSubmitted() && !errors.isEmpty()) {
                        // if concurrent, all tasks will be submitted, and we should wait;
                        // if not, then there might be queued tasks not yet submitted; if there are errors, they will never be submitted
                        return;
                    }
                    t.get();
                } catch (Throwable tt) {
                    errors.add(tt);
                }
            });

            if (!errors.isEmpty()) {
                throw Exceptions.propagate("Error"+(errors.size()>1 ? "s" : "")+" running sub-workflow"+(nestedWorkflowContexts.size()>1 ? "s" : "")+" in "+context.getWorkflowStepReference(), errors);
            }

        } else {
            // if reducing we need to wrap each execution to get/set last values

            assert submitted.isEmpty();
            if (lastWorkflowRunBeforeReplay!=null) {
                // if interrupted we need to explicitly take from the last step
                reducingV = updateReducingWorkflowVarsFromLastStep(lastWorkflowRunBeforeReplay, reducingV);
            }
            for (Pair<WorkflowExecutionContext, Task<?>> p : delayedBecauseReducing) {
                WorkflowExecutionContext wc = p.getLeft();
                if (wc.getCurrentStepIndex()==null || wc.getCurrentStepIndex()==WorkflowExecutionContext.STEP_INDEX_FOR_START) {
                    // initialize to last if it hasn't started
                    wc.updateWorkflowScratchVariables(reducingV);
                }

                DynamicTasks.queue(p.getRight()).getUnchecked();

                reducingV = updateReducingWorkflowVarsFromLastStep(wc, reducingV);
            }
        }

        Object returnValue;
        if (reducingV==null) {
            List result = MutableList.of();
            nestedWorkflowContexts.forEach(nw -> result.add(nw.getOutput()));
            if (!wasList && result.size() != 1) {
                throw new IllegalStateException("Result mismatch, non-list target " + target + " yielded output " + result);
            }
            context.setOutput(result);
            returnValue = !wasList ? Iterables.getOnlyElement(result) : result;
        } else {
            context.setOutput(reducingV);
            context.getWorkflowExectionContext().updateWorkflowScratchVariables(reducingV);
            returnValue = reducingV;
        }

        return returnValue;
    }

    private static MutableMap<String, Object> updateReducingWorkflowVarsFromLastStep(WorkflowExecutionContext lastStep, Map<String, Object> prevWorkflowVars) {
        Map<String, Object> lastStepReducingWorkflowVars = lastStep.getWorkflowScratchVariables();
        Object lastStepOutput = lastStep.getOutput();
        MutableMap<String, Object> result = MutableMap.copyOf(prevWorkflowVars);
        prevWorkflowVars.keySet().forEach(k -> {
            result.put(k, lastStepReducingWorkflowVars.get(k));
            if (lastStepOutput instanceof Map && ((Map) lastStepOutput).containsKey(k)) {
                result.put(k, ((Map) lastStepOutput).get(k));
            }
        });
        return result;
    }

    protected Object getTargetFromString(WorkflowStepInstanceExecutionContext context, String target) {
        if ("children".equals(target)) return context.getEntity().getChildren();

        if ("members".equals(target)) {
            if (!(context.getEntity() instanceof Group))
                throw new IllegalArgumentException("Cannot specify target 'members' when not on a group");
            return ((Group) context.getEntity()).getMembers();
        }

        if (target.matches("-?[0-9]+\\.\\.-?[0-9]+")) {
            String[] numbers = target.split("\\.\\.");
            if (numbers.length==2) {
                int min = Integer.parseInt(numbers[0]);
                int max = Integer.parseInt(numbers[1]);
                if (min>max) throw new IllegalArgumentException("Invalid target range "+min+".."+max);
                List<Integer> result = MutableList.of();
                while (min<=max) result.add(min++);
                return result;
            }
        }

        throw new IllegalArgumentException("Invalid target '"+target+"'; if a string, it must match a known keyword ('children' or 'members') or pattern (a range, '1..10')");
    }

    public String getNameOrDefault() {
        return getNameOrDefault(() -> Strings.isNonBlank(shorthandTypeName) ? shorthandTypeName : "custom step");
    }
    public String getNameOrDefault(Supplier<String> defaultSupplier) {
        return Strings.isNonBlank(getName()) ? getName() : defaultSupplier==null ? null : defaultSupplier.get();
    }

    @Override
    public WorkflowStepDefinition applySpecialDefinition(ManagementContext mgmt, Object definition, String typeBestGuess, WorkflowStepDefinitionWithSpecialDeserialization firstParse) {
        // if we've resolved a custom workflow step, we need to make sure that the map supplied here
        // - doesn't set parameters
        // - doesn't set steps unless it is a simple `workflow` step (not a custom step)
        // - (also caller must not override shorthand definition, but that is explicitly removed by WorkflowStepResolution)
        // - has its output treated specially (workflow from output vs output when using this)
        BrooklynClassLoadingContext loader = RegisteredTypes.getCurrentClassLoadingContextOrManagement(mgmt);
        if (typeBestGuess==null || !(definition instanceof Map)) {
            throw new IllegalStateException("Should not be able to create a custom workflow definition from anything other than a map with a type");
        }
        CustomWorkflowStep result = (CustomWorkflowStep) firstParse;
        Map m = (Map)definition;
        for (String forbiddenKey: new String[] { "parameters" }) {
            if (m.containsKey(forbiddenKey)) {
                throw new IllegalArgumentException("Not permitted to override '" + forbiddenKey + "' when using a workflow step");
            }
        }
        if (!isPermittedToSetSteps(typeBestGuess)) {
            // custom workflow step
            for (String forbiddenKey : new String[]{"steps"}) {
                if (m.containsKey(forbiddenKey)) {
                    throw new IllegalArgumentException("Not permitted to override '" + forbiddenKey + "' when using a custom workflow step");
                }
            }
        }
        if (m.containsKey("output")) {
            // need to restore the workflow output from the base definition
            try {
                CustomWorkflowStep base = BeanWithTypeUtils.convert(mgmt, MutableMap.of("type", typeBestGuess), TypeToken.of(CustomWorkflowStep.class), true, loader, false);
                result.workflowOutput = base.output;
            } catch (JsonProcessingException e) {
                throw Exceptions.propagate(e);
            }
        } else {
            // if not specified in the definition, the output should be the workflow output
            result.workflowOutput = result.output;
            result.output = null;
        }
        return result;
    }

    protected boolean isPermittedToSetSteps(String typeBestGuess) {
        return typeBestGuess==null || SHORTHAND_TYPE_NAME_DEFAULT.equals(typeBestGuess) || CustomWorkflowStep.class.getName().equals(typeBestGuess);
    }

    protected WorkflowExecutionContext newWorkflow(WorkflowStepInstanceExecutionContext context, Object target, Integer targetIndexOrNull) {
        if (steps==null) throw new IllegalArgumentException("Cannot make new workflow with no steps");

        String indexName = targetIndexOrNull==null ? "" : " "+(targetIndexOrNull+1);
        String name = getNameOrDefault(null);
        name = (name == null ? "Sub-workflow" + indexName : "Sub-workflow"+indexName+" for " + name);
        String targetString = target==null ? null : target.toString();
        if (targetString!=null && targetString.length()<60 && !Strings.isMultiLine(targetString)) name += " ("+targetString+")";

        WorkflowExecutionContext nestedWorkflowContext = WorkflowExecutionContext.newInstanceUnpersistedWithParent(
                target instanceof BrooklynObject ? (BrooklynObject) target : context.getEntity(), context.getWorkflowExectionContext(),
                WorkflowExecutionContext.WorkflowContextType.NESTED_WORKFLOW,
                name,
                getConfigForSubWorkflow(false), null,
                ConfigBag.newInstance(getInput()), null);


        String tivn = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, target_index_var_name, String.class);
        if (targetIndexOrNull!=null) nestedWorkflowContext.updateWorkflowScratchVariable(tivn==null ? TARGET_INDEX_VAR_NAME_DEFAULT : tivn, targetIndexOrNull);
        initializeSubWorkflowForTarget(context, target, nestedWorkflowContext);

        return nestedWorkflowContext;
    }

    protected void initializeSubWorkflowForTarget(WorkflowStepInstanceExecutionContext context, Object target, WorkflowExecutionContext nestedWorkflowContext) {
        String tvn = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, target_var_name, String.class);
        nestedWorkflowContext.updateWorkflowScratchVariable(tvn==null ? TARGET_VAR_NAME_DEFAULT : tvn, target);
    }

    /** Returns a top-level workflow running the workflow defined here */
    public WorkflowExecutionContext newWorkflowExecution(Entity entity, String name, ConfigBag extraConfig) {
        return newWorkflowExecution(entity, name, extraConfig, null);
    }
    public WorkflowExecutionContext newWorkflowExecution(Entity entity, String name, ConfigBag extraConfig, Map extraTaskFlags) {
        if (steps==null) throw new IllegalArgumentException("Cannot make new workflow with no steps");

        if (target==null) {
            // copy everything as we are going to run it "flat"
            return WorkflowExecutionContext.newInstancePersisted(entity, WorkflowExecutionContext.WorkflowContextType.NESTED_WORKFLOW, name,
                    getConfigForSubWorkflow(true),
                    null,
                    ConfigBag.newInstance(getInput()).putAll(extraConfig), extraTaskFlags);
        } else {
            // if target specified, just reference the steps, the task below will trigger the block above, passing config through
            return WorkflowExecutionContext.newInstancePersisted(entity, WorkflowExecutionContext.WorkflowContextType.NESTED_WORKFLOW, name,
                    ConfigBag.newInstance()
                            .configure(WorkflowCommonConfig.PARAMETER_DEFS, parameters)
                            .configure(WorkflowCommonConfig.STEPS, MutableList.of(this)),
                    null,
                    ConfigBag.newInstance(getInput()).putAll(extraConfig), extraTaskFlags);
        }
    }

    private ConfigBag getConfigForSubWorkflow(boolean includeInput) {
        ConfigBag result = ConfigBag.newInstance()
                .configure(WorkflowCommonConfig.PARAMETER_DEFS, parameters)
                .configure(WorkflowCommonConfig.STEPS, steps)
                .configure(WorkflowCommonConfig.INPUT, includeInput ? input : null)  // input is resolved in outer workflow so it can reference outer workflow vars
                .configure(WorkflowCommonConfig.OUTPUT, workflowOutput)
                .configure(WorkflowCommonConfig.RETENTION, retention)
                .configure(WorkflowCommonConfig.REPLAYABLE, replayable)
                .configure(WorkflowCommonConfig.IDEMPOTENT, idempotent)
                .configure(WorkflowCommonConfig.ON_ERROR, onError)
                .configure(WorkflowCommonConfig.TIMEOUT, timeout)
                .configure(WorkflowCommonConfig.LOCK, lock)
                .configure((ConfigKey) WorkflowCommonConfig.CONDITION, target != null ? condition : null /* condition applies at subworkflow if target given */);
        MutableMap.copyOf(result.getAllConfigRaw()).forEach( (k,v) -> { if (v==null) result.remove(k); });
        return result;
    }

    @VisibleForTesting
    public List<Object> peekSteps() {
        return steps;
    }

    @Override protected Boolean isDefaultIdempotent() { return null; }
}
