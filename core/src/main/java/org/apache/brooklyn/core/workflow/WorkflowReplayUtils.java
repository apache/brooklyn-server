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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class WorkflowReplayUtils {

    private static final Logger log = LoggerFactory.getLogger(WorkflowReplayUtils.class);

    public static Integer findNearestReplayPoint(WorkflowExecutionContext context, int stepIndex) {
        int stepIndex0 = stepIndex;
        Set<Integer> considered = MutableSet.of();
        Set<Integer> possibleOthers = MutableSet.of();
        while (true) {
            if (WorkflowReplayUtils.isReplayableAtStep(context, stepIndex)) {
                break;
            }
            if (stepIndex == WorkflowExecutionContext.STEP_INDEX_FOR_START) {
                return null;
            }

            // look at the previous step
            WorkflowExecutionContext.OldStepRecord osi = context.oldStepInfo.get(stepIndex);
            if (osi == null) {
                log.warn("Unable to backtrack from step " + stepIndex + "; no step information. Will try to replay from start.");
                stepIndex = WorkflowExecutionContext.STEP_INDEX_FOR_START;
                continue;
            }

            Set<Integer> prev = osi.previous;
            if (prev == null || prev.isEmpty()) {
                log.warn("Unable to backtrack from step " + stepIndex + "; no previous step recorded. Will try to replay from start.");
                stepIndex = WorkflowExecutionContext.STEP_INDEX_FOR_START;
                continue;
            }

            boolean repeating = !considered.add(stepIndex);
            if (repeating) {
                if (possibleOthers.size() != 1) {
                    log.warn("Unable to backtrack from step " + stepIndex + "; ambiguous precedents " + prev + " / " + possibleOthers + ". Will try to replay from start.");
                    stepIndex = WorkflowExecutionContext.STEP_INDEX_FOR_START;
                    continue;
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
        return stepIndex;
    }

    public enum ReplayableOption {
        OFF, ON, YES, NO, TRUE, FALSE;
        // where a step has nested workflow,
        // if it is replaying from an explicit step
        // - all previous children workflows are marked as old
        // - new workflows are started
        // if it is replaying from latest
        // - if all children are replayable, it simply replays the children, then collects output
        // - if some children not replayable, it recomputes set of children and
        //   - skips those which are pre-existing and completed
        //   - replays from latest those which are pre-existing and replayable
        //   - marks others as old
        //   - replays others

        // supporting 'true' as a boolean and string "on" is tedious
        @JsonCreator
        public static ReplayableOption fromObject(Object b) {
            if (b instanceof ReplayableOption) return (ReplayableOption) b;
            if (b==null) return null;
            return fromString(b.toString());
        }

        public static ReplayableOption fromBoolean(boolean b) {
            if (b) return YES; else return NO;
        }
        public static ReplayableOption fromString(String s) {
            for (ReplayableOption opt: ReplayableOption.values()) {
                if (opt.name().equalsIgnoreCase(s)) return opt;
            }
            throw new IllegalArgumentException("Invalid replayabout option '"+s+"'");
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WorkflowReplayRecord {
        String taskId;
        String reasonForReplay;
        String submittedByTaskId;
        long submitTimeUtc;
        long startTimeUtc;
        long endTimeUtc;
        String status;
        Boolean isError;
        Object result;

        private static void add(WorkflowExecutionContext ctx, Task<?> task, String reasonForReplay) {
            WorkflowReplayRecord wrr = new WorkflowReplayRecord();
            wrr.taskId = task.getId();
            wrr.reasonForReplay = reasonForReplay;
            ctx.replays.add(wrr);
            ctx.replayCurrent = wrr;
            update(ctx, task);
        }

        private static void updateInternal(WorkflowExecutionContext ctx, Task<?> task, Boolean forceEndSuccessOrError, Object result) {
            if (ctx.replayCurrent ==null || ctx.replayCurrent.taskId!=task.getId()) {
                log.warn("Mismatch in workflow replays for "+ctx+": "+ctx.replayCurrent +" vs "+task);
                return;
            }

            // try hard to get submitter data in case tasks go awol before execution
            if (task.getSubmittedByTaskId()!=null) {
                ctx.replayCurrent.submittedByTaskId = task.getSubmittedByTaskId();
            } else if (ctx.replayCurrent.submittedByTaskId==null && Tasks.current()!=null && !Tasks.current().equals(task)) {
                ctx.replayCurrent.submittedByTaskId = Tasks.current().getId();
            }
            ctx.replayCurrent.submitTimeUtc = task.getSubmitTimeUtc();
            // fake this because we won't see the real value until we also see the start value.
            // however we need to ensure any workflow that is created is intended to be run.
            if (ctx.replayCurrent.submitTimeUtc<=0) ctx.replayCurrent.submitTimeUtc = Instant.now().toEpochMilli();

            ctx.replayCurrent.startTimeUtc = task.getStartTimeUtc();
            ctx.replayCurrent.endTimeUtc = task.getEndTimeUtc();
            ctx.replayCurrent.status = task.getStatusSummary();

            if (forceEndSuccessOrError==null) {
                ctx.replayCurrent.isError = task.isDone() ? task.isError() : null;
                try {
                    ctx.replayCurrent.result = task.isDone() ? task.get() : null;
                } catch (Throwable t) {
                    ctx.replayCurrent.result = Exceptions.collapseTextInContext(t, task);
                }
            } else {
                // when forcing end, we are invoked _by_ the task so we fake the completion information
                if (ctx.replayCurrent.endTimeUtc <= 0) {
                    ctx.replayCurrent.endTimeUtc = System.currentTimeMillis();
                    ctx.replayCurrent.status = forceEndSuccessOrError ? "Completed" : "Failed";
                }
                ctx.replayCurrent.isError = !forceEndSuccessOrError;
                ctx.replayCurrent.result = result;
            }
        }

        private static void update(WorkflowExecutionContext ctx, Task<?> task) {
            updateInternal(ctx, task, null, null);
        }
    }

    /** called when the task is being created */
    public static void updateOnWorkflowStartOrReplay(WorkflowExecutionContext ctx, Task<?> task, String reasonForReplay, boolean fixedStepReplay) {
        WorkflowReplayRecord.add(ctx, task, reasonForReplay);

        if (ctx.replayableCurrentSetting == null) ctx.replayableCurrentSetting = ReplayableOption.OFF;

        if (fixedStepReplay) {
            // if an explicit step replays, this needs to be reset; otherwise leave it unset (if new) or unchanged (if replay from last)
            ctx.replayableLastStep = ctx.replayableFromStart ? -1 : null;
        }
    }

    public static void updateOnWorkflowSuccess(WorkflowExecutionContext ctx, Task<?> task, Object result) {
        WorkflowReplayRecord.updateInternal(ctx, task, true, result);
        ctx.replayableLastStep = WorkflowExecutionContext.STEP_INDEX_FOR_END;
    }

    public static void updateOnWorkflowError(WorkflowExecutionContext ctx, Task<?> task, Throwable error) {
        WorkflowReplayRecord.updateInternal(ctx, task, false, Exceptions.collapseTextInContext(error, task));
        // no change to last replayable step
    }

    /** called when the workflow task is starting to run, after WorkflowStartOrReplay */
    public static void updateOnWorkflowTaskStartupOrReplay(WorkflowExecutionContext ctx, Task<?> task, List<WorkflowStepDefinition> stepsResolved, boolean firstRun, Integer optionalReplayStep) {
        WorkflowReplayRecord.updateInternal(ctx, task, null, null);

        if (firstRun) {
            // set the workflow replayable status, overriding on the basis of the first step (in case they didn't set it on the workflow but did on the first step)
            ctx.replayableFromStart = isReplayable(ctx.replayableCurrentSetting);
            if (!ctx.replayableFromStart && !stepsResolved.isEmpty()) {
                ctx.replayableFromStart = isReplayable(stepsResolved.get(0).replayable);
                ctx.replayableLastStep = ctx.replayableFromStart ? -1 : null;
            }
        }

        // set workflow replayable default value state, in step context
        if (!firstRun && optionalReplayStep!=null) {
            // user has requested to replay from a given step, or effectively that
            WorkflowExecutionContext.OldStepRecord oldStep = ctx.oldStepInfo.get(optionalReplayStep);
            if (oldStep==null) {
                // shouldn't happen; possibly an init error
            } else {
                ctx.replayableCurrentSetting = oldStep.replayableCurrentSetting;
            }
        }
    }

    public static void updateOnWorkflowStepChange(WorkflowExecutionContext.OldStepRecord currentStepRecord, WorkflowStepInstanceExecutionContext ctx, WorkflowStepDefinition step) {
        // if new step is replayable, mark it as last
        ReplayableOption r = step.getReplayable();
        boolean isReplayableHere;
        if (r!=null) {
            isReplayableHere = isReplayable(r);
            ctx.context.replayableCurrentSetting = r;
        } else {
            r = ctx.context.replayableCurrentSetting;
            isReplayableHere = r==ReplayableOption.ON;
        }
        currentStepRecord.replayableCurrentSetting = r;
        currentStepRecord.replayableFromHere = isReplayableHere;
        if (isReplayableHere) ctx.context.replayableLastStep = ctx.context.currentStepIndex;
    }

    public static boolean isReplayable(ReplayableOption setting) {
        return setting==ReplayableOption.ON || setting==ReplayableOption.YES || setting==ReplayableOption.TRUE;
    }

    public static boolean isReplayableAtLast(WorkflowExecutionContext workflowExecutionContext, boolean requireDeeplyReplayable) {
        WorkflowStepInstanceExecutionContext csi = workflowExecutionContext.currentStepInstance;
        if (csi !=null) {
            WorkflowStepDefinition stepDefinition = workflowExecutionContext.getStepsResolved().get(csi.stepIndex);
            if (stepDefinition instanceof WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow) {
                List<WorkflowExecutionContext> subWorkflows = ((WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow) stepDefinition).getSubWorkflowsForReplay(csi, false, true);
                if (subWorkflows==null) return false;
                if (!requireDeeplyReplayable) return true;
                return subWorkflows.stream().allMatch(sub -> isReplayableAtLast(sub, requireDeeplyReplayable));
            } else {
                return isReplayableAtStep(workflowExecutionContext, csi.stepIndex);
            }
        }
        if (workflowExecutionContext.currentStepIndex!=null) {
            return isReplayableAtStep(workflowExecutionContext, workflowExecutionContext.currentStepIndex);
        }

        return isReplayableAtStep(workflowExecutionContext, WorkflowExecutionContext.STEP_INDEX_FOR_START);
    }

    public static boolean isReplayableAtStep(WorkflowExecutionContext workflowExecutionContext, int stepIndex) {
        if (stepIndex==WorkflowExecutionContext.STEP_INDEX_FOR_START) {
            return workflowExecutionContext.replayableFromStart;
        }

        WorkflowExecutionContext.OldStepRecord osi = workflowExecutionContext.oldStepInfo.get(stepIndex);
        if (osi!=null && osi.replayableCurrentSetting!=null) {
            return osi.replayableFromHere;
        }

        // if no step info so hasn't even started to play yet (will be updated before start)
        return false;
    }

    public static boolean isReplayableAnywhere(WorkflowExecutionContext workflowExecutionContext) {
        return isReplayableAtLast(workflowExecutionContext, true)
                || workflowExecutionContext.replayableLastStep!=null
                || isReplayableAtStep(workflowExecutionContext, WorkflowExecutionContext.STEP_INDEX_FOR_START);
    }

    /** creates a task to replay the subworkflow, returning it, or null if the workflow completed successfully, or throwing if the workflow cannot be replayed */
    private static Task<Object> createReplaySubWorkflowTaskOrThrow(WorkflowExecutionContext subWorkflow, WorkflowStepDefinition.ReplayContinuationInstructions instructions) {
        if (instructions.stepToReplayFrom!=null) {
            // shouldn't come here
            throw new IllegalStateException("Cannot replay a nested workflow where the parent started at a specific step");
        } else if (instructions.forced && instructions.customBehavior !=null) {
            // forced, eg throwing exception
            log.debug("Creating task to replay subworkflow " + subWorkflow+" from last, forced with custom behaviour - "+instructions);
            return subWorkflow.createTaskReplaying(subWorkflow.makeInstructionsForReplayingLastForcedWithCustom(instructions.customBehaviorExplanation, instructions.customBehavior));
        } else {
            if (Objects.equals(subWorkflow.replayableLastStep, WorkflowExecutionContext.STEP_INDEX_FOR_END)) {
                log.debug("Creating task to replay subworkflow " + subWorkflow+" from last, but already at end - "+instructions);
                return null;
            }
            // may throw if not forced and not replayable
            log.debug("Creating task to replay subworkflow " + subWorkflow+" from last: "+instructions);
            WorkflowStepDefinition.ReplayContinuationInstructions subInstr = subWorkflow.makeInstructionsForReplayingLast(instructions.customBehaviorExplanation, instructions.forced);
            log.debug("Creating task to replay subworkflow " + subWorkflow+", will use: "+subInstr);

            return subWorkflow.createTaskReplaying(subInstr);
        }
    }

    /** replays the workflow indicated by the set, returning the result, or if not possible creates a new task */
    public static Object replayInSubWorkflow(String summary, WorkflowStepInstanceExecutionContext context, WorkflowExecutionContext w, WorkflowStepDefinition.ReplayContinuationInstructions instructions, BiFunction<WorkflowExecutionContext,Exception,Object> ifNotReplayable) {
        Pair<Boolean, Object> check = checkReplayInSubWorkflowAlsoReturningTaskOrResult(summary, context, w, instructions, ifNotReplayable);
        if (check.getLeft()) return DynamicTasks.queue((Task<?>) check.getRight()).getUnchecked();
        else return check.getRight();
    }

    public static Pair<Boolean,Object> checkReplayInSubWorkflowAlsoReturningTaskOrResult(String summary, WorkflowStepInstanceExecutionContext context, WorkflowExecutionContext w, WorkflowStepDefinition.ReplayContinuationInstructions instructions, BiFunction<WorkflowExecutionContext,Exception,Object> ifNotReplayable) {
        Task<Object> t;
        try {
            t = WorkflowReplayUtils.createReplaySubWorkflowTaskOrThrow(w, instructions);
            if (t == null) {
                // subworkflow completed
                return Pair.of(false, w.getOutput());
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.debug("Step " + context.getWorkflowStepReference() + " could not resume nested workflow " + w.getWorkflowId() + " (running anew): "+e);

            return Pair.of(false, ifNotReplayable.apply(w, e));
        }

        log.debug("Step " + context.getWorkflowStepReference() + " resuming nested workflow " + w.getWorkflowId() + " in task " + t.getId());
        return Pair.of(true, t);
    }

    public static void setNewSubWorkflows(WorkflowStepInstanceExecutionContext context, List<BrooklynTaskTags.WorkflowTaskTag> tags, String supersededId) {
        // make sure parent knows about child before child workflow is persisted, otherwise there is a chance the child workflow gets orphaned (if interrupted before parent persists)
        // supersede old and save the new sub-workflow ID before submitting it, and before child knows about parent, per invoke effector step notes
        context.getSubWorkflows().forEach(tag -> tag.setSupersededByTaskId(supersededId));
        tags.forEach(nw ->  context.getSubWorkflows().add(nw));
        context.getWorkflowExectionContext().persist();
    }

    public static List<WorkflowExecutionContext> getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, boolean forced, boolean peekingOnly) {
        Set<BrooklynTaskTags.WorkflowTaskTag> sws = context.getSubWorkflows();
        if (sws!=null && !sws.isEmpty()) {
            // replaying

            List<WorkflowExecutionContext> nestedWorkflowsToReplay = sws.stream().filter(tag -> tag.getSupersededByTaskId()==null)
                    .map(tag -> {
                        Entity targetEntity = (Entity) context.getManagementContext().lookup(tag.getEntityId());
                        if (targetEntity == null) {
                            log.warn("Unable to find entity for sub-workflow "+tag+" in "+context+"; assuming entity has gone, and may trigger recomputation of targets");
                            return null;
                        }
                        return new WorkflowStatePersistenceViaSensors(context.getManagementContext()).getWorkflows(targetEntity).get(tag.getWorkflowId());
                    }).collect(Collectors.toList());

            if (nestedWorkflowsToReplay.isEmpty()) {
                if (!peekingOnly) log.info("Step "+context.getWorkflowStepReference()+" has all sub workflows superseded; replaying from start");
                if (!peekingOnly) log.debug("Step "+context.getWorkflowStepReference()+" superseded sub workflows detail: "+sws+" -> "+nestedWorkflowsToReplay);
                // fall through to below
            } else if (nestedWorkflowsToReplay.contains(null)) {
                if (!peekingOnly) log.info("Step "+context.getWorkflowStepReference()+" has uninitialized sub workflows; replaying from start");
                if (!peekingOnly) log.debug("Step "+context.getWorkflowStepReference()+" uninitialized/unpersisted sub workflow detail: "+sws+" -> "+nestedWorkflowsToReplay);
                // fall through to below
            } else if (!forced && nestedWorkflowsToReplay.stream().anyMatch(nest -> !isReplayableAnywhere(nest))) {
                if (!peekingOnly) log.info("Step "+context.getWorkflowStepReference()+" has non-replayable sub workflows; replaying from start");
                if (!peekingOnly) log.debug("Step "+context.getWorkflowStepReference()+" non-replayable sub workflow detail: "+sws+" -> "+nestedWorkflowsToReplay);
                // fall through to below
            } else {

                if (!peekingOnly) log.debug("Step "+context.getWorkflowStepReference()+" replay sub workflow detail: "+sws+" -> "+nestedWorkflowsToReplay);
                return nestedWorkflowsToReplay;
            }
        }
        return null;
    }
}
