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

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class WorkflowReplayUtils {

    private static final Logger log = LoggerFactory.getLogger(WorkflowReplayUtils.class);

    public enum ReplayableOption {
        OFF, ON, YES, NO
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
    }

    public static class WorkflowReplayRecord {
        String taskId;
        String reasonForReplay;
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
            ctx.replayCurrent.submitTimeUtc = task.getSubmitTimeUtc();
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
                if (ctx.replayCurrent.endTimeUtc <= 0) ctx.replayCurrent.endTimeUtc = System.currentTimeMillis();
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
        ctx.replayableLastStep = -2;
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
        return setting==ReplayableOption.ON || setting==ReplayableOption.YES;
    }

    public static boolean isReplayable(WorkflowExecutionContext workflowExecutionContext, Integer stepIndex) {
        if (stepIndex==null || stepIndex==-1) {
            return workflowExecutionContext.replayableFromStart;
        }

        WorkflowExecutionContext.OldStepRecord osi = workflowExecutionContext.oldStepInfo.get(stepIndex);
        if (osi!=null && osi.replayableCurrentSetting!=null) {
            return osi.replayableFromHere;
        }
        return isReplayable(workflowExecutionContext, null);
    }


    /** creates a task to replay the subworkflow, returning it, or null if the workflow completed successfully, or throwing if the workflow cannot be replayed */
    private static Task<Object> replaySubWorkflow(WorkflowExecutionContext subWorkflow, WorkflowStepDefinition.ReplayContinuationInstructions instructions) {
        if (instructions.stepToReplayFrom!=null) {
            // shouldn't come here
            throw new IllegalStateException("Cannot replay a nested workflow where the parent started at a specific step");
        } else if (instructions.customBehaviour!=null) {
            // forced, eg throwing exception
            return subWorkflow.createTaskReplaying(subWorkflow.makeInstructionsForReplayingLastForcedWithCustom(instructions.customBehaviourExplanation, instructions.customBehaviour));
        } else {
            if (Objects.equals(subWorkflow.replayableLastStep, -2)) return null;
            // may throw if not forced and not replayable
            return subWorkflow.createTaskReplaying(subWorkflow.makeInstructionsForReplayingLast(instructions.customBehaviourExplanation, instructions.forced));
        }
    }

    /** replays the workflow indicated by the set, returning the result, or if not possible creates a new task */
    public static Object replayInSubWorkflow(String summary, WorkflowStepInstanceExecutionContext context, List<WorkflowExecutionContext> subworkflows, WorkflowStepDefinition.ReplayContinuationInstructions instructions, Supplier<Object> ifNotReplayable) {
        // TODO handle multiple workflows
        WorkflowExecutionContext w = Iterables.getOnlyElement(subworkflows);

        Task<Object> t;
        try {
            t = WorkflowReplayUtils.replaySubWorkflow(w, instructions);
            if (t == null) {
                // subworkflow completed
                return w.getOutput();
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.debug("Step " + context.getWorkflowStepReference() + " could not resume nested workflow " + w.getWorkflowId() + ", running again: "+e);

            return ifNotReplayable.get();
        }

        log.debug("Step " + context.getWorkflowStepReference() + " resuming nested workflow " + w.getWorkflowId() + " in task " + t.getId());
        return DynamicTasks.queue(t).getUnchecked();
    }
}
