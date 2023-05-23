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

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.collections.MutableSet;
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

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class WorkflowReplayUtils {

    private static final Logger log = LoggerFactory.getLogger(WorkflowReplayUtils.class);


    public static boolean isReplayResumable(WorkflowExecutionContext workflowExecutionContext, boolean requireDeeplyReplayable, boolean allowInternallyEvenIfDisabled) {
        WorkflowStepInstanceExecutionContext csi = workflowExecutionContext.currentStepInstance;
        if (csi!=null) {
            if (csi.getStepIndex()!=workflowExecutionContext.currentStepIndex) {
                // always resumable between steps
                return true;
            }

            WorkflowStepDefinition stepDefinition = workflowExecutionContext.getStepsResolved().get(csi.stepIndex);
            if (stepDefinition!=null) {
                Boolean idempotence = stepDefinition.isIdempotent(csi);

                // 'no' is (currently) not overridable by auto-detection, but `yes` is.
                if (Boolean.FALSE.equals(idempotence)) return false;

                // if 'yes' or null, we check.
                if (stepDefinition instanceof WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow) {
                    List<WorkflowExecutionContext> subWorkflows = ((WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow) stepDefinition).getSubWorkflowsForReplay(csi, false, true, allowInternallyEvenIfDisabled);
                    if (subWorkflows == null) return false;
                    if (!requireDeeplyReplayable) return true;
                    return subWorkflows.stream().allMatch(sub -> isReplayResumable(sub, requireDeeplyReplayable, allowInternallyEvenIfDisabled));
                }

                if (idempotence!=null) return idempotence;

                // shouldn't come here
                return false;

            } else {
                return isReplayableFromStep(workflowExecutionContext, csi.stepIndex);
            }
        } else {
            // if between steps or at start or end, can use replayable from step
            return isReplayableFromStep(workflowExecutionContext, workflowExecutionContext.currentStepIndex);
        }
    }

    static boolean isReplayableFromStep(WorkflowExecutionContext workflowExecutionContext, Integer stepIndex) {
        if (stepIndex==null || stepIndex==WorkflowExecutionContext.STEP_INDEX_FOR_START) {
            return (Boolean.TRUE.equals(workflowExecutionContext.replayableFromStart) || Objects.equals(workflowExecutionContext.replayableLastStep, WorkflowExecutionContext.STEP_INDEX_FOR_START));
        }
        if (stepIndex==WorkflowExecutionContext.STEP_INDEX_FOR_END) {
            // always "resumable" at end
            return true;
        }
        if (Objects.equals(stepIndex, workflowExecutionContext.currentStepIndex) && Objects.equals(workflowExecutionContext.replayableLastStep, stepIndex)) {
            return true;
        }

        WorkflowExecutionContext.OldStepRecord osi = workflowExecutionContext.oldStepInfo.get(stepIndex);
        if (osi!=null) {
            return Boolean.TRUE.equals(osi.replayableFromHere);
        }

        // no step info so hasn't even been registered yet (will be updated before start, but shouldn't come here)
        return false;
    }

    public static boolean isReplayableAnywhere(WorkflowExecutionContext workflowExecutionContext, boolean allowInternallyEvenIfDisabled) {
        if (workflowExecutionContext.factory(allowInternallyEvenIfDisabled).isDisabled()) return false;
        return workflowExecutionContext.currentStepIndex==null
                || workflowExecutionContext.replayableLastStep !=null
                || Boolean.TRUE.equals(workflowExecutionContext.replayableFromStart)
                || (workflowExecutionContext.currentStepInstance!=null && workflowExecutionContext.currentStepInstance.getStepIndex()!=workflowExecutionContext.currentStepIndex)
                || workflowExecutionContext.currentStepIndex==WorkflowExecutionContext.STEP_INDEX_FOR_START
                || workflowExecutionContext.currentStepIndex==WorkflowExecutionContext.STEP_INDEX_FOR_END
                || isReplayResumable(workflowExecutionContext, true, allowInternallyEvenIfDisabled);
    }

    /** throws error if any argument non-blank invalid; null if nothing to do; otherwise a consumer which will initialize the WEC */
    public static Consumer<WorkflowExecutionContext> updaterForReplayableAtWorkflow(String replayable, String idempotent, boolean isNestedWorkflowStep) {
        if (replayable==null) replayable = "";
        replayable = replayable.toLowerCase().replaceAll("[^a-z]+", " ").trim();

        if (idempotent==null) idempotent = "";
        idempotent = idempotent.toLowerCase().replaceAll("[^a-z]+", " ").trim();
        boolean idempotentAll = "all".equals(idempotent);
        if (idempotentAll) idempotent = "";

        if (!Strings.isBlank(idempotent)) throw new IllegalArgumentException("Invalid value for `idemopotent` on workflow step");

        // replayable:
        //
        //`enabled` (the default):  is is permitted to replay resuming wherever the workflow fails on idempotent steps or where there are explicit replay points
        //`disabled`:  it is not permitted for callers to replay the workflow, whether operator-driven or automatic; resumable steps and replay points in the workflow are not externally visible (but may still be used by replays triggered within the workflow)
        //`from start`:  the workflow start is a replay point
        //`automatically`: indicates that on an unhandled Brooklyn failover (DanglingWorkflowException), the workflow should attempt to replay resuming; implies `enabled`,
        // can be combined with `from start`

        boolean replayableAutomatically = (replayable.contains("automatically"));
        if (replayableAutomatically) replayable = replayable.replace("automatically", "").trim();

        if (!replayableAutomatically && replayable.equals("enabled")) { replayable = ""; }

        boolean replayableDisabled = !replayableAutomatically && replayable.equals("disabled");
        if (replayableDisabled) replayable = "";

        boolean replayableFromStart = replayable.equals("from start");
        if (replayableFromStart) replayable = "";

        if (!Strings.isBlank(replayable)) {
            if (!replayableAutomatically && isNestedWorkflowStep)
                validateReplayableAndIdempotentAtStep(replayable, idempotent, false);
            else if (replayableAutomatically)
                throw new IllegalArgumentException("Invalid 'replayable' value: 'automatically' cannot be used with '"+replayable+"'");
            else
                throw new IllegalArgumentException("Invalid 'replayable' value: '"+replayable+"'");
        }

        return ctx -> {
            if (replayableFromStart) {
                ctx.replayableFromStart = replayableFromStart;
                ctx.replayableLastStep = WorkflowExecutionContext.STEP_INDEX_FOR_START;
            }
            ctx.replayableAutomatically = replayableAutomatically ? true : null;
            ctx.replayableDisabled = replayableDisabled ? true : null;
            ctx.idempotentAll = idempotentAll ? true : null;
        };
    }

    public static Consumer<WorkflowExecutionContext> updaterForReplayableAtWorkflow(ConfigBag paramsDefiningWorkflow, boolean isNestedWorkflowStep) {
        return updaterForReplayableAtWorkflow(paramsDefiningWorkflow.get(WorkflowCommonConfig.REPLAYABLE), paramsDefiningWorkflow.get(WorkflowCommonConfig.IDEMPOTENT), isNestedWorkflowStep);
    }

    public static void updateReplayableFromStep(WorkflowExecutionContext context, WorkflowStepDefinition step) {
        Pair<ReplayableAtStepOption, Boolean> opt = step.validateReplayableAndIdempotent();

        // idempotence on RHS ignored here, considered when we find the last

        if (opt.getLeft()!=null && opt.getLeft().isReplayPoint) {
            context.oldStepInfo.get(context.currentStepIndex).replayableFromHere = true;
            context.replayableLastStep = context.currentStepIndex;
        }
        if (opt.getLeft()!=null && opt.getLeft().forcesReset) {
            context.replayableLastStep = null;
            context.oldStepInfo.forEach( (k,v) -> {
                v.replayableFromHere = (opt.getLeft().isReplayPoint && Objects.equals(k, context.currentStepIndex)) ? true : null;
            });
        }
    }

    public enum ReplayableAtStepOption {
        RESET(false, true), FROM_HERE(true, false), FROM_HERE_ONLY(true, true);

        private final boolean isReplayPoint;
        private final boolean forcesReset;

        ReplayableAtStepOption(boolean isReplayPoint, boolean forcesReset) {
            this.isReplayPoint = isReplayPoint;
            this.forcesReset = forcesReset;
        }

        public static Maybe<ReplayableAtStepOption> ofMaybe(String replayable) {
            if (Strings.isBlank(replayable)) return Maybe.absentNull();
            if ("reset".equalsIgnoreCase(replayable)) return Maybe.of(RESET);
            if ("from here".equalsIgnoreCase(replayable)) return Maybe.of(FROM_HERE);
            if ("from here only".equalsIgnoreCase(replayable)) return Maybe.of(FROM_HERE_ONLY);
            return Maybe.absent("Invalid 'replayable' value: "+replayable);
        }
    }

    public static Pair<ReplayableAtStepOption,Boolean> validateReplayableAndIdempotentAtStep(String replayable, String idempotent, boolean asWorkflowDefinition) {
        if (replayable==null) replayable = "";
        if (idempotent==null) idempotent = "";
        replayable = replayable.toLowerCase().replaceAll("[^a-z]+", " ").trim();
        idempotent = idempotent.toLowerCase().replaceAll("[^a-z]+", " ").trim();

        Maybe<ReplayableAtStepOption> rv = Strings.isBlank(replayable) ? Maybe.ofAllowingNull(null) : ReplayableAtStepOption.ofMaybe(replayable);

        Maybe<Boolean> id = validateIdempotentAtStep(idempotent);

        if (asWorkflowDefinition) {
            // this will through exception if unacceptable
            updaterForReplayableAtWorkflow(rv.isPresent() ? null : replayable, id.isPresent() ? null : idempotent, false);
        } else {
            rv.get();
            id.get();
        }

        return Pair.of(rv.orNull(), id.orNull());
    }

    private static Maybe<Boolean> validateIdempotentAtStep(String idempotent) {
        if (Strings.isBlank(idempotent) || idempotent.equals("default")) return Maybe.ofAllowingNull(null);
        if (idempotent.equals("yes") || idempotent.equals("true")) return Maybe.of(true);
        if (idempotent.equals("no") || idempotent.equals("false")) return Maybe.of(false);
        return Maybe.absent("Invalid value for idempotent: '"+idempotent+"'");
    }

    public static Integer findNearestReplayPoint(final WorkflowExecutionContext context, final int stepIndex0) {
        return findNearestReplayPoint(context, stepIndex0, true);
    }
    public static Integer findNearestReplayPoint(final WorkflowExecutionContext context, final int stepIndex0, boolean allowInclusive) {
        int stepIndex = stepIndex0;
        Set<Integer> considered = MutableSet.of();
        Set<Integer> possibleOthers = MutableSet.of();
        while (true) {
            if (allowInclusive && WorkflowReplayUtils.isReplayableFromStep(context, stepIndex)) {
                break;
            }
            allowInclusive = true;

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

        public String getTaskId() {
            return taskId;
        }

        @Override
        public String toString() {
            return super.toString()+"[task="+taskId+"]";
        }
    }

    /** called when the task is being created */
    public static void updateOnWorkflowStartOrReplay(WorkflowExecutionContext ctx, Task<?> task, String reasonForReplay, Integer fixedStepToReplayFrom) {
        WorkflowReplayRecord.add(ctx, task, reasonForReplay);

        if (fixedStepToReplayFrom!=null) {
            // if an explicit step for replay given, that becomes the last step to replay from
            ctx.replayableLastStep = fixedStepToReplayFrom;
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
    }

    /** creates a task to replay the subworkflow, returning it, or null if the workflow completed successfully, or throwing if the workflow cannot be replayed */
    private static Task<Object> createReplayResumingSubWorkflowTaskOrThrow(WorkflowExecutionContext subWorkflow, WorkflowStepDefinition.ReplayContinuationInstructions instructions, boolean allowInternallyEvenIfDisabled) {
        if (instructions.stepToReplayFrom!=null) {
            // shouldn't come here
            throw new IllegalStateException("Cannot replay a nested workflow where the parent started at a specific step");
        } else if (instructions.forced && instructions.customBehavior !=null) {
            // forced, eg throwing exception
            log.debug("Creating task to replay subworkflow " + subWorkflow+" from last, forced with custom behaviour - "+instructions);
            return subWorkflow.factory(allowInternallyEvenIfDisabled).createTaskReplaying(subWorkflow.factory(allowInternallyEvenIfDisabled).makeInstructionsForReplayResumingForcedWithCustom(instructions.customBehaviorExplanation, instructions.customBehavior));
        } else {
            if (Objects.equals(subWorkflow.replayableLastStep, WorkflowExecutionContext.STEP_INDEX_FOR_END)) {
                log.debug("Creating task to replay subworkflow " + subWorkflow+" from last, but already at end - "+instructions);
                return null;
            }
            // may throw if not forced and not replayable
            log.debug("Creating task to replay subworkflow " + subWorkflow+" from last: "+instructions);
            WorkflowStepDefinition.ReplayContinuationInstructions subInstr = subWorkflow.factory(allowInternallyEvenIfDisabled).makeInstructionsForReplayResuming(instructions.customBehaviorExplanation, instructions.forced);
            log.debug("Creating task to replay subworkflow " + subWorkflow+", will use: "+subInstr);

            return subWorkflow.factory(allowInternallyEvenIfDisabled).createTaskReplaying(subInstr);
        }
    }

    /** replays the workflow indicated by the set, returning the result, or if not possible creates a new task */
    public static Object replayResumingInSubWorkflow(String summary, WorkflowStepInstanceExecutionContext context, WorkflowExecutionContext w, WorkflowStepDefinition.ReplayContinuationInstructions instructions, BiFunction<WorkflowExecutionContext,Exception,Object> ifNotReplayable, boolean allowInternallyEvenIfDisabled) {
        Pair<Boolean, Object> check = checkReplayResumingInSubWorkflowAlsoReturningTaskOrResult(summary, context, w, instructions, ifNotReplayable, allowInternallyEvenIfDisabled);
        if (check.getLeft()) return DynamicTasks.queue((Task<?>) check.getRight()).getUnchecked();
        else return check.getRight();
    }

    public static Pair<Boolean,Object> checkReplayResumingInSubWorkflowAlsoReturningTaskOrResult(String summary, WorkflowStepInstanceExecutionContext context, WorkflowExecutionContext w, WorkflowStepDefinition.ReplayContinuationInstructions instructions, BiFunction<WorkflowExecutionContext,Exception,Object> ifNotReplayable, boolean allowInternallyEvenIfDisabled) {
        Task<Object> t;
        try {
            t = WorkflowReplayUtils.createReplayResumingSubWorkflowTaskOrThrow(w, instructions, allowInternallyEvenIfDisabled);
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
        markSubWorkflowsSupersededByTask(context, supersededId);
        tags.forEach(nw -> addNewSubWorkflow(context, nw));
        context.getWorkflowExectionContext().persist();
    }

    public static void markSubWorkflowsSupersededByTask(WorkflowStepInstanceExecutionContext context, String supersededId) {
        context.getSubWorkflows().forEach(tag -> tag.setSupersededByTaskId(supersededId));
    }

    public static boolean addNewSubWorkflow(WorkflowStepInstanceExecutionContext context, BrooklynTaskTags.WorkflowTaskTag nw) {
        return context.getSubWorkflows().add(nw);
    }

    public static List<WorkflowExecutionContext> getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, boolean forced, boolean peekingOnly, boolean allowInternallyEvenIfDisabled) {
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
            } else if (!forced && nestedWorkflowsToReplay.stream().anyMatch(nest -> !isReplayableAnywhere(nest, allowInternallyEvenIfDisabled))) {
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

    public static Object getNext(Object ...sources) {
        Object result = null;

        for (Object o: sources) {
            if (o==null) continue;
            if (o instanceof WorkflowStepInstanceExecutionContext) result = ((WorkflowStepInstanceExecutionContext)o).next;
            else if (o instanceof WorkflowStepDefinition) result = ((WorkflowStepDefinition)o).next;
            else if (o instanceof String || o instanceof WorkflowStepDefinition.ReplayContinuationInstructions) result = o;
            else throw new IllegalArgumentException("Next not supported for "+o+" (type "+o.getClass()+")");

            if (result!=null) break;
        }
        return result;
    }
}
