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
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.steps.flow.FailWorkflowStep.WorkflowFailException;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.brooklyn.core.workflow.WorkflowExecutionContext.*;

public class WorkflowErrorHandling implements Callable<WorkflowErrorHandling.WorkflowErrorHandlingResult> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowErrorHandling.class);

    /*
     * ui for error handling
     *
     * step task's workflow tag will have ERROR_HANDLED_BY single-key map tag pointing at handler parent task, created in this method.
     * error handler parent task will have 'errorHandlerForTask' field in the workflow tag pointing at step task, but no errorHandlerIndex.
     * if any of the handler steps match, the parent will have ERROR_HANDLED_BY pointing at it, and will have a task with the workflow tag with
     * 'errorHandlerForTask' field pointing at the parent and also 'errorHandlerIndex' set to the index in the step's error list, but not ERROR_HANDLED_BY.
     *
     * the workflow execution context's errorHandlerContext will point at the error handler context,
     * but this is cleared when the step runs, and sub-workflows there are not stored or replayable.
     */

    /** returns a result, or null if nothing applied (and caller should rethrow the error) */
    public static Task<WorkflowErrorHandlingResult> createStepErrorHandlerTask(WorkflowStepDefinition step, WorkflowStepInstanceExecutionContext context, Task<?> stepTask, Throwable error, Integer errorStepIfNested) {
        log.debug("Encountered error in step "+context.getWorkflowStepReference()+" '" + stepTask.getDisplayName() + "' (handler present): " + Exceptions.collapseText(error));
        String taskName = context.getWorkflowStepReference();

        if (errorStepIfNested!=null) {
            // already has error-handler reference, either from here or from computed name
            taskName = context.getWorkflowStepReference()+"-"+(errorStepIfNested+1);
        } else {
            if (!taskName.contains(LABEL_FOR_ERROR_HANDLER)) {
                // think this may happen but only for workflow errors?
                taskName += "-" + LABEL_FOR_ERROR_HANDLER;
            }
        }

        Task<WorkflowErrorHandlingResult> task = Tasks.<WorkflowErrorHandlingResult>builder().dynamic(true).displayName(taskName)
                .tag(BrooklynTaskTags.tagForWorkflowStepErrorHandler(context, null, context.getTaskId()))
                .tag(BrooklynTaskTags.WORKFLOW_TAG)
                .body(new WorkflowErrorHandling(step.getOnError(), context.getWorkflowExectionContext(), context.getWorkflowExectionContext().currentStepIndex, stepTask, error))
                .build();
        TaskTags.addTagDynamically(stepTask, BrooklynTaskTags.tagForErrorHandledBy(task));
        log.trace("Creating error handler for step  "+context.getWorkflowStepReference()+" - "+task.getDisplayName()+" in task " + task.getId());
        return task;
    }

    /** returns a result, or null if nothing applied (and caller should rethrow the error) */
    public static Task<WorkflowErrorHandlingResult> createWorkflowErrorHandlerTask(WorkflowExecutionContext context, Task<?> workflowTask, Throwable error) {
        log.debug("Encountered error in workflow "+context.getWorkflowId()+"/"+context.getTaskId()+" '" + workflowTask.getDisplayName() + "' (handler present): " + Exceptions.collapseText(error));
        Task<WorkflowErrorHandlingResult> task = Tasks.<WorkflowErrorHandlingResult>builder().dynamic(true).displayName(context.getWorkflowId()+"-"+LABEL_FOR_ERROR_HANDLER)
                .tag(BrooklynTaskTags.tagForWorkflowStepErrorHandler(context))
                .tag(BrooklynTaskTags.WORKFLOW_TAG)
                .body(new WorkflowErrorHandling(context.onError, context, null, workflowTask, error))
                .build();
        TaskTags.addTagDynamically(workflowTask, BrooklynTaskTags.tagForErrorHandledBy(task));
        log.trace("Creating error handler for workflow "+context.getWorkflowId()+"/"+context.getTaskId()+" - "+task.getDisplayName()+" in task " + task.getId());
        return task;
    }

    final List<WorkflowStepDefinition> errorOptions;
    final WorkflowExecutionContext context;
    final Integer stepIndexIfStepErrorHandler;
    /** The step or the workflow task */
    final Task<?> failedTask;
    final Throwable error;

    public static List<Object> wrappedInListIfNecessaryOrNullIfEmpty(Object onError) {
        if (onError==null) return null;
        MutableList errorList = onError instanceof Collection ? MutableList.copyOf((Collection) onError) : MutableList.of(onError);
        if (errorList.isEmpty()) return null;
        return errorList;
    }

    public WorkflowErrorHandling(Object errorOptionsO, WorkflowExecutionContext context, Integer stepIndexIfStepErrorHandler, Task<?> failedTask, Throwable error) {
        List<Object> errorOptions = wrappedInListIfNecessaryOrNullIfEmpty(errorOptionsO);
        this.errorOptions = WorkflowStepResolution.resolveSubSteps(context.getManagementContext(), "error handling", errorOptions);
        this.context = context;
        this.stepIndexIfStepErrorHandler = stepIndexIfStepErrorHandler;
        this.failedTask = failedTask;
        this.error = error;
    }

    @Override
    public WorkflowErrorHandlingResult call() throws Exception {
        WorkflowErrorHandlingResult result = new WorkflowErrorHandlingResult();
        Task handlerParent = Tasks.current();
        log.debug("Starting "+
                // handler parent is of form task-error-handler if first level, or task-error-handler-N if nested error in outer handler step N
                (handlerParent.getDisplayName().endsWith(LABEL_FOR_ERROR_HANDLER) ? "" : "nested error handler for ")+
                handlerParent.getDisplayName()+" with "+ errorOptions.size()+" step"+(errorOptions.size()!=1 ? "s" : "")+" in task "+handlerParent.getId());

        int errorStepsMatching = 0;
        WorkflowStepDefinition errorStep = null;
        boolean lastStepConditionMatched = false;

        for (int i = 0; i< errorOptions.size(); i++) {
            // go through steps, find first that matches

            errorStep = errorOptions.get(i);

            WorkflowStepInstanceExecutionContext handlerContext = new WorkflowStepInstanceExecutionContext(stepIndexIfStepErrorHandler!=null ? stepIndexIfStepErrorHandler : WorkflowExecutionContext.STEP_INDEX_FOR_ERROR_HANDLER, errorStep, context);
            context.errorHandlerContext = handlerContext;
            handlerContext.setError(error);
            lastStepConditionMatched = false;

            String potentialTaskName = Tasks.current().getDisplayName()+"-"+(i+1);
            DslPredicates.DslPredicate condition = errorStep.getConditionResolved(handlerContext);
            if (condition!=null) {
                if (log.isTraceEnabled()) log.trace("Considering condition " + condition + " for " + potentialTaskName);
                boolean conditionMet = DslPredicates.evaluateDslPredicateWithBrooklynObjectContext(condition, error, handlerContext.getEntity());
                if (log.isTraceEnabled()) log.trace("Considered condition " + condition + " for " + potentialTaskName + ": " + conditionMet);
                if (!conditionMet) continue;
            }

            errorStepsMatching++;
            lastStepConditionMatched = true;

            Task<?> handlerI = errorStep.newTaskAsSubTask(handlerContext,
                    potentialTaskName, BrooklynTaskTags.tagForWorkflowStepErrorHandler(handlerContext, i, handlerParent.getId()));
            TaskTags.addTagDynamically(handlerParent, BrooklynTaskTags.tagForErrorHandledBy(handlerI));

            log.trace("Creating error handler step " + potentialTaskName + " '" + errorStep.computeName(handlerContext, false)+"' in task "+handlerI.getId());
            if (!potentialTaskName.equals(context.getWorkflowStepReference(handlerI))) {
                // shouldn't happen, but double check
                log.warn("Mismatch in step name: "+potentialTaskName+" / "+context.getWorkflowStepReference(handlerI));
            }

            try {
                result.output = DynamicTasks.queue(handlerI).getUnchecked();

                if (errorStep.output!=null) {
                    result.output = handlerContext.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_FINISHING_POST_OUTPUT, errorStep.output, Object.class);
                }
                context.lastErrorHandlerOutput = result.output;
                result.next = WorkflowReplayUtils.getNext(handlerContext, errorStep);

            } catch (Exception errorInErrorHandlerStep) {
                BiConsumer onFinish = (output, next) -> {
                    result.output = output;
                    result.next = next;
                };
                WorkflowErrorHandling.handleErrorAtStep(context.getEntity(), errorStep, handlerContext, handlerI, onFinish, errorInErrorHandlerStep, i);
            }

            if (result.next!=null) {
                log.debug("Completed handler " + Tasks.current().getDisplayName() +
                        (errorStepsMatching > 1 ? " with "+errorStepsMatching+" steps matching" : "") +
                        "; proceeding to explicit next step '" + result.next + "'");
                break;
            }
        }

        if (result.next==null) {
            // no 'next' defined; check we properly handled it
            if (errorStepsMatching==0 || errorStep==null) {
                log.debug("Error handler options were present but did not apply at "+handlerParent.getId());
                return null;
            }
            if (!lastStepConditionMatched) {
                log.warn("Error handler ran but did not return a next execution point and final step had a condition which did not match; " +
                        "this may be an error. " +
                        "For clarity, error handlers should either indicate a next step or have a non-conditional final step. " +
                        "Continuing with next step of outer workflow assuming that the error was handled." +
                        "The target `next: exit` can be used to exit a handler.");
            }
            log.debug("Completed handler " + Tasks.current().getDisplayName() +
                            (errorStepsMatching > 1 ? " with "+errorStepsMatching+" steps matching" : "") +
                    "; no next step indicated so proceeding to default next step");
        }

        return result;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WorkflowErrorHandlingResult {
        Object next;
        Object output;
    }

    public static void handleErrorAtStep(Entity entity, WorkflowStepDefinition step, WorkflowStepInstanceExecutionContext currentStepInstance, Task<?> stepTaskThrowingError, BiConsumer<Object, Object> onFinish, Exception error, Integer errorStepIfNested) {
        String problemHere = null;
        if (wrappedInListIfNecessaryOrNullIfEmpty(step.onError)!=null) {

            if (errorStepIfNested!=null) {
                log.debug("Nested error handler running on "+stepTaskThrowingError+" to handle "+error);
            }
            try {
                Task<WorkflowErrorHandling.WorkflowErrorHandlingResult> stepErrorHandlerTask = WorkflowErrorHandling.createStepErrorHandlerTask(step, currentStepInstance, stepTaskThrowingError, error, errorStepIfNested);
                currentStepInstance.errorHandlerTaskId = stepErrorHandlerTask.getId();

                WorkflowErrorHandling.WorkflowErrorHandlingResult result = DynamicTasks.queue(stepErrorHandlerTask).getUnchecked();
                if (result!=null) {
                    //if (errorStepIfNested==null && STEP_TARGET_NAME_FOR_EXIT.equals(result.next)) {
                    // above would make exit leave all handlers
                    if (STEP_TARGET_NAME_FOR_EXIT.equals(result.next)) {
                        result.next = null;
                    }
                    onFinish.accept(result.output, result.next);
                    return;

                } else {
                    // no steps applied
                    problemHere = "error handler present but no steps applicable, ";
                }

            } catch (Exception e2) {
                logExceptionWhileHandlingException(() -> "in step '"+stepTaskThrowingError.getDisplayName()+"'", entity, e2, error);
                throw Exceptions.propagate(e2);
            } finally {
                if (errorStepIfNested==null) currentStepInstance.getWorkflowExectionContext().lastErrorHandlerOutput = null;
            }

        } else {
            problemHere = "";
        }

        // don't consider replaying automatically here; only done at workflow level

        // previously logged as warning, but author has done everything right so we shouldn't warn, just debug
//        logWarnOnExceptionOrDebugIfKnown(entity, error,
        log.debug(
                currentStepInstance.getWorkflowExectionContext().getName() + ": " +
                "Error in step '" + stepTaskThrowingError.getDisplayName() + "'; " + problemHere + "rethrowing: " + Exceptions.collapseText(error));
        log.trace("Trace for error being rethrown", error);

        throw Exceptions.propagate(error);
    }

    public static void logExceptionWhileHandlingException(Supplier<String> src, Entity entity, Exception newError, Throwable oldError) {
        if (Exceptions.getCausalChain(newError).stream().anyMatch(e3 -> e3== oldError)) {
            // is, or wraps, original error, don't need to log

//                    } else if (Exceptions.isCausedByInterruptInAnyThread(e) && Exceptions.isCausedByInterruptInAnyThread(e2)) {
//                         // now either handled prior to this, or rethrown by the interruption
//                         // if both are interrupted we can drop the trace, and return original;
//                        log.debug("Error where error handler was interrupted, after main thread was also interrupted: " + e2);
//                        log.trace("Full trace of original error was: " + e, e);

        } else if (newError instanceof WorkflowFailException) {
            log.debug("Workflow fail " + src.get() + "; throwing failure object -- "+Exceptions.collapseText(newError)+" -- and dropping original error: "+Exceptions.collapseText(oldError));
            log.trace("Full trace of original error was: " + oldError, oldError);
        } else {
            logWarnOnExceptionOrDebugIfKnown(entity, newError, "Error "+src.get()+"; error handler for -- " + Exceptions.collapseText(oldError) + " -- threw another error (rethrowing): " + Exceptions.collapseText(newError), oldError);
        }
    }

    public static void logWarnOnExceptionOrDebugIfKnown(Entity entity, Throwable e, String msg) {
        logWarnOnExceptionOrDebugIfKnown(entity, e, msg, null);
    }
    public static void logWarnOnExceptionOrDebugIfKnown(Entity entity, Throwable e, String msg, Throwable optionalErrorForStackTrace) {
        boolean logDebug = false;
        logDebug |= (Exceptions.getFirstThrowableOfType(e, DanglingWorkflowException.class)!=null);
        logDebug |= (Entities.isUnmanagingOrNoLongerManaged(entity));
        if (logDebug) {
            log.debug(msg);
            if (optionalErrorForStackTrace!=null) log.trace("Trace of error:", optionalErrorForStackTrace);
        } else {
            log.warn(msg);
            if (optionalErrorForStackTrace != null) log.debug("Trace of error:", optionalErrorForStackTrace);
        }
    }

}