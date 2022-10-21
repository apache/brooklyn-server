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
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public class WorkflowErrorHandling implements Callable<WorkflowErrorHandling.WorkflowErrorHandlingResult> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowErrorHandling.class);

    /*
     * TODO ui for error handling
     * step task's workflow tag will have ERROR_HANDLED_BY single-key map tag pointing at handler parent task, created in this method.
     * error handler parent task will have 'errorHandlerForTask' field in the workflow tag pointing at step task, but no errorHandlerIndex.
     * if any of the handler steps match, the parent will have ERROR_HANDLED_BY pointing at it, and will have a task with the workflow tag with
     * 'errorHandlerForTask' field pointing at the parent and also 'errorHandlerIndex' set to the index in the step's error list, but not ERROR_HANDLED_BY.
     *
     * the workflow execution context's errorHandlerContext will point at the error handler context,
     * but this is cleared when the step runs, and sub-workflows there are not stored or replayable.
     */

    /** returns a result, or null if nothing applied (and caller should rethrow the error) */
    public static Task<WorkflowErrorHandlingResult> createStepErrorHandlerTask(WorkflowStepDefinition step, WorkflowStepInstanceExecutionContext context, Task<?> stepTask, Throwable error) {
        log.debug("Encountered error in step "+context.getWorkflowStepReference()+" '" + stepTask.getDisplayName() + "' (handler present): " + Exceptions.collapseText(error));
        Task<WorkflowErrorHandlingResult> task = Tasks.<WorkflowErrorHandlingResult>builder().dynamic(true).displayName(context.getWorkflowStepReference()+"-error-handler")
                .tag(BrooklynTaskTags.tagForWorkflowStepErrorHandler(context, null, context.getTaskId()))
                .tag(BrooklynTaskTags.WORKFLOW_TAG)
                .body(new WorkflowErrorHandling(step.getOnError(), context.getWorkflowExectionContext(), context.getWorkflowExectionContext().currentStepIndex, stepTask, error))
                .build();
        TaskTags.addTagDynamically(stepTask, BrooklynTaskTags.tagForErrorHandledBy(task));
        log.debug("Creating step "+context.getWorkflowStepReference()+" error handler "+task.getDisplayName()+" in task " + task.getId());
        return task;
    }

    /** returns a result, or null if nothing applied (and caller should rethrow the error) */
    public static Task<WorkflowErrorHandlingResult> createWorkflowErrorHandlerTask(WorkflowExecutionContext context, Task<?> workflowTask, Throwable error) {
        log.debug("Encountered error in workflow "+context.getWorkflowId()+"/"+context.getTaskId()+" '" + workflowTask.getDisplayName() + "' (handler present): " + Exceptions.collapseText(error));
        Task<WorkflowErrorHandlingResult> task = Tasks.<WorkflowErrorHandlingResult>builder().dynamic(true).displayName(context.getWorkflowId()+"-error-handler")
                .tag(BrooklynTaskTags.tagForWorkflowStepErrorHandler(context))
                .tag(BrooklynTaskTags.WORKFLOW_TAG)
                .body(new WorkflowErrorHandling(context.onError, context, null, workflowTask, error))
                .build();
        TaskTags.addTagDynamically(workflowTask, BrooklynTaskTags.tagForErrorHandledBy(task));
        log.debug("Creating workflow "+context.getWorkflowId()+"/"+context.getTaskId()+" error handler "+task.getDisplayName()+" in task " + task.getId());
        return task;
    }

    final List<WorkflowStepDefinition> errorOptions;
    final WorkflowExecutionContext context;
    final Integer stepIndexIfStepErrorHandler;
    /** The step or the workflow task */
    final Task<?> failedTask;
    final Throwable error;

    public WorkflowErrorHandling(List<Object> errorOptions, WorkflowExecutionContext context, Integer stepIndexIfStepErrorHandler, Task<?> failedTask, Throwable error) {
        this.errorOptions = WorkflowStepResolution.resolveOnErrorSteps(context.getManagementContext(), errorOptions);
        this.context = context;
        this.stepIndexIfStepErrorHandler = stepIndexIfStepErrorHandler;
        this.failedTask = failedTask;
        this.error = error;
    }

    @Override
    public WorkflowErrorHandlingResult call() throws Exception {
        WorkflowErrorHandlingResult result = new WorkflowErrorHandlingResult();
        Task handlerParent = Tasks.current();
        log.debug("Starting "+handlerParent.getDisplayName()+" with "+ errorOptions.size()+" handler"+(errorOptions.size()!=1 ? " options" : "")+" in task "+handlerParent.getId());

        for (int i = 0; i< errorOptions.size(); i++) {
            // go through steps, find first that matches

            WorkflowStepDefinition errorStep = errorOptions.get(i);

            WorkflowStepInstanceExecutionContext handlerContext = new WorkflowStepInstanceExecutionContext(stepIndexIfStepErrorHandler!=null ? stepIndexIfStepErrorHandler : WorkflowExecutionContext.STEP_INDEX_FOR_ERROR_HANDLER, errorStep, context);
            context.errorHandlerContext = handlerContext;
            handlerContext.error = error;

            String potentialTaskName = Tasks.current().getDisplayName()+"-"+(i+1);
            DslPredicates.DslPredicate condition = errorStep.getConditionResolved(handlerContext);
            if (condition!=null) {
                if (log.isTraceEnabled()) log.trace("Considering condition " + condition + " for " + potentialTaskName);
                boolean conditionMet = DslPredicates.evaluateDslPredicateWithBrooklynObjectContext(condition, error, handlerContext.getEntity());
                if (log.isTraceEnabled()) log.trace("Considered condition " + condition + " for " + potentialTaskName + ": " + conditionMet);
                if (!conditionMet) continue;
            }


            Task<?> handlerI = errorStep.newTaskForErrorHandler(handlerContext,
                    potentialTaskName, BrooklynTaskTags.tagForWorkflowStepErrorHandler(handlerContext, i, handlerParent.getId()));
            TaskTags.addTagDynamically(handlerParent, BrooklynTaskTags.tagForErrorHandledBy(handlerI));

            log.debug("Creating handler " + potentialTaskName + " '" + errorStep.computeName(handlerContext, false)+"' in task "+handlerI.getId());
            if (!potentialTaskName.equals(context.getWorkflowStepReference(handlerI))) {
                // shouldn't happen, but double check
                log.warn("Mismatch in step name: "+potentialTaskName+" / "+context.getWorkflowStepReference(handlerI));
            }

            result.output = DynamicTasks.queue(handlerI).getUnchecked();

            if (errorStep.output!=null) {
                result.output = handlerContext.resolve(errorStep.output, Object.class);
            }
            result.next = errorStep.getNext();
            log.debug("Completed handler " + potentialTaskName + "; proceeding to " + (result.next!=null ? result.next : "default next step"));
            return result;
        }

        // if none apply
        log.debug("No error handler options applied at "+handlerParent.get()+"; will rethrow error");
        return null;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WorkflowErrorHandlingResult {
        String next;
        Object output;
    }
}