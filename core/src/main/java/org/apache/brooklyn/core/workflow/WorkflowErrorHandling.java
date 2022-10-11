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
     * the step context's errorHandlerContext will point at the error handler context,
     * but this is cleared when the step runs, and sub-workflows there are not stored or replayable.
     */

    /** returns a result, or null if nothing applied (and caller should rethrow the error) */
    public static Task<WorkflowErrorHandlingResult> createTask(WorkflowStepDefinition step, WorkflowStepInstanceExecutionContext context, Task<?> stepTask, Exception error) {
        log.debug("Creating handler for error in step '" + stepTask.getDisplayName() + ": " + Exceptions.collapseText(error));
        Task<WorkflowErrorHandlingResult> task = Tasks.<WorkflowErrorHandlingResult>builder().dynamic(true).displayName("Error handler for: " + stepTask.getDisplayName())
                .tag(BrooklynTaskTags.tagForWorkflowError(context, null, context.getTaskId()))
                .body(new WorkflowErrorHandling(step, context, stepTask, error))
                .build();
        TaskTags.addTagDynamically(stepTask, BrooklynTaskTags.tagForErrorHandledBy(task));
        return task;
    }

    final WorkflowStepDefinition step;
    final WorkflowStepInstanceExecutionContext callerContext;
    final Task<?> stepTask;
    final Exception error;

    public WorkflowErrorHandling(WorkflowStepDefinition step, WorkflowStepInstanceExecutionContext context, Task<?> stepTask, Exception error) {
        this.step = step;
        this.callerContext = context;
        this.stepTask = stepTask;
        this.error = error;
    }

    @Override
    public WorkflowErrorHandlingResult call() throws Exception {
        WorkflowErrorHandlingResult result = new WorkflowErrorHandlingResult();
        log.debug("Running handler for error in step '" + stepTask.getDisplayName() + ", "+step.getOnError().size()+" handler option(s)");
        Task handlerParent = Tasks.current();

        for (int i=0; i<step.getOnError().size(); i++) {
            // go through steps, find first that matches

            WorkflowStepDefinition errorStep = (WorkflowStepDefinition) step.getOnError().get(i);

            WorkflowStepInstanceExecutionContext handlerContext = new WorkflowStepInstanceExecutionContext(-3, errorStep, callerContext.getWorkflowExectionContext());
            callerContext.errorHandlerContext = handlerContext;
            handlerContext.error = error;

            DslPredicates.DslPredicate condition = errorStep.getConditionResolved(handlerContext);
            if (condition!=null) {
                // TODO new tests on predicate: instance-of (condition), error-cause, error-field

                if (log.isTraceEnabled()) log.trace("Considering condition " + condition + " for error handler " + i);
                boolean conditionMet = DslPredicates.evaluateDslPredicateWithBrooklynObjectContext(condition, error, handlerContext.getEntity());
                if (log.isTraceEnabled()) log.trace("Considered condition " + condition + " for error handler " + i + ": " + conditionMet);
                if (!conditionMet) continue;
            }

            log.debug("Error handler " + i + " applies: " + errorStep);

            Task<?> handlerI = errorStep.newTaskForErrorHandler(handlerContext, i, handlerParent);
            TaskTags.addTagDynamically(handlerParent, BrooklynTaskTags.tagForErrorHandledBy(handlerI));

            result.output = DynamicTasks.queue(handlerI).getUnchecked();
            if (errorStep.output!=null) {
                // TODO inject special contextual resolution for handler, local, and error
                result.output = handlerContext.resolve(errorStep.output, Object.class);
            }
            result.next = errorStep.getNext();
            return result;
        }

        // if none apply
        log.debug("No error handlers applied for step '" + stepTask.getDisplayName() + " error, caller should rethrow");
        return null;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class WorkflowErrorHandlingResult {
        String next;
        Object output;
    }
}