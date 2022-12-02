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

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStepDefinition.class);

    protected String id;
    public String getId() {
        return id;
    }

    //    name:  a name to display in the UI; if omitted it is constructed from the step ID and step type
    protected String name;
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    protected String userSuppliedShorthand;
    protected String shorthandTypeName;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    protected Map<String,Object> input = MutableMap.of();

    //    next:  the next step to go to, assuming the step runs and succeeds; if omitted, or if the condition does not apply, it goes to the next step per the ordering (described below)
    @JsonProperty("next")  //use this field for access, not the getter/setter
    protected String next;

    //    condition:  a condition to require for the step to run; if false, the step is skipped
    protected Object condition;
    @JsonIgnore
    public Object getConditionRaw() {
        return condition;
    }
    @JsonIgnore
    public DslPredicates.DslPredicate getConditionResolved(WorkflowStepInstanceExecutionContext context) {
        try {
            return context.resolveWrapped(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, getConditionRaw(), TypeToken.of(DslPredicates.DslPredicate.class));
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Unresolveable condition ("+getConditionRaw()+")", e);
        }
    }

    // output of steps can be overridden
    protected Object output;
    protected boolean isOutputHandledByTask() { return false; }

    protected String replayable;
    protected String idempotent;
    protected Pair<WorkflowReplayUtils.ReplayableAtStepOption, Boolean> validateReplayableAndIdempotent() {
        return WorkflowReplayUtils.validateReplayableAndIdempotentAtStep(replayable, idempotent, false);
    }

    @JsonProperty("timeout")
    protected Duration timeout;
    @JsonIgnore
    public Duration getTimeout() {
        return timeout;
    }

    // might be nice to support a shorthand for on-error; but not yet
    @JsonProperty("on-error")
    protected Object onError = MutableList.of();
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Object getOnError() {
        return onError;
    }

    @JsonAnySetter
    public void setInput(String key, Object value) {
        input.put(key, value);
    }

    public <T> void setInput(ConfigKey<T> key, T value) {
        input.put(key.getName(), value);
    }

    public void setInput(Map<String, Object> input) {
        this.input.putAll(input);
    }

    /** Returns the unresolved map of inputs */
    public Map<String, Object> getInput() {
        return input;
    }

    /** note, this should _not_ have the type string first, whereas in YAML the shorthand must have the type string first */
    abstract public void populateFromShorthand(String value);

    protected void populateFromShorthandTemplate(String template, String value) {
        populateFromShorthandTemplate(template, value, false, true);
    }
    protected void populateFromShorthandTemplate(String template, String value, boolean finalMatchRaw, boolean failOnMismatch) {
        Maybe<Map<String, Object>> result = new ShorthandProcessor(template).withFinalMatchRaw(finalMatchRaw).withFailOnMismatch(failOnMismatch).process(value);
        if (result.isAbsent()) throw new IllegalArgumentException("Invalid shorthand expression: '"+value+"'", Maybe.Absent.getException(result));

        input.putAll((Map) CollectionMerger.builder().build().merge(input, result.get()));
    }

    final Task<?> newTask(WorkflowStepInstanceExecutionContext context) {
        return newTask(context, null, null, null);
    }

    public final Task<?> newTaskAsSubTask(WorkflowStepInstanceExecutionContext context, String specialName, BrooklynTaskTags.WorkflowTaskTag specialTag) {
        return newTask(context, null, specialName, specialTag);
    }

    final Task<?> newTaskContinuing(WorkflowStepInstanceExecutionContext context, ReplayContinuationInstructions continuationInstructions) {
        return newTask(context, Preconditions.checkNotNull(continuationInstructions), null, null);
    }

    protected Task<?> newTask(WorkflowStepInstanceExecutionContext context, ReplayContinuationInstructions continuationInstructions,
                              String specialName, BrooklynTaskTags.WorkflowTaskTag tagOverride) {
        Task<?> t = Tasks.builder().displayName(specialName!=null ? specialName : computeName(context, true))
                .tag(tagOverride != null ? tagOverride : BrooklynTaskTags.tagForWorkflow(context))
                .tag(BrooklynTaskTags.WORKFLOW_TAG)
                .tag(TaskTags.INESSENTIAL_TASK)  // we handle this specially, don't want the thread to fail
                .body(() -> {
            log.debug("Starting " +
                    (specialName!=null ? specialName : "step "+context.getWorkflowExectionContext().getWorkflowStepReference(context.stepIndex, this))
                    + (Strings.isNonBlank(name) ? " '"+name+"'" : "")
                    + (continuationInstructions!=null ? " (continuation"
                            + (continuationInstructions.customBehaviorExplanation !=null ? " - "+continuationInstructions.customBehaviorExplanation : "")
                            + ")"
                        : "")
                    + " in task "+Tasks.current().getId());
            Callable<Object> handler = null;

            if (continuationInstructions!=null && this instanceof WorkflowStepDefinitionWithSubWorkflow) {
                List<WorkflowExecutionContext> unfinished = ((WorkflowStepDefinitionWithSubWorkflow) this).getSubWorkflowsForReplay(context, continuationInstructions.forced, false, true);
                if (unfinished!=null) {
                    handler = () ->
                            ((WorkflowStepDefinitionWithSubWorkflow) this).doTaskBodyWithSubWorkflowsForReplay(context, unfinished, continuationInstructions);
                } else {
                    // fall through to below; the sub-workflows were not persisted so shouldn't have been started
                }
            }
            if (handler==null) {
                handler = () -> {
                    if (continuationInstructions != null && continuationInstructions.customBehavior != null) {
                        continuationInstructions.customBehavior.run();
                    }
                    return doTaskBody(context);
                };
            };

            Object result = handler.call();
            if (log.isTraceEnabled()) log.trace("Completed task for "+computeName(context, true)+", output "+result);
            return result;
        }).build();
        context.taskId = t.getId();
        return t;
    }

    protected abstract Object doTaskBody(WorkflowStepInstanceExecutionContext context);

    public String computeName(WorkflowStepInstanceExecutionContext context, boolean includeStepNumber) {
        //if (Strings.isNonBlank(context.name)) return context.name;

        List<String> parts = MutableList.of();
        if (includeStepNumber) parts.add("" + (context.stepIndex + 1));

        boolean hasId = false;
        if (context.stepDefinitionDeclaredId != null) {
            hasId = true;
            String s = context.stepDefinitionDeclaredId;

            if (!parts.isEmpty() && s.startsWith(parts.get(0))) {
                // if step 1 id is `1-foo` then don't prepend "1 - "
                s = Strings.removeFromStart(s, parts.get(0));
                if (Strings.isBlank(s) || !Character.isDigit(s.charAt(0))) {
                    // id starts with step number so don't include
                    parts.remove(0);
                }
            }
            parts.add(context.stepDefinitionDeclaredId);
        }

        if (Strings.isNonBlank(context.name)) {
            // if there is a name, add that also, removing id if name starts with id
            String last = parts.isEmpty() ? null : parts.get(parts.size()-1);
            if (last!=null && context.name.startsWith(last)) {
                parts.remove(parts.size()-1);
            }
            parts.add(context.name);
        } else if (!hasId) {
            if (Strings.isNonBlank(userSuppliedShorthand)) {
                // if there is shorthand, add it only if no name and no id
                parts.add(userSuppliedShorthand);
            } else {
                // name will just be the number. including the type for a bit of visibility.
                parts.add(getShorthandTypeName());
            }
        }

        return Strings.join(parts, " - ");
    }

    public void setShorthandTypeName(String shorthandTypeDefinition) { this.shorthandTypeName = shorthandTypeDefinition; }
    @JsonProperty("shorthandTypeName")  // REST API should prefer this accessor
    public String getShorthandTypeName() {
        if (Strings.isNonBlank(shorthandTypeName)) return shorthandTypeName;

        String name = getClass().getSimpleName();
        if (Strings.isBlank(name)) return getClass().getCanonicalName();
        name = Strings.removeFromEnd(name, "WorkflowStep");
        return name;
    }

    /** allows subclasses to throw exception early if required fields not set */
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        validateReplayableAndIdempotent();
    }

    @JsonIgnore
    protected Object getStepState(WorkflowStepInstanceExecutionContext context) {
        return context.getStepState();
    }

    Boolean isIdempotent(WorkflowStepInstanceExecutionContext csi) {
        Boolean idempotence = validateReplayableAndIdempotent().getRight();

        if (idempotence==null) {
            if (csi!=null && csi.getWorkflowExectionContext()!=null) idempotence = csi.getWorkflowExectionContext().idempotentAll;
        }
        if (idempotence==null) {
            idempotence = isDefaultIdempotent();
        }

        return idempotence;
    }

    protected abstract Boolean isDefaultIdempotent();

    public interface WorkflowStepDefinitionWithSpecialDeserialization {
        WorkflowStepDefinition applySpecialDefinition(ManagementContext mgmt, Object definition, String typeBestGuess, WorkflowStepDefinitionWithSpecialDeserialization firstParse);
    }

    public interface WorkflowStepDefinitionWithSubWorkflow {
        /** returns null if this task hasn't yet recorded its subworkflows; otherwise list of those which are replayable, empty if none need to be replayed (ended successfully) */
        @JsonIgnore List<WorkflowExecutionContext> getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, boolean forced, boolean peekingOnly, boolean allowInternallyEvenIfDisabled);
        /** called by framework if {@link #getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext, boolean, boolean, boolean)} returns non-null (empty is okay),
         * and the implementation pass the replay and optional custom behaviour to the subworkflows before doing any finalization;
         * if the subworkflow for replay is null,  the normal {@link #doTaskBody(WorkflowStepInstanceExecutionContext)} is called. */
        Object doTaskBodyWithSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, @Nonnull List<WorkflowExecutionContext> subworkflows, ReplayContinuationInstructions instructions);
    }

    public static class ReplayContinuationInstructions {
        /** null means last, -1 means workflow start */
        public final Integer stepToReplayFrom;

        public final String customBehaviorExplanation;
        /** if supplied, custom behavior run before the primary doTaskBody; may throw exceptions or set things in stepState which are interpreted by the body */
        public final Runnable customBehavior;
        public final boolean forced;

        public ReplayContinuationInstructions(Integer stepToReplayFrom, String customBehaviourExplanation, Runnable customBehavior, boolean forced) {
            this.stepToReplayFrom = stepToReplayFrom;
            this.customBehaviorExplanation = customBehaviourExplanation;
            this.customBehavior = customBehavior;
            this.forced = forced;
        }

        @Override
        public String toString() {
            return "Replay["+(Strings.isNonBlank(customBehaviorExplanation) ? customBehaviorExplanation : "(no explanation)")
                    +(stepToReplayFrom!=null ? "; step "+stepToReplayFrom : "; continuing from last")
                    +(forced ? "; FORCED" : "")
                    +"]";
        }
    }

}
