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
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStepDefinition.class);

//    name:  a name to display in the UI; if omitted it is constructed from the step ID and step type
    protected Map<String,Object> input = MutableMap.of();

    protected String id;
    public String getId() {
        return id;
    }

    protected String name;
    public String getName() {
        return name;
    }

    @JsonIgnore
    protected String userSuppliedShorthand;

    // TODO
//    //    timeout:  a duration, after which the task is interrupted (and should cancel the task); if omitted, there is no explicit timeout at a step (the containing workflow may have a timeout)
//    protected Duration timeout;
//    public Duration getTimeout() {
//        return timeout;
//    }

    //    next:  the next step to go to, assuming the step runs and succeeds; if omitted, or if the condition does not apply, it goes to the next step per the ordering (described below)
    protected String next;
    public String getNext() {
        return next;
    }

    //    condition:  a condition to require for the step to run; if false, the step is skipped
    protected Object condition;
    @JsonIgnore
    public Object getConditionRaw() {
        return condition;
    }
    @JsonIgnore
    public DslPredicates.DslPredicate getConditionResolved(WorkflowStepInstanceExecutionContext context) {
        return context.resolveWrapped(condition, TypeToken.of(DslPredicates.DslPredicate.class));
    }

    // output of steps can be overridden
    protected Object output;
    protected boolean isOutputHandledByTask() { return false; }

    // TODO
//    on-error:  a description of how to handle errors section
//    log-marker:  a string which is included in all log messages in the workflow or step and any sub-tasks and subtasks in to easily identify the actions of this workflow (in addition to task IDs)

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
        populateFromShorthandTemplate(template, value, false);
    }
    protected void populateFromShorthandTemplate(String template, String value, boolean finalMatchRaw) {
        Maybe<Map<String, Object>> result = new ShorthandProcessor(template).withFinalMatchRaw(finalMatchRaw).process(value);
        if (result.isAbsent()) throw new IllegalArgumentException("Invalid shorthand expression: '"+value+"'", Maybe.Absent.getException(result));

        input.putAll((Map) CollectionMerger.builder().build().merge(input, result.get()));
    }

    protected Task<?> newTask(WorkflowStepInstanceExecutionContext context) {
        context.name = Strings.isNonBlank(this.name) ? this.name : computeName(context, false);
        if (log.isTraceEnabled()) log.trace("Creating task for "+computeName(context, true)+", input "+input);
        Task<?> t = Tasks.builder().displayName(computeName(context, true)).body(() -> {
            log.debug("Starting step "+context.getWorkflowExectionContext().getWorkflowStepReference(context.stepIndex, this)
                    + (Strings.isNonBlank(name) ? " '"+name+"'" : "")
                    + " in task "+context.taskId);
            Object result = doTaskBody(context);
            if (log.isTraceEnabled()) log.trace("Completed task for "+computeName(context, true)+", output "+result);
            return result;
        }).tag(context).build();
        context.taskId = t.getId();
        return t;
    }

    protected abstract Object doTaskBody(WorkflowStepInstanceExecutionContext context);

    protected String computeName(WorkflowStepInstanceExecutionContext context, boolean includeStepNumber) {
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

    protected String getShorthandTypeName() {
        String name = getClass().getSimpleName();
        if (Strings.isBlank(name)) return getClass().getCanonicalName();
        name = Strings.removeFromEnd(name, "WorkflowStep");
        return name;
    }

    /** allows subclasses to throw exception early if required fields not set */
    public void validateStep() {}

    public interface SpecialWorkflowStepDefinition {
        WorkflowStepDefinition applySpecialDefinition(ManagementContext mgmt, Object definition, String typeBestGuess, SpecialWorkflowStepDefinition firstParse);
    }
}
