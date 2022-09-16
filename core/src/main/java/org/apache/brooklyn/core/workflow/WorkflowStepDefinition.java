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
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;

import java.util.Map;

public abstract class WorkflowStepDefinition {

//    name:  a name to display in the UI; if omitted it is constructed from the step ID and step type
    protected Map<String,Object> input = MutableMap.of();

    protected String name;
    public String getName() {
        return name;
    }

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

    protected Task<?> newTask(WorkflowStepInstanceExecutionContext context) {
        String name = computeTaskName(context);
        Task<Object> t = Tasks.create(name, () -> doTaskBody(context));
        context.name = name;
        context.taskId = t;
        return t;
    }

    protected abstract Object doTaskBody(WorkflowStepInstanceExecutionContext context);

    protected String computeTaskName(WorkflowStepInstanceExecutionContext context) {
        if (Strings.isBlank(getName()) || context.stepDefinitionId.contains(getName())) return context.stepDefinitionId;
        return context.stepDefinitionId + " - " + getName();
    }

    protected String getShorthandTypeName() {
        String name = getClass().getSimpleName();
        if (Strings.isBlank(name)) return getClass().getCanonicalName();
        name = Strings.removeFromEnd(name, "WorkflowStep");
        return name;
    }

    /** allows subclasses to throw exception early if required fields not set */
    public void validateStep() {}

}
