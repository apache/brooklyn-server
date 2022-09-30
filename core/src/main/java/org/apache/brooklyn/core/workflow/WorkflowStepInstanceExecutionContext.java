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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.internal.ConfigUtilsInternal;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Identifiers;

import java.util.Map;
import java.util.function.Supplier;

public class WorkflowStepInstanceExecutionContext {


    private WorkflowStepInstanceExecutionContext() {}
    public WorkflowStepInstanceExecutionContext(int stepIndex, WorkflowStepDefinition step, WorkflowExecutionContext context) {
        this.stepIndex = stepIndex;
        this.stepDefinitionDeclaredId = step.id;
        this.input = step.getInput();
        this.context = context;
    }

    int stepIndex;
    String stepDefinitionDeclaredId;
    public String name;
    String taskId;
    Map<String,Object> input = MutableMap.of();
    transient WorkflowExecutionContext context;

    /** optional object which can be used on a per-step basis to hold and persist any step-specific state;
     * steps which have special replay-when-interrupted behaviour should store data they need to replay-when-interrupted,
     * and the {@link WorkflowStepDefinition#doTaskBody(WorkflowStepInstanceExecutionContext)} method should check this
     * at start to determine if resumption is necessary. this will be null on any replay-with-reinitialize. */
    Object stepState;

    Object output;

    public void injectContext(WorkflowExecutionContext context) {
        if (this.context!=null && this.context!=context) throw new IllegalStateException("Cannot change context, from "+this.context+" to "+context);
        this.context = context;
    }

    /** Returns the resolved value of the given key, converting to the type of the key */
    public <T> T getInput(ConfigKey<T> key) {
        return getInput(key.getName(), key.getTypeToken());
    }
    /** Returns the resolved value of the given key, converting to the type of the key if the key is known */
    public Object getInput(String key) {
        ConfigKey<?> keyTyped = ConfigUtilsInternal.findConfigKeys(getClass(), null).get(key);
        if (keyTyped!=null) return getInput(keyTyped);
        return getInput(key, Object.class);
    }
    /** Returns the resolved value of the given key, converting to the given type */
    public <T> T getInput(String key, Class<T> type) {
        return getInput(key, TypeToken.of(type));
    }
    /** Returns the resolved value of the given key, converting to the given type */
    public <T> T getInput(String key, TypeToken<T> type) {
        return context.resolve(input.get(key), type);
    }
    /** Returns the unresolved value of the given key */
    public Object getInputRaw(String key) {
        return input.get(key);
    }

    @JsonIgnore
    public Entity getEntity() {
        return context.getEntity();
    }

    @JsonIgnore
    public WorkflowExecutionContext getWorkflowExectionContext() {
        return context;
    }

    @JsonIgnore
    public Object getPreviousStepOutput() {
        return getWorkflowExectionContext().getPreviousStepOutput();
    }

    public void setStepState(Object stepState, boolean persist) {
        this.stepState = stepState;
        if (persist) getWorkflowExectionContext().persist();
    }
    public Object getStepState() {
        return stepState;
    }

    public TypeToken<?> lookupType(String type, Supplier<TypeToken<?>> ifUnset) {
        return context.lookupType(type, ifUnset);
    }

    public Object resolve(String expression) {
        return context.resolve(expression);
    }

    public <T> T resolve(Object expression, Class<T> type) {
        return context.resolve(expression, type);
    }

    public <T> T resolve(Object expression, TypeToken<T> type) {
        return context.resolve(expression, type);
    }
    public <T> T resolveWrapped(Object expression, TypeToken<T> type) {
        return context.resolveWrapped(expression, type);
    }
    public <T> T resolveWaiting(Object expression, TypeToken<T> type) {
        return context.resolveWaiting(expression, type);
    }

    @JsonIgnore
    public String getWorkflowStepReference() {
        return context.getWorkflowStepReference(stepIndex, stepDefinitionDeclaredId);
    }

    @JsonIgnore
    public ManagementContext getManagementContext() {
        return getWorkflowExectionContext().getManagementContext();
    }
}
