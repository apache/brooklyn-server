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

    Object output;

    public WorkflowExecutionContext getContext() {
        return context;
    }

    public void setContext(WorkflowExecutionContext context) {
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

    public String getWorkflowStepReference() {
        return context.getWorkflowStepReference(stepIndex, stepDefinitionDeclaredId);
    }
}
