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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.reflect.TypeToken;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.internal.ConfigUtilsInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.LossySerializingThrowable;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkflowStepInstanceExecutionContext {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStepInstanceExecutionContext.class);

    // see getInput, here and for workflow context; once resolved, use the resolved value, without re-resolving;
    // do not return the resolved value via REST/JSON as it might have secrets, but do persist it so replays
    // can use the same values
    static final boolean REMEMBER_RESOLVED_INPUT = true;

    private WorkflowStepInstanceExecutionContext() {}
    public WorkflowStepInstanceExecutionContext(int stepIndex, WorkflowStepDefinition step, WorkflowExecutionContext context) {
        this.name = step.getName();
        this.stepIndex = stepIndex;
        this.stepDefinitionDeclaredId = step.id;
        this.context = context;
        this.input = MutableMap.copyOf(step.getInput());
    }

    int stepIndex;
    String stepDefinitionDeclaredId;
    String name;
    String taskId;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<String,Object> input = MutableMap.of();
    @JsonIgnore  // persist as sensor but not via REST in case it has secrets resolved
    Map<String,Object> inputResolved = MutableMap.of();

    @JsonIgnore
    public Map<String, Object> getAllInput() {
        return input;
    }
    @JsonIgnore
    public Map<String, Object> getAllInputResolved() {
        return inputResolved;
    }

    transient WorkflowExecutionContext context;
    // replay instructions or a string explicit next step identifier
    public Object next;
    public Boolean nextIsReturn;
    public Boolean isLocalSubworkflow;

    /** Return any error we are handling, if the step is in an error handler,
     * or an unhandled error if the step is not in an error handler,
     * otherwise null.
     *
     * After persistence, this stores a simplified form of the error (via {@link LossySerializingThrowable}). */
    public Throwable getError() {
        if (error==null && errorRecord!=null) error = errorRecord.getError();
        return error;
    }
    transient Throwable error;
    @XStreamAlias("error")
    @JsonIgnore
    Throwable errorLegacyDeserialized;
    @JsonIgnore
    LossySerializingThrowable errorRecord;
    void setError(Throwable t) {
        error = t;
        errorRecord = new LossySerializingThrowable(error);
    }
    /** The Jackson error is always just a string. */
    @JsonGetter("error") String getErrorForJson() { return Exceptions.collapseText(error); }
    @JsonSetter("error") void setErrorFromJson(String error) { setError(new RuntimeException(error)); }

    /** set if there was an error handled locally */
    String errorHandlerTaskId;

    public String getTaskId() {
        return taskId;
    }

    /** optional object which can be used on a per-step basis to hold and persist any step-specific state;
     * steps which have special replay-when-interrupted behaviour should store data they need to replay-when-interrupted,
     * and the {@link WorkflowStepDefinition#doTaskBody(WorkflowStepInstanceExecutionContext)} method should check this
     * at start to determine if resumption is necessary. this will be null on any replay-with-reinitialize. */
    Object stepState;

    /** workflows launched by this task; unused by most. if used, steps should implement {@link org.apache.brooklyn.core.workflow.WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow}
     * and track which ones are active in the stepState */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Set<BrooklynTaskTags.WorkflowTaskTag> subWorkflows = MutableSet.of();

    transient Object outputOld;
    Object output;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String,Object> otherMetadata = MutableMap.of();

    public void injectContext(WorkflowExecutionContext context) {
        if (this.context!=null && this.context!=context) throw new IllegalStateException("Cannot change context, from "+this.context+" to "+context);
        this.context = context;
    }

    public String getName() {
        return name;
    }

    public int getStepIndex() {
        return stepIndex;
    }

    /** Returns the resolved value of the given key, converting to the type of the key */
    public <T> T getInput(ConfigKey<T> key) {
        return getInput(key.getName(), key.getTypeToken());
    }
    public <T> T getInputOrDefault(ConfigKey<T> key) {
        T result = getInput(key.getName(), key.getTypeToken());
        if (result==null && !input.containsKey(key.getName())) result = key.getDefaultValue();
        return result;
    }
    /** Returns the resolved value of the given key, converting to the type of the key if the key is known */
    public Object getInput(String key) {
        ConfigKey<?> keyTyped = ConfigUtilsInternal.findConfigKeys(getClass(), null).get(key);
        if (keyTyped!=null) return getInput(keyTyped);
        return getInput(key, Object.class);
    }
    /** Returns the resolved value of the given key, converting to the given type. Applies Brooklyn DSL resolution AND Freemarker resolution. */
    public <T> T getInput(String key, Class<T> type) {
        return getInput(key, TypeToken.of(type));
    }
    public <T> Maybe<T> getInputMaybe(String key, Class<T> type) {
        return getInputMaybe(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, key, TypeToken.of(type));
    }
    /** Returns the resolved value of the given key, converting to the given type.
     * Stores the resolved input so if re-resolved it returns the same.
     * (Input is not resolved until first access because some implementations, such as 'let', might handle errors in resolution.
     * But once resolved we don't want inconsistent return values.) */
    public <T> T getInput(String key, TypeToken<T> type) {
        return getInput(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, key, type);
    }
    public boolean hasInput(ConfigKey<?> key) {
        return hasInput(key.getName());
    }
    public boolean hasInput(String key) {
        return input.containsKey(key);
    }
    public <T> T getInput(WorkflowExpressionResolution.WorkflowExpressionStage stage, String key, TypeToken<T> type) {
        return getInputMaybe(stage, key, type).orNull();
    }
    public <T> Maybe<T> getInputMaybe(WorkflowExpressionResolution.WorkflowExpressionStage stage, String key, TypeToken<T> type) {
        if (inputResolved.containsKey(key)) return Maybe.ofAllowingNull((T)inputResolved.get(key));

        Maybe<Object> vm = WorkflowExpressionResolution.getMapMaybe(input, key);
        if (vm.isAbsent()) return Maybe.castAbsent(vm);

        if (vm.isNull()) return Maybe.ofAllowingNull(null);
        Object v = vm.get();
        T v2;
        try {
            v2 = WorkflowExpressionResolution.allowingRecursionWhenSetting(context, WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, key,
                    () -> context.resolve(stage, v, type));
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Cannot resolve input " +
                    (Boxing.isPrimitiveOrStringOrBoxedObject(v) ? "'" + v + "'" : v.getClass().getName() + " (" + v + ")"), e);
        }
        if (REMEMBER_RESOLVED_INPUT) {
            if (!Objects.equals(v, v2)) {
                inputResolved.put(key, v2);
            }
        }
        return Maybe.of(v2);
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

    public Object getOutput() {
        return output;
    }
    public void setOutput(Object output) {
        this.outputOld = output;
        this.output = output;
    }

    @JsonIgnore
    public Object getPreviousStepOutput() {
        return getWorkflowExectionContext().getPreviousStepOutput();
    }

    public void setStepState(Object stepState, boolean persist) {
        this.stepState = stepState;
        if (persist) getWorkflowExectionContext().persist();
    }
    Object getStepState() {
        return stepState;
    }

    public Set<BrooklynTaskTags.WorkflowTaskTag> getSubWorkflows() {
        return subWorkflows;
    }

    public TypeToken<?> lookupType(String type, Supplier<TypeToken<?>> ifUnset) {
        return context.lookupType(type, ifUnset);
    }

    public Object resolve(WorkflowExpressionResolution.WorkflowExpressionStage stage, String expression) {
        return context.resolve(stage, expression);
    }

    public <T> T resolve(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, Class<T> type) {
        return context.resolve(stage, expression, type);
    }

    public <T> T resolve(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, TypeToken<T> type) {
        return context.resolve(stage, expression, type);
    }
    public <T> T resolveWrapped(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, TypeToken<T> type, WorkflowExpressionResolution.WrappingMode wrappingMode) {
        return context.resolveWrapped(stage, expression, type, wrappingMode);
    }
    public <T> T resolveWaiting(WorkflowExpressionResolution.WorkflowExpressionStage stage, Object expression, TypeToken<T> type) {
        return context.resolveWaiting(stage, expression, type);
    }

    @JsonIgnore
    public String getWorkflowStepReference() {
        return context==null ? "unknown-"+stepDefinitionDeclaredId+"-"+stepIndex : context.getWorkflowStepReference(stepIndex, stepDefinitionDeclaredId, getError()!=null);
    }

    @JsonIgnore
    public ManagementContext getManagementContext() {
        return getWorkflowExectionContext().getManagementContext();
    }

    /** sets other metadata, e.g. for the UI */
    public void noteOtherMetadata(String key, Object value) {
        noteOtherMetadata(key, value, true);
    }
    public void noteOtherMetadata(String key, Object value, boolean includeInLogs) {
        if (includeInLogs) log.debug(getWorkflowStepReference()+" note metadata '"+key+"': "+value);
        otherMetadata.put(key, value);
    }

    @Override
    public String toString() {
        return "WorkflowStepInstanceExecutionContext{"+getWorkflowStepReference()+" / "+getName()+"}";
    }

    // standard deserialization method
    private WorkflowStepInstanceExecutionContext readResolve() {
        if (errorLegacyDeserialized!=null && errorRecord==null) {
            setError(errorLegacyDeserialized);
            errorLegacyDeserialized = null;
        }
        return this;
    }

}
