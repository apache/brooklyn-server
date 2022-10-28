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

import com.google.common.reflect.TypeToken;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.predicates.ResolutionFailureTreatedAsAbsent;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class WorkflowExpressionResolution {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExpressionResolution.class);
    private final WorkflowExecutionContext context;
    private final boolean allowWaiting;
    private final boolean useWrappedValue;

    public WorkflowExpressionResolution(WorkflowExecutionContext context, boolean allowWaiting, boolean wrapExpressionValues) {
        this.context = context;
        this.allowWaiting = allowWaiting;
        this.useWrappedValue = wrapExpressionValues;
    }

    TemplateModel ifNoMatches() {
        // this causes the execution to fail. any other behaviour is hard with freemarker.
        // recommendation is to use freemarker attempts/escapes to recover.
        return null;
    }

    class WorkflowFreemarkerModel implements TemplateHashModel {
        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            if ("workflow".equals(key)) {
                return new WorkflowExplicitModel();
            }
            if ("entity".equals(key)) {
                Entity entity = context.getEntity();
                if (entity!=null) {
                    return TemplateProcessor.EntityAndMapTemplateModel.forEntity(entity, null);
                }
            }

            Object candidate;

            //workflow.current_step.input.somevar
            WorkflowStepInstanceExecutionContext currentStep = context.currentStepInstance;
            if (currentStep!=null) {
                if (currentStep.output instanceof Map) {
                    candidate = ((Map) currentStep.output).get(key);
                    if (candidate!=null) return TemplateProcessor.wrapAsTemplateModel(candidate);
                }

                candidate = currentStep.getInput(key, Object.class);
                if (candidate!=null) return TemplateProcessor.wrapAsTemplateModel(candidate);
            }
            //workflow.previous_step.output.somevar
            Object prevStepOutput = context.getPreviousStepOutput();
            if (prevStepOutput instanceof Map) {
                candidate = ((Map)prevStepOutput).get(key);
                if (candidate!=null) return TemplateProcessor.wrapAsTemplateModel(candidate);
            }

            //workflow.scratch.somevar
            candidate = context.workflowScratchVariables.get(key);
            if (candidate!=null) return TemplateProcessor.wrapAsTemplateModel(candidate);

            //workflow.input.somevar
            if (context.input.containsKey(key)) {
                candidate = context.getInput(key);
                if (candidate != null) return TemplateProcessor.wrapAsTemplateModel(candidate);
            }

            return ifNoMatches();
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }
    }

    class WorkflowExplicitModel implements TemplateHashModel {
        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            //id (a token representing an item uniquely within its root instance)
            if ("name".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.getName());
            if ("id".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.getWorkflowId());
            if ("task_id".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.getTaskId());

            // TODO variable reference for link
            //link (a link in the UI to this instance of workflow or step)

            //error (if there is an error in scope)
            WorkflowStepInstanceExecutionContext currentStepInstance = context.currentStepInstance;
            WorkflowStepInstanceExecutionContext errorHandlerContext = context.errorHandlerContext;
            if ("error".equals(key)) return TemplateProcessor.wrapAsTemplateModel(errorHandlerContext!=null ? errorHandlerContext.error : null);

            if ("input".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.input);
            if ("output".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.output);

            //current_step.yyy and previous_step.yyy (where yyy is any of the above)
            //step.xxx.yyy ? - where yyy is any of the above and xxx any step id
            if ("error_handler".equals(key)) return new WorkflowStepModel(errorHandlerContext);
            if ("current_step".equals(key)) return new WorkflowStepModel(currentStepInstance);
            if ("previous_step".equals(key)) return newWorkflowStepModelForStepIndex(context.previousStepIndex);
            if ("step".equals(key)) return new WorkflowStepModel();

            if ("var".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.workflowScratchVariables);

            return ifNoMatches();
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }
    }

    TemplateModel newWorkflowStepModelForStepIndex(Integer step) {
        WorkflowExecutionContext.OldStepRecord stepI = context.oldStepInfo.get(step);
        if (stepI==null || stepI.context==null) return ifNoMatches();
        return new WorkflowStepModel(stepI.context);
    }
    TemplateModel newWorkflowStepModelForStepId(String id) {
        for (WorkflowExecutionContext.OldStepRecord s: context.oldStepInfo.values()) {
            if (s.context!=null && id.equals(s.context.stepDefinitionDeclaredId)) return new WorkflowStepModel(s.context);
        }
        return ifNoMatches();
    }

    class WorkflowStepModel implements TemplateHashModel {
        private WorkflowStepInstanceExecutionContext step;

        WorkflowStepModel() {}
        WorkflowStepModel(WorkflowStepInstanceExecutionContext step) {
            this.step = step;
        }
        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            if (step==null) {
                return newWorkflowStepModelForStepId(key);
            }

            //id (a token representing an item uniquely within its root instance)
            if ("name".equals(key)) {
                return TemplateProcessor.wrapAsTemplateModel(step.name != null ? step.name : step.getWorkflowStepReference());
            }
            if ("task_id".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.taskId);
            if ("step_id".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.stepDefinitionDeclaredId);
            if ("step_index".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.stepIndex);

            // TODO link and error, as above
            //link (a link in the UI to this instance of workflow or step)
            //error (if there is an error in scope)

            if ("input".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.input);
            if ("output".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.output!=null ? step.output : MutableMap.of());

            return ifNoMatches();
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }
    }

    public <T> T resolveWithTemplates(Object expression, TypeToken<T> type) {
        expression = processTemplateExpression(expression);
        return resolveCoercingOnly(expression, type);
    }

    /** does not use templates */
    public <T> T resolveCoercingOnly(Object expression, TypeToken<T> type) {
        try {
            if (expression==null || Jsonya.isTypeJsonPrimitiveCompatible(expression)) {
                // only try yaml coercion, as values are normally set from yaml and will be raw at this stage (but not if they are from a DSL)
                // (might be better to always to TC.coerce)
                return BeanWithTypeUtils.convert(context.getManagementContext(), expression, type, true,
                        RegisteredTypes.getClassLoadingContext(context.getEntity()), false);
            } else {
                return TypeCoercions.coerce(expression, type);
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            try {
                // fallback to simple coercion
                return TypeCoercions.coerce(expression, type);
            } catch (Exception e2) {
                Exceptions.propagateIfFatal(e2);
                throw Exceptions.propagate(e);
            }
        }
    }

    public Object processTemplateExpression(Object expression) {
        if (expression instanceof String) return processTemplateExpressionString((String)expression);
        if (expression instanceof Map) return processTemplateExpressionMap((Map)expression);
        if (expression instanceof Collection) return processTemplateExpressionCollection((Collection)expression);
        if (expression==null || Boxing.isPrimitiveOrBoxedObject(expression)) return expression;
        // otherwise resolve DSL
        return resolveDsl(expression);
    }

    private Object resolveDsl(Object expression) {
        return Tasks.resolving(expression).as(Object.class).context(context.getEntity()).get();
    }

    public Object processTemplateExpressionString(String expression) {
        if (expression==null) return null;
        if (expression.startsWith("$brooklyn:")) {
            return processTemplateExpression(resolveDsl(expression));
        }

        TemplateHashModel model = new WorkflowFreemarkerModel();
        Object result;

        if (!allowWaiting) Thread.currentThread().interrupt();
        try {
            result = TemplateProcessor.processTemplateContents("workflow", expression, model, true, false);
        } catch (Exception e) {
            Exception e2 = e;
            if (!allowWaiting && Exceptions.isCausedByInterruptInAnyThread(e)) {
                e2 = new IllegalArgumentException("Expression value '"+expression+"' unavailable and not permitted to wait: "+ Exceptions.collapseText(e), e);
            }
            if (useWrappedValue) {
                // in wrapped value mode, errors don't throw until accessed, and when used in conditions they can be tested as absent
                return WrappedResolvedExpression.ofError(expression, new ResolutionFailureTreatedAsAbsent.ResolutionFailureTreatedAsAbsentDefaultException(e2));
            } else {
                throw Exceptions.propagate(e2);
            }
        } finally {
            if (!allowWaiting) {
                // clear interrupt status
                Thread.interrupted();
            }
        }

        if (useWrappedValue) {
            if (!expression.equals(result)) return new WrappedResolvedExpression<Object>(expression, result);
        }

        return result;
    }

    public Map<?,?> processTemplateExpressionMap(Map<?,?> object) {
        Map<Object,Object> result = MutableMap.of();
        object.forEach((k,v) -> result.put(processTemplateExpression(k), processTemplateExpression(v)));
        return result;

    }

    protected Collection<?> processTemplateExpressionCollection(Collection<?> object) {
        return object.stream().map(x -> processTemplateExpression(x)).collect(Collectors.toList());
    }

    public static class WrappedResolvedExpression<T> implements DeferredSupplier<T> {
        String expression;
        T value;
        Throwable error;
        public WrappedResolvedExpression() {}
        public WrappedResolvedExpression(String expression, T value) {
            this.expression = expression;
            this.value = value;
        }
        public static WrappedResolvedExpression ofError(String expression, Throwable error) {
            WrappedResolvedExpression result = new WrappedResolvedExpression(expression, null);
            result.error = error;
            return result;
        }
        @Override
        public T get() {
            if (error!=null) {
                throw Exceptions.propagate(error);
            }
            return value;
        }
        public String getExpression() {
            return expression;
        }
        public Throwable getError() {
            return error;
        }
    }

}
