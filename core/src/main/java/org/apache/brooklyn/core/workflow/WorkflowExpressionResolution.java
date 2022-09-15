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

import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeToken;
import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class WorkflowExpressionResolution {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExpressionResolution.class);
    private final WorkflowExecutionContext context;
    private final boolean useWrappedValue;

    public WorkflowExpressionResolution(WorkflowExecutionContext context, boolean wrap) {
        this.context = context;
        this.useWrappedValue = wrap;
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
            WorkflowStepInstanceExecutionContext currentStep = context.lastInstanceOfEachStep.get(context.getCurrentStepId());
            if (currentStep!=null) {
                candidate = currentStep.input.get(key);
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
            candidate = context.input.get(key);
            if (candidate!=null) return TemplateProcessor.wrapAsTemplateModel(candidate);

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
            if ("id".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.getWorkflowInstanceId());
            //task_id (the ID of the current corresponding Brooklyn Task)
            if ("task_id".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.getTaskId());

            // TODO
            //link (a link in the UI to this instance of workflow or step)
            //error (if there is an error in scope)

            if ("input".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.input);
            if ("output".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.output);

            //current_step.yyy and previous_step.yyy (where yyy is any of the above)
            //step.xxx.yyy ? - where yyy is any of the above and xxx any step id
            if ("current_step".equals(key)) return newWorkflowStepModelForStep(context.getCurrentStepId());
            if ("previous_step".equals(key)) return newWorkflowStepModelForStep(context.getPreviousStepId());
            if ("step".equals(key)) return new WorkflowStepModel();

            return ifNoMatches();
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }
    }

    TemplateModel newWorkflowStepModelForStep(String step) {
        WorkflowStepInstanceExecutionContext stepI = context.lastInstanceOfEachStep.get(step);
        if (stepI==null) return ifNoMatches();
        return new WorkflowStepModel(stepI);
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
                return newWorkflowStepModelForStep(key);
            }

            //id (a token representing an item uniquely within its root instance)
            if ("name".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.name);
            if ("uid".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.stepInstanceId);
            if ("step_id".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.stepDefinitionId);

            //task_id (the ID of the current corresponding Brooklyn Task)
            if ("task_id".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.taskId);

            // TODO
            //link (a link in the UI to this instance of workflow or step)
            //error (if there is an error in scope)

            if ("input".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.input);
            if ("output".equals(key)) return TemplateProcessor.wrapAsTemplateModel(step.output);

            return ifNoMatches();
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }
    }

    public <T> T resolveWithTemplates(Object expression, TypeToken<T> type) {
        expression = processTemplateExpression(expression);
        try {
            // try yaml coercion, as values are normally set from yaml and will be raw at this stage
            return BeanWithTypeUtils.convert(((EntityInternal)this.context.getEntity()).getManagementContext(), expression, type, true,
                    RegisteredTypes.getClassLoadingContext(context.getEntity()), false);
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
        return expression;
    }

    public Object processTemplateExpressionString(String expression) {
        if (expression==null) return null;

        TemplateHashModel model = new WorkflowFreemarkerModel();
        String result = TemplateProcessor.processTemplateContents(expression, model);
        if (expression.equals(result)) return expression;

        if (useWrappedValue) return new WrappedResolvedExpression<Object>(expression, result);
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
        public WrappedResolvedExpression() {}
        public WrappedResolvedExpression(String expression, T value) {
            this.expression = expression;
            this.value = value;
        }
        @Override
        public T get() {
            return value;
        }
        public String getExpression() {
            return expression;
        }
    }

}
