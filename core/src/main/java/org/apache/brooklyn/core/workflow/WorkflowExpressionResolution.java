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
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.ThreadLocalStack;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.predicates.ResolutionFailureTreatedAsAbsent;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.time.Time;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WorkflowExpressionResolution {

    public enum WorkflowExpressionStage implements Comparable<WorkflowExpressionStage> {
        WORKFLOW_INPUT,
        WORKFLOW_STARTING_POST_INPUT,
        STEP_PRE_INPUT,
        STEP_INPUT,
        STEP_RUNNING,
        STEP_OUTPUT,
        STEP_FINISHING_POST_OUTPUT,
        WORKFLOW_OUTPUT;

        public boolean after(WorkflowExpressionStage other) {
            return compareTo(other) > 0;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WorkflowExpressionResolution.class);
    private final WorkflowExecutionContext context;
    private final boolean allowWaiting;
    private final boolean useWrappedValue;
    private final WorkflowExpressionStage stage;
    private final TemplateProcessor.InterpolationErrorMode errorMode;

    public WorkflowExpressionResolution(WorkflowExecutionContext context, WorkflowExpressionStage stage, boolean allowWaiting, boolean wrapExpressionValues) {
        this(context, stage, allowWaiting, wrapExpressionValues, TemplateProcessor.InterpolationErrorMode.FAIL);
    }
    public WorkflowExpressionResolution(WorkflowExecutionContext context, WorkflowExpressionStage stage, boolean allowWaiting, boolean wrapExpressionValues, TemplateProcessor.InterpolationErrorMode errorMode) {
        this.context = context;
        this.stage = stage;
        this.allowWaiting = allowWaiting;
        this.useWrappedValue = wrapExpressionValues;
        this.errorMode = errorMode;
    }

    TemplateModel ifNoMatches() {
        // fail here - any other behaviour is hard with freemarker (exceptions intercepted etc).
        // error handling is done by 'process' method below, and by ?? notation handling in let,
        // or if needed freemarker attempts/escapes to recover could be used (not currently used much)
        return null;
    }

    class WorkflowFreemarkerModel implements TemplateHashModel, TemplateProcessor.UnwrappableTemplateModel {
        @Override
        public Maybe<Object> unwrap() {
            return Maybe.of(context);
        }

        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            List<Throwable> errors = MutableList.of();

            if ("workflow".equals(key)) {
                return new WorkflowExplicitModel();
            }
            if ("entity".equals(key)) {
                Entity entity = context.getEntity();
                if (entity!=null) {
                    return TemplateProcessor.EntityAndMapTemplateModel.forEntity(entity, null);
                }
            }

            if ("output".equals(key)) {
                if (context.getOutput()!=null) return TemplateProcessor.wrapAsTemplateModel(context.getOutput());
                if (context.currentStepInstance!=null && context.currentStepInstance.getOutput() !=null) return TemplateProcessor.wrapAsTemplateModel(context.currentStepInstance.getOutput());
                Object previousStepOutput = context.getPreviousStepOutput();
                if (previousStepOutput !=null) return TemplateProcessor.wrapAsTemplateModel(previousStepOutput);
                return ifNoMatches();
            }

            Object candidate;

            if (stage.after(WorkflowExpressionStage.STEP_PRE_INPUT)) {
                //somevar -> workflow.current_step.output.somevar
                WorkflowStepInstanceExecutionContext currentStep = context.currentStepInstance;
                if (currentStep != null && stage.after(WorkflowExpressionStage.STEP_OUTPUT)) {
                    if (currentStep.getOutput() instanceof Map) {
                        candidate = ((Map) currentStep.getOutput()).get(key);
                        if (candidate != null) return TemplateProcessor.wrapAsTemplateModel(candidate);
                    }
                }

                //somevar -> workflow.current_step.input.somevar
                try {
                    candidate = currentStep.getInput(key, Object.class);
                } catch (Throwable t) {
                    Exceptions.propagateIfFatal(t);
                    if (stage==WorkflowExpressionStage.STEP_INPUT && WorkflowVariableResolutionStackEntry.isStackForSettingVariable(RESOLVE_STACK.getAll(true), key) && Exceptions.getFirstThrowableOfType(t, WorkflowVariableRecursiveReference.class)!=null) {

                        // input evaluation can look at local input, and will gracefully handle some recursive references.
                        // this is needed so we can handle things like env:=${env} in input, and also {message:="Hi ${name}", name:="Bob"}.
                        // but there are
                        // if we have a chain input1:=input2, and input input2:=input1 with both defined on step and on workflow
                        //
                        // (a) eval of either will give recursive reference error and allow retry immediately;
                        //     then it's a bit weird, inconsistent, step input1 will resolve to local input2 which resolves as global input1;
                        //     but step input2 will resolve to local input1 which this time will resolve as global input2.
                        //     and whichever is invoked first will cause both to be stored as resolved, so if input2 resolved first then
                        //     step input1 subsequently returns global input2.
                        //
                        // (b) recursive reference error only recoverable at the outermost stage,
                        //     so step input1 = global input2, step input2 = global input1,
                        //     prevents inconsistency but blocks useful things, eg log ${message} wrapped with message:="Hi ${name}",
                        //     then invoked with name: "person who says ${message}" to refer to a previous step's message,
                        //     or even name:="Mr ${name}" to refer to an outer variable.
                        //     in this case if name is resolved first then message resolves as Hi Mr X, but if message resolved first
                        //     it only recovers when resolving message which would become "Hi X", and if message:="${greeting} ${name}"
                        //     then it fails to find a local ${greeting}. (with strategy (a) these both do what is expected.)
                        //
                        // (to handle this we include stage in the stack, needed in both cases above)
                        //
                        // ideally we would know which vars are from a wrapper, but that info is lost when we build up the step
                        //
                        // (c) we could just fail fast, disallow the nice things we wanted, require explicit
                        //
                        // (d) we could fail in edge cases, so the obvious cases above work as expected, but anything more sophisticated, eg A calling B calling A, will fail
                        //
                        // settled on (d) effectively; we allow local references, and fail on recursive references, with exceptions.
                        // the main exception, handled here, is if we are setting an input
                        candidate = null;
                        errors.add(t);
                    } else {
                        throw Exceptions.propagate(t);
                    }
                }
                if (candidate != null) return TemplateProcessor.wrapAsTemplateModel(candidate);
            }

            //workflow.previous_step.output.somevar
            if (stage.after(WorkflowExpressionStage.WORKFLOW_INPUT)) {
                Object prevStepOutput = context.getPreviousStepOutput();
                if (prevStepOutput instanceof Map) {
                    candidate = ((Map) prevStepOutput).get(key);
                    if (candidate != null) return TemplateProcessor.wrapAsTemplateModel(candidate);
                }
            }

            //workflow.scratch.somevar
            if (stage.after(WorkflowExpressionStage.WORKFLOW_INPUT)) {
                candidate = context.getWorkflowScratchVariables().get(key);
                if (candidate != null) return TemplateProcessor.wrapAsTemplateModel(candidate);
            }

            //workflow.input.somevar
            if (context.input.containsKey(key)) {
                candidate = context.getInput(key);
                // the subtlety around step input above doesn't apply here as workflow inputs are not resolved with freemarker
                if (candidate != null) return TemplateProcessor.wrapAsTemplateModel(candidate);
            }

            if (!errors.isEmpty()) Exceptions.propagate("Errors resolving "+key, errors);

            return ifNoMatches();
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }
    }

    class WorkflowExplicitModel implements TemplateHashModel, TemplateProcessor.UnwrappableTemplateModel {
        @Override
        public Maybe<Object> unwrap() {
            return Maybe.of(context);
        }

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
            if ("output".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.getOutput());

            //current_step.yyy and previous_step.yyy (where yyy is any of the above)
            //step.xxx.yyy ? - where yyy is any of the above and xxx any step id
            if ("error_handler".equals(key)) return new WorkflowStepModel(errorHandlerContext);
            if ("current_step".equals(key)) return new WorkflowStepModel(currentStepInstance);
            if ("previous_step".equals(key)) return newWorkflowStepModelForStepIndex(context.previousStepIndex);
            if ("step".equals(key)) return new WorkflowStepModel();
            if ("util".equals(key)) return new WorkflowUtilModel();

            if ("var".equals(key)) return TemplateProcessor.wrapAsTemplateModel(context.getWorkflowScratchVariables());

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
            if ("output".equals(key)) {
                Pair<Object, Set<Integer>> outputOfStep = context.getStepOutputAndBacktrackedSteps(step.stepIndex);
                Object output = (outputOfStep != null && outputOfStep.getLeft() != null) ? outputOfStep.getLeft() : MutableMap.of();
                return TemplateProcessor.wrapAsTemplateModel(output);
            }

            return ifNoMatches();
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }
    }

    class WorkflowUtilModel implements TemplateHashModel {

        WorkflowUtilModel() {}
        @Override
        public TemplateModel get(String key) throws TemplateModelException {

            //id (a token representing an item uniquely within its root instance)
            if ("now".equals(key)) return TemplateProcessor.wrapAsTemplateModel(System.currentTimeMillis());
            if ("now_utc".equals(key)) return TemplateProcessor.wrapAsTemplateModel(System.currentTimeMillis());
            if ("now_instant".equals(key)) return TemplateProcessor.wrapAsTemplateModel(Instant.now());
            if ("now_iso".equals(key)) return TemplateProcessor.wrapAsTemplateModel(Time.makeIso8601DateStringZ(Instant.now()));
            if ("now_stamp".equals(key)) return TemplateProcessor.wrapAsTemplateModel(Time.makeDateStampString());
            if ("now_nice".equals(key)) return TemplateProcessor.wrapAsTemplateModel(Time.makeDateString(Instant.now()));
            if ("random".equals(key)) return TemplateProcessor.wrapAsTemplateModel(Math.random());

            return ifNoMatches();
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }
    }

    AllowBrooklynDslMode defaultAllowBrooklynDsl = AllowBrooklynDslMode.ALL;

    public void setDefaultAllowBrooklynDsl(AllowBrooklynDslMode defaultAllowBrooklynDsl) {
        this.defaultAllowBrooklynDsl = defaultAllowBrooklynDsl;
    }

    public AllowBrooklynDslMode getDefaultAllowBrooklynDsl() {
        return defaultAllowBrooklynDsl;
    }

    public <T> T resolveWithTemplates(Object expression, TypeToken<T> type) {
        expression = processTemplateExpression(expression, getDefaultAllowBrooklynDsl());
        return resolveCoercingOnly(expression, type);
    }

    /** does not use templates */
    public <T> T resolveCoercingOnly(Object expression, TypeToken<T> type) {
        try {
            if (expression==null || (Jsonya.isJsonPrimitiveDeep(expression) && !(expression instanceof Set))) {
                // only try yaml coercion, as values are normally set from yaml and will be raw at this stage (but not if they are from a DSL)
                // (might be better to always to TC.coerce)
                return BeanWithTypeUtils.convert(context.getManagementContext(), expression, type, true,
                        RegisteredTypes.getClassLoadingContext(context.getEntity()), true /* needed for wrapped resolved holders */);
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

    static class WorkflowVariableResolutionStackEntry {
        WorkflowExecutionContext context;
        WorkflowExpressionStage stage;
        Object object;
        String settingVariable;

        public static WorkflowVariableResolutionStackEntry of(WorkflowExecutionContext context, WorkflowExpressionStage stage, Object expression) {
            WorkflowVariableResolutionStackEntry result = new WorkflowVariableResolutionStackEntry();
            result.context = context;
            result.stage = stage;
            result.object = expression;
            return result;
        }

        public static WorkflowVariableResolutionStackEntry setting(WorkflowExecutionContext context, WorkflowExpressionStage stage, String settingVariable) {
            WorkflowVariableResolutionStackEntry result = new WorkflowVariableResolutionStackEntry();
            result.context = context;
            result.stage = stage;
            result.settingVariable = settingVariable;
            return result;
        }

        public static boolean isStackForSettingVariable(Collection<WorkflowVariableResolutionStackEntry> stack, String key) {
            if (stack==null) return true;
            MutableList<WorkflowVariableResolutionStackEntry> s2 = MutableList.copyOf(stack);
            Collections.reverse(s2);
            Optional<WorkflowVariableResolutionStackEntry> s = s2.stream().filter(si -> si.settingVariable != null).findFirst();
            if (!s.isPresent()) return false;
            return s.get().settingVariable.equals(key);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            WorkflowVariableResolutionStackEntry that = (WorkflowVariableResolutionStackEntry) o;

            if (context != null && that.context != null ? !Objects.equals(context.getWorkflowId(), that.context.getWorkflowId()) : !Objects.equals(context, that.context)) return false;
            if (stage != that.stage) return false;
            if (object != null ? !object.equals(that.object) : that.object != null) return false;
            if (settingVariable != null ? !settingVariable.equals(that.settingVariable) : that.settingVariable != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = context != null && context.getWorkflowId()!=null ? context.getWorkflowId().hashCode() : 0;
            result = 31 * result + (stage != null ? stage.hashCode() : 0);
            result = 31 * result + (object != null ? object.hashCode() : 0);
            result = 31 * result + (settingVariable != null ? settingVariable.hashCode() : 0);
            return result;
        }
    }

    /** method which can be used to indicate that a reference to the variable, if it is recursive, is recoverable, because we are in the process of setting that variable.
     * see discussion on usages of WorkflowVariableResolutionStackEntry.isStackForSettingVariable */
    public static <T> T allowingRecursionWhenSetting(WorkflowExecutionContext context, WorkflowExpressionStage stage, String variable, Supplier<T> callable) {
        WorkflowVariableResolutionStackEntry entry = null;
        try {
            entry = WorkflowVariableResolutionStackEntry.setting(context, stage, variable);
            if (!RESOLVE_STACK.push(entry)) {
                entry = null;
                throw new WorkflowVariableRecursiveReference("Recursive reference setting "+variable+": "+RESOLVE_STACK.getAll(false).stream().map(p -> p.object!=null ? p.object.toString() : p.settingVariable).collect(Collectors.joining("->")));
            }

            return callable.get();

        } finally {
            if (entry!=null) {
                RESOLVE_STACK.pop(entry);
            }
        }
    }

    static ThreadLocalStack<WorkflowVariableResolutionStackEntry> RESOLVE_STACK = new ThreadLocalStack<>(false);

    WorkflowExpressionStage previousStage() {
        return RESOLVE_STACK.peekPenultimate().map(s -> s.stage).orNull();
    }

    public static class WorkflowVariableRecursiveReference extends IllegalArgumentException {
        public WorkflowVariableRecursiveReference(String msg) {
            super(msg);
        }
    }

    public static class AllowBrooklynDslMode {
        public static AllowBrooklynDslMode ALL = new AllowBrooklynDslMode(true, null);
        static { ALL.next = () -> ALL; }
        public static AllowBrooklynDslMode NONE = new AllowBrooklynDslMode(false, null);
        static { NONE.next = () -> NONE; }
        public static AllowBrooklynDslMode CHILDREN_BUT_NOT_HERE = new AllowBrooklynDslMode(false, ()->ALL);
        //public static AllowBrooklynDslMode HERE_BUT_NOT_CHILDREN = new AllowBrooklynDslMode(true, ()->NONE);

        private Supplier<AllowBrooklynDslMode> next;
        private boolean allowedHere;

        public AllowBrooklynDslMode(boolean allowedHere, Supplier<AllowBrooklynDslMode> next) {
            this.allowedHere = allowedHere;
            this.next = next;
        }

        public boolean isAllowedHere() { return allowedHere; }
        public AllowBrooklynDslMode next() { return next.get(); }
    }

    public Object processTemplateExpression(Object expression, AllowBrooklynDslMode allowBrooklynDsl) {
        WorkflowVariableResolutionStackEntry entry = null;
        try {
            entry = WorkflowVariableResolutionStackEntry.of(context, stage, expression);
            if (!RESOLVE_STACK.push(entry)) {
                entry = null;
                throw new WorkflowVariableRecursiveReference("Recursive reference: " + RESOLVE_STACK.getAll(false).stream().map(p -> "" + p.object).collect(Collectors.joining("->")));
            }
            if (RESOLVE_STACK.size() > 100) {
                throw new WorkflowVariableRecursiveReference("Reference exceeded max depth 100: " + RESOLVE_STACK.getAll(false).stream().map(p -> "" + p.object).collect(Collectors.joining("->")));
            }

            if (expression instanceof String) return processTemplateExpressionString((String) expression, allowBrooklynDsl);
            if (expression instanceof Map) return processTemplateExpressionMap((Map) expression, allowBrooklynDsl);
            if (expression instanceof Collection)
                return processTemplateExpressionCollection((Collection) expression, allowBrooklynDsl);
            if (expression == null || Boxing.isPrimitiveOrBoxedObject(expression)) return expression;
            // otherwise resolve DSL
            return allowBrooklynDsl.isAllowedHere() ? resolveDsl(expression) : expression;

        } finally {
            if (entry != null) RESOLVE_STACK.pop(entry);
        }
    }

    private Object resolveDsl(Object expression) {
        boolean DEFINITELY_DSL = false;
        if (expression instanceof String || expression instanceof Map || expression instanceof Collection) {
            if (expression instanceof String) {
                if (!((String)expression).startsWith("$brooklyn:")) {
                    // not DSL
                    return expression;
                } else {
                    DEFINITELY_DSL = true;
                }
            }
            if (BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings.BROOKLYN_PARSE_DSL_FUNCTION==null) {
                if (DEFINITELY_DSL) {
                    log.warn("BROOKLYN_PARSE_DSL_FUNCTION not set when processing DSL expression "+expression+"; will not be resolved");
                }
            } else {
                expression = BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings.BROOKLYN_PARSE_DSL_FUNCTION.apply(context.getManagementContext(), expression);
            }
        }
        return processDslComponents(expression);
    }

    private Object processDslComponents(Object expression) {
        return Tasks.resolving(expression).as(Object.class).deep().context(context.getEntity()).get();
    }

    public Object processTemplateExpressionString(String expression, AllowBrooklynDslMode allowBrooklynDsl) {
        if (expression==null) return null;
        if (expression.startsWith("$brooklyn:") && allowBrooklynDsl.isAllowedHere()) {

            Object expressionTemplateResolved = processTemplateExpressionString(expression, AllowBrooklynDslMode.NONE);
            Object expressionTemplateAndDslResolved = resolveDsl(expressionTemplateResolved);
            return expressionTemplateAndDslResolved;

            // previous to 2023-03-30, instead of above, we resolved DSL first. this meant DSL expressions that contained workflow expressions were allowed,
            // which might be useful but probably shouldn't be supported; and furthermore you couldn't pass workflow vars to DSL expressions which should be supported.
//            if (!Objects.equals(e2, expression)) {
//                if (e2 instanceof String) {
//                    // proceed to below
//                    expression = (String) e2;
//                } else {
//                    return processTemplateExpression(e2);
//                }
//            }

        }

        TemplateHashModel model = new WorkflowFreemarkerModel();
        Object result;

        boolean ourWait = interruptSetIfNeededToPreventWaiting();
        try {
            result = TemplateProcessor.processTemplateContents("workflow", expression, model, true, false, errorMode);
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
            if (ourWait) interruptClear();
        }

        if (!expression.equals(result)) {
            if (useWrappedValue) {
                return new WrappedResolvedExpression<Object>(expression, result);
            } else {
                // we try, but don't guarantee, that DSL expressions aren't re-resolved, ie $brooklyn:literal("$brooklyn:literal(\"x\")") won't return x;
                // this block will
                result = processDslComponents(result);
            }
        }

        return result;
    }

    private static ThreadLocal<Boolean> interruptSetIfNeededToPreventWaiting = new ThreadLocal<>();
    public static boolean isInterruptSetToPreventWaiting() {
        Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
        if (entity!=null && Entities.isUnmanagingOrNoLongerManaged(entity)) return false;
        return Boolean.TRUE.equals(interruptSetIfNeededToPreventWaiting.get());
    }
    private boolean interruptSetIfNeededToPreventWaiting() {
        if (!allowWaiting && !Thread.currentThread().isInterrupted() && !isInterruptSetToPreventWaiting()) {
            interruptSetIfNeededToPreventWaiting.set(true);
            Thread.currentThread().interrupt();
            return true;
        }
        return false;
    }
    private void interruptClear() {
        // clear interrupt status
        Thread.interrupted();
        interruptSetIfNeededToPreventWaiting.remove();
    }

    public Object processTemplateExpressionMap(Map<?,?> object, AllowBrooklynDslMode allowBrooklynDsl) {
        if (allowBrooklynDsl.isAllowedHere() && object.size()==1) {
            Object key = object.keySet().iterator().next();
            if (key instanceof String && ((String)key).startsWith("$brooklyn:")) {
                Object expressionTemplateValueResolved = processTemplateExpression(object.values().iterator().next(), allowBrooklynDsl.next());
                Object expressionTemplateAndDslResolved = resolveDsl(MutableMap.of(key, expressionTemplateValueResolved));
                return expressionTemplateAndDslResolved;
            }
        }

        Map<Object,Object> result = MutableMap.of();
        object.forEach((k,v) -> result.put(processTemplateExpression(k, allowBrooklynDsl.next()), processTemplateExpression(v, allowBrooklynDsl.next())));
        return result;

    }

    protected Collection<?> processTemplateExpressionCollection(Collection<?> object, AllowBrooklynDslMode allowBrooklynDsl) {
        return object.stream().map(x -> processTemplateExpression(x, allowBrooklynDsl.next())).collect(Collectors.toList());
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
