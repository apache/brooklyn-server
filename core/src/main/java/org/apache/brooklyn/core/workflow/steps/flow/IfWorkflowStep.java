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
package org.apache.brooklyn.core.workflow.steps.flow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution.WorkflowExpressionStage;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution.WrappedResolvedExpression;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution.WrappingMode;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.predicates.DslPredicates.DslEntityPredicateDefault;
import org.apache.brooklyn.util.core.predicates.DslPredicates.DslPredicate;
import org.apache.brooklyn.util.core.predicates.DslPredicates.DslPredicateDefault;
import org.apache.brooklyn.util.core.predicates.DslPredicates.WhenPresencePredicate;
import org.apache.brooklyn.util.core.task.DeferredSupplier;

public class IfWorkflowStep extends SubWorkflowStep {

    // more conditions could be done in future, or (probably better) we bake it in to the interpolation langauge
    public static final String SHORTHAND = "${condition_target} [ \"==\" ${condition_equals} ] [ \" then \" ${step...} ]";

    public static final String SHORTHAND_TYPE_NAME_DEFAULT = "if";

    protected boolean isInternalClassNotExtendedAndUserAllowedToSetMostThings(String typeBestGuess) {
        return !isRegisteredTypeExtensionToClass(IfWorkflowStep.class, SHORTHAND_TYPE_NAME_DEFAULT, typeBestGuess);
    }

    Object condition_target;
    Object condition_equals;

    @Override
    public void populateFromShorthand(String value) {
        if (input==null) input = MutableMap.of();
        populateFromShorthandTemplate(SHORTHAND, value, true, true);

        if (input.containsKey("step")) {
            Object step = input.remove("step");
            if (this.steps!=null) throw new IllegalArgumentException("Cannot set step in shorthand and steps in body");
            this.steps = MutableList.of(step);
        } else if (steps==null) throw new IllegalArgumentException("'if' step requires a step or steps");

        if (input.containsKey("condition_target")) {
            if (this.condition!=null) throw new IllegalArgumentException("Cannot set condition in shorthand and in body");
            this.condition_target = input.remove("condition_target");
            if (input.containsKey("condition_equals")) {
                this.condition_equals = input.remove("condition_equals");
            }
        } else if (condition == null) throw new IllegalArgumentException("'if' step requires a condition");
    }

    @Override
    public Object getConditionRaw() {
        // not accurate, but not used
        return super.getConditionRaw();
    }

    @JsonIgnore
    @Override
    public DslPredicate getConditionResolved(WorkflowStepInstanceExecutionContext context) {
        Map<String, Object> raw = MutableMap.of("target", getConditionRaw());
        Object conditionConstructed = this.condition;
        if (conditionConstructed==null) {
            boolean isCondition = false;
            boolean seemsStatic = false;
            Object k = condition_target;
            if (condition_equals==null) {
                if (condition_target instanceof Map) isCondition = true;
                else if (condition_target instanceof String) {
                    if (((String) condition_target).trim().startsWith("$")) isCondition = false;
                    if (((String) condition_target).trim().startsWith("{")) {
                        isCondition = true;
                        k = context.getWorkflowExectionContext().resolveWrapped(
                                WorkflowExpressionStage.STEP_RUNNING, condition_target, TypeToken.of(Map.class),
                                WrappingMode.WRAPPED_RESULT_DEFER_THROWING_ERROR_BUT_NO_RETRY);
                    } else seemsStatic = true;
                } else seemsStatic = true;
            }
            k = context.getWorkflowExectionContext().resolveWrapped(
                    WorkflowExpressionStage.STEP_RUNNING, k, TypeToken.of(isCondition ? DslEntityPredicateDefault.class : Object.class),
                    WrappingMode.WRAPPED_RESULT_DEFER_THROWING_ERROR_BUT_NO_RETRY);

            if (seemsStatic && !(k instanceof DslPredicate || k instanceof Boolean || k instanceof DeferredSupplier)) {
                throw new IllegalArgumentException("Argument supplied as condition target will always evaluate as true.");
            }

            if (isCondition) {
                if (WrappedValue.getMaybe( ((DslPredicateDefault<?>)k).implicitEquals ).map(
                        impEq -> condition_target.equals(impEq)).or(false)) {
                    // will probably have thrown error when coercing from map, so probably not needed, but for good measure
                    throw new IllegalArgumentException("Argument supplied as condition target could not be evaluated as condition.");
                }
                conditionConstructed = k;

            } else {
                Map c = MutableMap.of("target", WrappedResolvedExpression.ifNonDeferred(condition_target, k));

                if (condition_equals != null) {
                    Object v = context.getWorkflowExectionContext().resolveWrapped(
                            WorkflowExpressionStage.STEP_RUNNING, condition_equals, TypeToken.of(Object.class),
                            WrappingMode.WRAPPED_RESULT_DEFER_THROWING_ERROR_BUT_NO_RETRY);
                    c.put("check", WrappedResolvedExpression.ifNonDeferred(condition_equals, v));
                    c.put("assert", MutableMap.of("when", WhenPresencePredicate.PRESENT));  // when doing equals we need LHS and RHS to be present
                } else {
                    c.put("when", WhenPresencePredicate.TRUTHY);
                }
                conditionConstructed = c;
            }
        }

        DslPredicate result = getConditionResolved(context, conditionConstructed);

        return result;
    }
}
