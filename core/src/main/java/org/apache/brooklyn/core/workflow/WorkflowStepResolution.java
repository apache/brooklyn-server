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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;

import java.util.List;
import java.util.Map;

public class WorkflowStepResolution {

    public static List<WorkflowStepDefinition> resolveSteps(ManagementContext mgmt, List<Object> steps) {
        return resolveSteps(mgmt, steps, null);
    }
    public static List<WorkflowStepDefinition> resolveSteps(ManagementContext mgmt, List<Object> steps, Object outputDefinition) {
        List<WorkflowStepDefinition> result = MutableList.of();
        if (steps==null || steps.isEmpty()) {
            if (outputDefinition==null) throw new IllegalStateException("No steps defined in workflow and no output set");
            // if there is output, an empty workflow makes sense
            return result;
        }
        for (int i=0; i<steps.size(); i++) {
            try {
                result.add(resolveStep(mgmt, steps.get(i)));
            } catch (Exception e) {
                throw Exceptions.propagateAnnotated("Error in definition of step "+(i+1)+" ("+steps.get(i)+")", e);
            }
        }
        WorkflowExecutionContext.validateSteps(mgmt, result, true);
        return result;
    }

    public static List<WorkflowStepDefinition> resolveSubSteps(ManagementContext mgmt, String scope, List<Object> subSteps) {
        List<WorkflowStepDefinition> result = MutableList.of();
        if (subSteps!=null) {
            subSteps.forEach(subStep -> {
                WorkflowStepDefinition subStepResolved = resolveStep(mgmt, subStep);
                if (subStepResolved.getId() != null)
                    throw new IllegalArgumentException("Sub steps for "+scope+" are not permitted to have IDs: " + subStep);
                if (subStepResolved instanceof CustomWorkflowStep && ((CustomWorkflowStep)subStepResolved).peekSteps()!=null)
                    throw new IllegalArgumentException("Sub steps for "+scope+" are not permitted to run sub-workflows: " + subStep);
                result.add(subStepResolved);
            });
        }
        return result;
    }

    static WorkflowStepDefinition resolveStep(ManagementContext mgmt, Object def) {
        if (def instanceof WorkflowStepDefinition) return (WorkflowStepDefinition) def;

        BrooklynClassLoadingContext loader = RegisteredTypes.getCurrentClassLoadingContextOrManagement(mgmt);
        String shorthand = null;

        Map defM = null;
        if (def instanceof String) {
            shorthand = (String) def;
            defM = MutableMap.of();
        } else if (def instanceof Map) {
            defM = MutableMap.copyOf((Map)def);
            if (!defM.containsKey("type")) {
                // if there isn't a type, pull out shorthand
                Object s = defM.remove("step");
                if (s == null) s = defM.remove("shorthand");
                if (s == null) s = defM.remove("s");
                if (s == null) {
                    if (defM.size()==1) {
                        // assume the colon caused it accidentally to be a map
                        s = Iterables.getOnlyElement(defM.keySet());
                        if (s instanceof String && ((String)s).contains(" ")) {
                            s = s + " : " + Iterables.getOnlyElement(defM.values());
                        } else {
                            s = null;
                        }
                    }
                    if (s==null) {
                        throw new IllegalArgumentException("Step definition must indicate a `type` or a `step` / `shorthand` / `s` (" + def + ")");
                    }
                }
                if (!(s instanceof String)) {
                    throw new IllegalArgumentException("step shorthand must be a string");
                }
                shorthand = (String) s;
            }
        }

        String userSuppliedShorthand = shorthand;
        if (shorthand!=null) {
            shorthand = shorthand.trim();
            int wordBreak = shorthand.indexOf(" ");
            if (defM.containsKey("type")) throw new IllegalStateException("Must not supply 'type' when shorthand is used for step");
            if (wordBreak<0) {
                defM.put("type", shorthand);
                shorthand = null;
            } else {
                defM.put("type", shorthand.substring(0, wordBreak));
                shorthand = shorthand.substring(wordBreak + 1).trim();
            }
        }

        String typeBestGuess = defM != null ? ""+defM.get("type") : null;

        try {
            Object def0 = defM !=null ? defM : def;
            def = BeanWithTypeUtils.convert(mgmt, def0, TypeToken.of(WorkflowStepDefinition.class), true, loader, true);

            if (def instanceof WorkflowStepDefinition.WorkflowStepDefinitionWithSpecialDeserialization) {
                def = ((WorkflowStepDefinition.WorkflowStepDefinitionWithSpecialDeserialization)def).applySpecialDefinition(mgmt, def0, typeBestGuess, (WorkflowStepDefinition.WorkflowStepDefinitionWithSpecialDeserialization) def);
            }
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Unable to resolve step '"+def+"'", e);
        }

        if (def instanceof WorkflowStepDefinition) {
            WorkflowStepDefinition defW = (WorkflowStepDefinition) def;

            if (userSuppliedShorthand!=null) {
                defW.userSuppliedShorthand = userSuppliedShorthand;
            }
            if (typeBestGuess!=null) {
                defW.shorthandTypeName = typeBestGuess;
            }
            if (shorthand!=null) {
                defW.populateFromShorthand(shorthand);
            }

            List<Object> onError = WorkflowErrorHandling.wrappedInListIfNecessaryOrNullIfEmpty(defW.getOnError());
            if (onError!=null && !onError.isEmpty()) {
                defW.onError = resolveSubSteps(mgmt, "error handling", onError);
            }

            defW.validateStep(mgmt, null);

            return defW;
        } else {
            throw new IllegalArgumentException("Unable to resolve step; unexpected object "+ def);
        }
    }

    public static void validateWorkflowParameters(BrooklynObject entityOrAdjunctWhereRunningIfKnown, ConfigBag params) {
        List<Object> steps = params.get(WorkflowCommonConfig.STEPS);
        if (steps==null || steps.isEmpty()) throw new IllegalArgumentException("It is required to supply 'steps' to define a workflow effector");

        boolean hasCondition = params.containsKey(WorkflowCommonConfig.CONDITION.getName());
        if (!hasCondition && entityOrAdjunctWhereRunningIfKnown!=null) {
            // ideally try to resolve the steps at entity init time; except if a condition is required we skip that so you can have steps that only resolve late,
            // and if entity isn't available then we don't need that either
            WorkflowStepResolution.resolveSteps( ((BrooklynObjectInternal)entityOrAdjunctWhereRunningIfKnown).getManagementContext(), steps, params.containsKey(WorkflowCommonConfig.OUTPUT.getName()) ? "has_output" : null);
        }
    }

    public static Maybe<Entity> findEntity(WorkflowStepInstanceExecutionContext context, Object entityO) {
        Entity entity=null;
        String entityId;
        if (entityO instanceof Entity) {
            entity = (Entity) entityO;
        } else if (entityO instanceof String || Boxing.isPrimitiveOrBoxedObject(entityO)) {
            entityId = entityO.toString();

            List<Entity> firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context.getEntity(), true,
                    Predicates.and(EntityPredicates.configEqualTo(BrooklynConfigKeys.PLAN_ID, entityId), x->true)::apply);
            if (firstGroupOfMatches.isEmpty()) {
                firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context.getEntity(), true,
                        Predicates.and(EntityPredicates.idEqualTo(entityId), x->true)::apply);
            }
            if (!firstGroupOfMatches.isEmpty()) {
                entity = firstGroupOfMatches.get(0);
            }

            if (entity==null) return Maybe.absent("Cannot find entity with id '"+entityId+"'");

        } else return Maybe.absent("Invalid expression for entity; must be a string or entity, not '"+entityO+"'");
        return Maybe.of(entity);
    }
}
