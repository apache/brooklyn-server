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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAdjuncts.EntityAdjunctProxyable;
import org.apache.brooklyn.core.mgmt.internal.AbstractManagementContext;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.SubWorkflowStep;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;

public class WorkflowStepResolution {

    public static List<WorkflowStepDefinition> resolveSteps(ManagementContext mgmt, List<Object> steps) {
        return resolveSteps(mgmt, steps, null);
    }
    public static List<WorkflowStepDefinition> resolveSteps(ManagementContext mgmt, List<Object> steps, Object outputDefinition) {
        return new WorkflowStepResolution(mgmt, null, null).resolveSteps(steps, outputDefinition);
    }
    public static List<WorkflowStepDefinition> resolveSubSteps(ManagementContext mgmt, String scope, List<Object> subSteps) {
        return new WorkflowStepResolution(mgmt, null, null).resolveSubSteps(scope, subSteps);
    }

    private final ManagementContext _mgmt;
    private final BrooklynObject _broolynObject;
    private final WorkflowExecutionContext _workflow;

    public WorkflowStepResolution(WorkflowExecutionContext context) {
        this(null, null, context);
    }
    public WorkflowStepResolution(BrooklynObject bo) {
        this(null, bo, null);
    }
    public WorkflowStepResolution(ManagementContext mgmt, BrooklynObject bo, WorkflowExecutionContext workflow) {
        this._mgmt = mgmt;
        this._broolynObject = bo;
        this._workflow = workflow;
    }

    public ManagementContext mgmt() {
        if (_mgmt!=null) return _mgmt;
        if (_workflow!=null) return _workflow.getManagementContext();
        BrooklynObject bo = brooklynObject();
        if (bo instanceof BrooklynObjectInternal) return ((BrooklynObjectInternal)bo).getManagementContext();
        if (bo instanceof EntityAdjunctProxyable) return ((EntityAdjunctProxyable)bo).getManagementContext();
        return null;
    }

    public BrooklynObject brooklynObject() {
        if (_broolynObject!=null) return _broolynObject;
        if (_workflow!=null) return _workflow.getEntityOrAdjunctWhereRunning();
        return null;
    }

    public Entity entity() {
        BrooklynObject bo = brooklynObject();
        if (bo==null || (bo instanceof Entity)) return (Entity) bo;
        if (bo instanceof EntityAdjunctProxyable) return ((EntityAdjunctProxyable)bo).getEntity();
        return null;
    }

    public List<WorkflowStepDefinition> resolveSteps(List<Object> steps, Object outputDefinition) {
        List<WorkflowStepDefinition> result = MutableList.of();
        if (steps==null || steps.isEmpty()) {
            if (outputDefinition==null) throw new IllegalStateException("No steps defined in workflow and no output set");
            // if there is output, an empty workflow makes sense
            return result;
        }
        for (int i=0; i<steps.size(); i++) {
            try {
                result.add(resolveStep(steps.get(i)));
            } catch (Exception e) {
                throw Exceptions.propagateAnnotated("Error in definition of step "+(i+1)+" ("+steps.get(i)+")", e);
            }
        }
        WorkflowExecutionContext.validateSteps(mgmt(), result, true);
        return result;
    }

    public List<WorkflowStepDefinition> resolveSubSteps(String scope, List<Object> subSteps) {
        List<WorkflowStepDefinition> result = MutableList.of();
        if (subSteps!=null) {
            subSteps.forEach(subStep -> {
                WorkflowStepDefinition subStepResolved = resolveStep(subStep);
                if (subStepResolved.getId() != null)
                    throw new IllegalArgumentException("Sub steps for "+scope+" are not permitted to have IDs: " + subStep);
                // don't allow foreach and workflow with target, but do allow subworkflow and if
                if (subStepResolved instanceof CustomWorkflowStep && !(subStepResolved instanceof SubWorkflowStep) &&
                        ((CustomWorkflowStep)subStepResolved).peekSteps()!=null)
                    throw new IllegalArgumentException("Sub steps for "+scope+" are not permitted to run custom or foreach sub-workflows: " + subStep);
                result.add(subStepResolved);
            });
        }
        return result;
    }

    public WorkflowStepDefinition resolveStep(Object def) {
        if (def instanceof WorkflowStepDefinition) return (WorkflowStepDefinition) def;

        BrooklynClassLoadingContext loader = RegisteredTypes.getClassLoadingContextMaybe(brooklynObject()).or(() -> RegisteredTypes.getCurrentClassLoadingContextOrManagement(mgmt()));
        String shorthand = null;

        Map defM = null;

        if (def instanceof List) {
            // list treated as implicit subworkflow, eg step: [ "sleep 1" ] = step: { steps: [ "sleep 1" ] }
            def = MutableMap.of(SubWorkflowStep.SHORTHAND_TYPE_NAME_DEFAULT, def);
        }

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

                if (s==null && defM.containsKey(WorkflowCommonConfig.STEPS.getName())) {
                    // if it has steps, but no step or s, assume it is a subworkflow
                    s = SubWorkflowStep.SHORTHAND_TYPE_NAME_DEFAULT;
                }

                if (s == null && defM.size()==1) {
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
                if (s instanceof Map && defM.size()==1) {
                    // allow shorthand to contain a nested map if the shorthand is the only thing in the map, eg { step: { step: "xxx" } }
                    return resolveStep(s);
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

            // if it's unable to convert a complex type via the above, the original type will be returned; the above doesn't fail.
            // this is checked below so it's not a serious error, but the reason for it might be obscured.
            Callable<Object> converter = () -> BeanWithTypeUtils.convert(mgmt(), def0, TypeToken.of(WorkflowStepDefinition.class), true, loader, true);
            Entity entity = entity();
            if (entity==null) {
                def = converter.call();
            } else {
                // run in a task context if we can, to facilitate conversion and type lookup
                def = Entities.submit(entity, Tasks.create("convert steps", converter)).getUnchecked();
            }

            if (def instanceof WorkflowStepDefinition.WorkflowStepDefinitionWithSpecialDeserialization) {
                def = ((WorkflowStepDefinition.WorkflowStepDefinitionWithSpecialDeserialization)def).applySpecialDefinition(mgmt(), def0, typeBestGuess, (WorkflowStepDefinition.WorkflowStepDefinitionWithSpecialDeserialization) def);
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
                defW.onError = resolveSubSteps("error handling", onError);
            }

            defW.validateStep(mgmt(), null);

            return defW;
        } else {
            throw new IllegalArgumentException("Unable to resolve step; unexpected object "+ def);
        }
    }

    public static void validateWorkflowParametersForEffector(BrooklynObject entityOrAdjunctWhereRunningIfKnown, ConfigBag params) {
        List<Object> steps = params.get(WorkflowCommonConfig.STEPS);
        if ((steps==null || steps.isEmpty()) && !params.containsKey(WorkflowCommonConfig.OUTPUT))
            throw new IllegalArgumentException("It is required to supply 'steps' or 'output' to define a workflow effector");

        boolean hasCondition = params.containsKey(WorkflowCommonConfig.CONDITION.getName());
        if (!hasCondition && entityOrAdjunctWhereRunningIfKnown!=null) {
            // ideally try to resolve the steps at entity init time; except if a condition is required we skip that so you can have steps that only resolve late,
            // and if entity isn't available then we don't need that either
            new WorkflowStepResolution(entityOrAdjunctWhereRunningIfKnown).resolveSteps(steps, params.containsKey(WorkflowCommonConfig.OUTPUT.getName()) ? "has_output" : null);
        }
    }

    public static Maybe<Entity> findEntity(WorkflowStepInstanceExecutionContext context, Object entityO) {
        return AbstractManagementContext.findEntity(context.getEntity(), entityO);
    }

}
