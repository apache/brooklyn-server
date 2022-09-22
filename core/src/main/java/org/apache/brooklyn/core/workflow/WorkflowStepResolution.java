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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.NaturalOrderComparator;
import org.apache.brooklyn.util.text.Strings;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.checkerframework.checker.units.UnitsTools.s;

public class WorkflowStepResolution {

    static Map<String,WorkflowStepDefinition> resolveSteps(ManagementContext mgmt, Map<String,Object> steps) {
        Map<String,WorkflowStepDefinition> result = MutableMap.of();
        if (steps==null || steps.isEmpty()) throw new IllegalStateException("No steps defined in workflow");
        steps.forEach((name, def) -> result.put(name, resolveStep(mgmt, name, def)));
        return result;
    }

    static WorkflowStepDefinition resolveStep(ManagementContext mgmt, String name, Object def) {
        BrooklynClassLoadingContext loader = RegisteredTypes.getCurrentClassLoadingContextOrManagement(mgmt);

//        // OLD SHORTHAND PROPOSAL - single key map
//        if (def instanceof Map) {
//            Map<String,Object> map = (Map<String,Object>) def;
//            if (map.size()==1 && !map.containsKey("type")) {
//                // shorthand definition. use string constructor.
//                Map.Entry<String, Object> ent = map.entrySet().iterator().next();
//                def = MutableMap.of("type", ent.getKey(), "shorthandValue", ent.getValue());
//            }
//        }

        // NEW SHORTHAND PROPOSAL - string, type in first word, remainder as shorthand
        BiConsumer<String,Map> extractShorthand = (s, map) -> {
            int wordBreak = s.indexOf(" ");
            if (wordBreak < 0) {
                map.put("type", s);
            } else {
            }
        };

        String shorthand = null;

        if (def instanceof String) {
            shorthand = (String) def;
            def = MutableMap.of();
        } else if (def instanceof Map) {
            Map defM = (Map)def;
            if (!defM.containsKey("type")) {
                // if there isn't a type, pull out shorthand
                Object s = defM.remove("shorthand");
                if (s == null) s = defM.remove("s");
                if (s == null) {
                    throw new IllegalArgumentException("Step definition must indicate a `type` or a `shorthand` / `s`");
                }
                if (!(s instanceof String)) {
                    throw new IllegalArgumentException("shorthand must be a string");
                }
                shorthand = (String) s;
            }
        }

        if (shorthand!=null) {
            Map defM = (Map)def;
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

        String typeBestGuess = def instanceof Map ? ""+((Map)def).get("type") : null;

        try {
            def = BeanWithTypeUtils.convert(mgmt, def, TypeToken.of(WorkflowStepDefinition.class), true, loader, false);
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Unable to resolve step "+ name, e);
        }
        if (def instanceof WorkflowStepDefinition) {
            WorkflowStepDefinition defW = (WorkflowStepDefinition) def;

            // TODO might be a better place to put this metadata than to chuck it in to 'name'
            if (Strings.isBlank(defW.getName())) {
                defW.name = name;
            }
            if (Strings.isBlank(defW.getName())) {
                defW.name = typeBestGuess;
            }

            if (shorthand!=null) {
                defW.populateFromShorthand(shorthand);
            }
            defW.validateStep();
            return defW;
        } else {
            throw new IllegalArgumentException("Unable to resolve step "+ name +"; unexpected object "+ def);
        }
    }

    public static void validateWorkflowParameters(BrooklynObject entityOrAdjunctWhereRunningIfKnown, ConfigBag params) {
        Map<String, Object> steps = params.get(WorkflowCommonConfig.STEPS);
        if (steps==null || steps.isEmpty()) throw new IllegalArgumentException("It is required to supply 'steps' to define a workflow effector");

        Object condition = params.containsKey(WorkflowCommonConfig.CONDITION.getName());
        if (condition==null && entityOrAdjunctWhereRunningIfKnown!=null) {
            // ideally try to resolve the steps at entity init time; except if a condition is required we skip that so you can have steps that only resolve late,
            // and if entity isn't available then we don't need that either
            WorkflowStepResolution.resolveSteps( ((BrooklynObjectInternal)entityOrAdjunctWhereRunningIfKnown).getManagementContext(), steps);
        }
    }

}
