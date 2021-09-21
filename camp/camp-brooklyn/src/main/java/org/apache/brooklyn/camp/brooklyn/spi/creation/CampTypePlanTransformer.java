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
package org.apache.brooklyn.camp.brooklyn.spi.creation;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredType.TypeImplementationPlan;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.typereg.*;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;

public class CampTypePlanTransformer extends AbstractTypePlanTransformer {

    private static final List<String> FORMATS = ImmutableList.of("brooklyn-camp");
    // TODO any use in having these formats? if not, remove. Nov 2015.
    // , "camp", "brooklyn");
    
    public static final String FORMAT = FORMATS.get(0);
    
    public CampTypePlanTransformer() {
        super(FORMAT, "OASIS CAMP / Brooklyn", "The Apache Brooklyn implementation of the OASIS CAMP blueprint plan format and extensions");
    }

    @Override
    protected double scoreForNullFormat(Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        double weight;
        if (type!=null) {
            if (type.getKind()==RegisteredTypeKind.SPEC) {
                weight = 1.0;
            } else if (type.getKind()==RegisteredTypeKind.UNRESOLVED) {
                // might be a template
                weight = 0.4;
            } else {
                return 0;
            }
        } else {
            // have a plan but no type, weaken slightly
            weight = 0.8;
        }
        
        Maybe<Map<?,?>> plan = RegisteredTypes.getAsYamlMap(planData);
        if (plan.isPresent()) {
            return weight * scoreObject(plan.get(), (map,s) -> map.containsKey(s));
        }
        if (planData==null) {
            return 0;
        }
        double unparseableScore = scoreObject(planData.toString(), (p,s) -> p.contains(s));
        if (unparseableScore>0) {
            return 0.5 + 0.25 * unparseableScore;
        }
        return 0;
    }
    
    protected <T> double scoreObject(T plan, BiFunction<T, String, Boolean> contains) {
        if (contains.apply(plan, "services")) return 0.8;
        if (contains.apply(plan, "type")) return 0.4;
        if (contains.apply(plan, "brooklyn.locations")) return 0.7;
        if (contains.apply(plan, "brooklyn.policies")) return 0.7;
        if (contains.apply(plan, "brooklyn.enrichers")) return 0.7;
        // score low so we can say it's the wrong place
        if (contains.apply(plan, "catalog.bom")) return 0.2;
        return 0;
    }

    @Override
    protected double scoreForNonmatchingNonnullFormat(String planFormat, Object planData, RegisteredType type, RegisteredTypeLoadingContext context) {
        if (type!=null && type.getKind()!=RegisteredTypeKind.SPEC) return 0;

        if (FORMATS.contains(planFormat.toLowerCase())) return 0.9;
        return 0;
    }

    @Override
    public double scoreForType(RegisteredType type, RegisteredTypeLoadingContext context) {
        if (RegisteredTypeKind.BEAN.equals(type.getKind())) return 0;
        return super.scoreForType(type, context);
    }

    @Override
    protected AbstractBrooklynObjectSpec<?, ?> createSpec(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        try {
            return decorateWithCommonTagsModifyingSpecSummary(new CampResolver(mgmt, type, context).createSpec(),
                    type, null, null, prevHeadSpecSummary ->
                            prevHeadSpecSummary.summary.startsWith(prevHeadSpecSummary.format) ? "Based on "+prevHeadSpecSummary.summary :
                                    prevHeadSpecSummary.summary);

        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            String message = null;
            // check a few common errors, annotate if so
            if (type==null || type.getPlan()==null || type.getPlan().getPlanData()==null) {
                // shouldn't happen
                message = "Type/plan/data is null when resolving CAMP blueprint";
            } else {
                Object planData = type.getPlan().getPlanData();
                if (RegisteredTypes.getAsYamlMap(planData).isAbsent()) {
                    message = "Type or plan is invalid YAML when resolving CAMP blueprint";
                } else if (planData.toString().contains("brooklyn.catalog")) {
                    message = "CAMP blueprint for type definition looks like a catalog file";
                } else {
                    // leave null, don't annotate
                }
            }
            if (message!=null) {
                throw Exceptions.propagateAnnotated(message, e);
            } else {
                throw e;
            }
        }
    }

    @Override
    protected Object createBean(RegisteredType type, RegisteredTypeLoadingContext context) throws Exception {
        // beans not supported by this?
        throw new UnsupportedTypePlanException("beans not supported here");
    }

    public static class CampTypeImplementationPlan extends AbstractFormatSpecificTypeImplementationPlan<String> {
        public CampTypeImplementationPlan(TypeImplementationPlan otherPlan) {
            super(FORMATS.get(0), String.class, otherPlan);
        }
        public CampTypeImplementationPlan(String planData) {
            this(new BasicTypeImplementationPlan(FORMATS.get(0), planData));
        }
    }
}
