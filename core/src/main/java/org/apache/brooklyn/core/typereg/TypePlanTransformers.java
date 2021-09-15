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
package org.apache.brooklyn.core.typereg;

import com.google.common.collect.*;
import java.util.*;

import java.util.function.Supplier;
import org.apache.brooklyn.api.framework.FrameworkLookup;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;

public class TypePlanTransformers {

    private static final Logger log = LoggerFactory.getLogger(TypePlanTransformers.class);

    private static Collection<BrooklynTypePlanTransformer> getAll() {
        return ImmutableList.copyOf(FrameworkLookup.lookupAll(BrooklynTypePlanTransformer.class));
    }

    private static Collection<Class<? extends BrooklynTypePlanTransformer>> OVERRIDE;
    @SafeVarargs
    @VisibleForTesting
    public synchronized static void forceAvailable(Class<? extends BrooklynTypePlanTransformer> ...classes) {
        OVERRIDE = Arrays.asList(classes);
    }
    public synchronized static void clearForced() {
        OVERRIDE = null;
    }

    public static Collection<BrooklynTypePlanTransformer> all(ManagementContext mgmt) {
        // TODO cache these in the TypeRegistry, looking for new ones periodically or supplying a way to register them
        Collection<Class<? extends BrooklynTypePlanTransformer>> override = OVERRIDE;
        Collection<BrooklynTypePlanTransformer> result = new ArrayList<BrooklynTypePlanTransformer>();
        if (override!=null) {
            for (Class<? extends BrooklynTypePlanTransformer> o1: override) {
                try {
                    result.add(o1.newInstance());
                } catch (Exception e) {
                    Exceptions.propagate(e);
                }
            }
        } else {
            result.addAll(getAll());
        }
        for(BrooklynTypePlanTransformer t : result) {
            t.setManagementContext(mgmt);
        }
        return result;
    }

    /** returns a list of {@link BrooklynTypePlanTransformer} instances for this {@link ManagementContext}
     * which may be able to handle the given plan; the list is sorted with highest-score transformer first */
    @Beta
    public static List<BrooklynTypePlanTransformer> forType(ManagementContext mgmt, RegisteredType type, RegisteredTypeLoadingContext constraint) {
        Multimap<Double,BrooklynTypePlanTransformer> byScoreMulti = TreeMultimap.create(Comparator.reverseOrder(), (r1, r2) -> 0);
        Collection<BrooklynTypePlanTransformer> transformers = all(mgmt);
        for (BrooklynTypePlanTransformer transformer : transformers) {
            double score = transformer.scoreForType(type, constraint);
            if (log.isTraceEnabled()) {
                log.trace("SCORE for '" + type + "' at " + transformer + ": " + score);
            }
            if (score>0) byScoreMulti.put(score, transformer);
        }
        return ImmutableList.copyOf(Iterables.concat(byScoreMulti.values()));
    }

    /** transforms the given type to an instance, if possible
     * <p>
     * callers should generally use one of the create methods on {@link BrooklynTypeRegistry} rather than using this method directly. */
    @Beta
    public static Maybe<Object> transform(ManagementContext mgmt, RegisteredType type, RegisteredTypeLoadingContext constraint) {
        if (type==null) return Maybe.absent("type cannot be null");
        if (type.getPlan()==null) return Maybe.absent("type plan cannot be null, when instantiating "+type);
        
        List<BrooklynTypePlanTransformer> transformers = forType(mgmt, type, constraint);
        Collection<String> transformersWhoDontSupport = new ArrayList<String>();
        List<Exception> failuresFromTransformers = new ArrayList<Exception>();
        for (BrooklynTypePlanTransformer t: transformers) {
            try {
                Object result = t.create(type, constraint);
                if (result==null) {
                    transformersWhoDontSupport.add(t.getFormatCode() + " (returned null)");
                    continue;
                }
                return Maybe.of(result);
            } catch (@SuppressWarnings("deprecation") org.apache.brooklyn.core.plan.PlanNotRecognizedException | UnsupportedTypePlanException e) {
                transformersWhoDontSupport.add(t.getFormatCode() +
                    (Strings.isNonBlank(e.getMessage()) ? " ("+e.getMessage()+")" : ""));
            } catch (Throwable e) {
                Exceptions.propagateIfFatal(e);
                if (log.isTraceEnabled()) {
                    log.trace("Transformer for "+t.getFormatCode()+" gave an error creating this plan (may retry): "+e, e);
                }
                PropagatedRuntimeException e1 = new PropagatedRuntimeException(
                        (type.getSymbolicName() != null ?
                                t.getFormatCode() + " plan creation error in " + type.getId() :
                                t.getFormatCode() + " plan creation error") + ": " +
                                Exceptions.collapseText(e), e);
                if (Exceptions.getFirstThrowableOfType(e, TypePlanException.class)!=null &&
                        (type.getPlan().getPlanFormat()==null || Objects.equals(type.getPlan().getPlanFormat(), t.getFormatCode()))) {
                    // prefer this type of exception unless format is specified and we are opportunistically trying a different converter
                    failuresFromTransformers.add(0, e1);
                } else {
                    failuresFromTransformers.add(e1);
                }
            }
        }
        
        if (log.isDebugEnabled()) {
            Supplier<String> s = () -> "Failure transforming plan; returning summary failure, but for reference "
                + "potentially applicable transformers were "+transformers+", "
                + "available ones are "+MutableList.builder().addAll(all(mgmt)).build()+"; "
                + "failures: "+failuresFromTransformers +"; "
                + "unsupported by: "+transformersWhoDontSupport;
            if (BasicBrooklynCatalog.currentlyResolvingType.get()==null) {
                log.debug(s.get());
            } else if (log.isTraceEnabled()) {
                log.trace(s.get());
            }
        }

        // failed
        Exception result;
        if (!failuresFromTransformers.isEmpty()) {
            // at least one thought he could do it
            result = failuresFromTransformers.size()==1 ? Exceptions.create(null, failuresFromTransformers) :
                Exceptions.create("All applicable plan transformers failed", failuresFromTransformers);
        } else {
            String prefix = Strings.isBlank(type.getPlan().getPlanFormat()) ? "Invalid plan" : "Invalid '"+type.getPlan().getPlanFormat()+"' plan";
            if (transformers.isEmpty()) {
                result = new UnsupportedTypePlanException(prefix + "; format could not be recognized, none of the available transformers "+all(mgmt)+" support "+
                    (type.getId()!=null ? type.getId() : "plan:\n"+ Sanitizer.sanitizeJsonTypes(type.getPlan().getPlanData())));
            } else {
                result = new UnsupportedTypePlanException(prefix + "; potentially applicable transformers "+transformers+" do not support it, and other available transformers "+
//                    // the removeAll call below won't work until "all" caches it
//                    MutableList.builder().addAll(all(mgmt)).removeAll(transformers).build()+" "+
                    "do not accept it");
            }
        }
        return Maybe.absent(result);
    }
    
}
