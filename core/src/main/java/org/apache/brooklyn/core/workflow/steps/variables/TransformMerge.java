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
package org.apache.brooklyn.core.workflow.steps.variables;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class TransformMerge extends WorkflowTransformDefault {

    private static final Logger log = LoggerFactory.getLogger(TransformMerge.class);

    boolean deep;
    boolean wait;
    boolean lax;
    boolean map;
    boolean list;
    boolean set;

    @Override
    public void init(WorkflowExecutionContext context, List<String> definition) {
        super.init(context, null);
        Set<String> d = MutableSet.copyOf(definition.subList(1, definition.size()));
        deep = d.remove("deep");
        wait = d.remove("wait");
        lax = d.remove("lax");
        map = d.remove("map");
        list = d.remove("list");
        set = d.remove("set");

        if (map && list) throw new IllegalArgumentException("Invalid arguments to merge; cannot specify map and list");
        if (map && set) throw new IllegalArgumentException("Invalid arguments to merge; cannot specify map and set");
        if (list && set) throw new IllegalArgumentException("Invalid arguments to merge; cannot specify list and set");

        if (!d.isEmpty()) throw new IllegalArgumentException("Unsupported merge arguments: " + d);
    }

    @Override
    public boolean isResolver() {
        return true;
    }

    QuotedStringTokenizer qst(String input) {
        return QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(true).expectQuotesDelimited(true).failOnOpenQuote(true).build(input);
    }

    @Override
    public Object apply(Object v) {
        Object result = null;

        if (v instanceof Iterable) {
            // given an explicit list; we will merge each item
        } else if (v instanceof String) {
            v = qst((String) v).remainderRaw();
        } else {
            throw new IllegalStateException("Invalid value to merge: " + v);
        }

        if (!map && !list && !set) {
            map = Iterables.any((Iterable) v, vi -> vi instanceof Map || ((vi instanceof String) && ((String) vi).startsWith("{")));
            list = Iterables.any((Iterable) v, vi -> vi instanceof Iterable || ((vi instanceof String) && ((String) vi).startsWith("[")));
        }

        if (map && list)
            throw new IllegalStateException("Invalid value to merge; contains combination of list and map");

        Class<?> type = map ? Map.class : list ? List.class : set ? Set.class : Object.class;
        Function<Class, Object> resultInit = t -> {
            if (t == Map.class) return MutableMap.of();
            if (t == List.class) return MutableList.of();
            if (t == Set.class) return MutableSet.of();
            throw new IllegalStateException("Unexpected type " + t);
        };

        for (Object vi : (Iterable) v) {
            try {
                vi = wait ? context.resolveWaiting(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, vi, TypeToken.of(type)) :
                        context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, vi, TypeToken.of(type));
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                if (lax) {
                    log.debug("Ignoring entry " + vi + " when transforming for merge because cannot be resolved to " + type.getName());
                    continue;
                } else {
                    throw Exceptions.propagate(e);
                }
            }

            if (vi instanceof Map) {
                type = Map.class;
                if (result == null) result = resultInit.apply(type);
                if (!(result instanceof Map))
                    throw new IllegalArgumentException("Invalid value to merge; contains a map when expected a list");

                if (deep) {
                    result = CollectionMerger.builder().build().merge((Map) result, (Map) vi);
                } else {
                    ((Map) result).putAll((Map) vi);
                }
            } else if (vi instanceof Collection) {
                type = set ? Set.class : List.class;
                if (result == null) result = resultInit.apply(type);
                if (!(result instanceof Collection))
                    throw new IllegalArgumentException("Invalid value to merge; contains a map when expected a list or set");
                if (deep && !lax)
                    throw new IllegalArgumentException("Invalid value to deep merge; deep only applies to maps and a collection was encountered");

                ((Collection) result).addAll((Collection) vi);
            } else {
                if (lax) {
                    // ignore
                } else {
                    throw new IllegalArgumentException("Invalid value to merge (specify lax mode to ignore): " + vi);
                }
            }
        }
        return result;
    }
}
