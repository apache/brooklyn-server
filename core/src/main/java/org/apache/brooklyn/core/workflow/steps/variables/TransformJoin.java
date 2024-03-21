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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;

public class TransformJoin extends WorkflowTransformDefault {

    String separator;

    @Override
    protected void initCheckingDefinition() {
        List<String> d = MutableList.copyOf(definition.subList(1, definition.size()));
        if (d.size()>1) throw new IllegalArgumentException("Transform requires zero or one arguments being a token to insert between elements");
        if (!d.isEmpty()) separator = d.get(0);
    }

    @Override
    public Object apply(Object v) {
        Object separatorResolvedO = separator==null ? "" : context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, separator, Object.class);
        if (!(Boxing.isPrimitiveOrStringOrBoxedObject(separatorResolvedO))) {
            throw new IllegalStateException("Argument must be a string or primitive to use as the separator");
        }
        String separatorResolved = ""+separatorResolvedO;
        if (v instanceof Iterable) {
            List list = MutableList.copyOf((Iterable)v);
            return list.stream().map(x -> {
                if (!(Boxing.isPrimitiveOrStringOrBoxedObject(x))) {
                    throw new IllegalStateException("Elements in the list to join must be a strings or primitives");
                }
                return ""+x;
            }).collect(Collectors.joining(separatorResolved));
        } else {
            throw new IllegalStateException("Input must be a list to join");
        }
    }

}
