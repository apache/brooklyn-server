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

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;

import java.util.List;
import java.util.stream.Collectors;

public class TransformSlice extends WorkflowTransformDefault {
    private String startIndex;
    private String endIndex;

    @Override
    protected void initCheckingDefinition() {
        List<String> d = MutableList.copyOf(definition.subList(1, definition.size()));
        if (d.isEmpty()) throw new IllegalArgumentException("Transform 'slice' requires arguments: <start_index> [<end_index>]");
        startIndex = d.remove(0);
        if (!d.isEmpty()) endIndex = d.remove(0);
        if (!d.isEmpty()) throw new IllegalArgumentException("Transform 'slice' takes max 2 arguments: <start_index> [<end_index>]");
    }

    @Override
    public Object apply(Object v) {
        if (v instanceof String) {
            List<Integer> codePoints = ((String) v).codePoints().boxed().collect(Collectors.toList());
            return applyList(codePoints).stream()
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append )
                    .toString();
        } else {
            List list = context.resolveCoercingOnly(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, v, TypeToken.of(List.class));
            if (list==null) throw new IllegalArgumentException("List to slice is null");
            return applyList(list);
        }
    }

    protected <T> List<T> applyList(List<T> v) {
        List list = context.resolveCoercingOnly(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, v, TypeToken.of(List.class));
        if (list==null) throw new IllegalArgumentException("List to slice is null");

        Integer from = resolveAs(startIndex, context, "First argument start-index", true, Integer.class, "an integer");
        Integer to = resolveAs(endIndex, context, "Second argument end-index", false, Integer.class, "an integer");

        if (from<0) from = list.size() + from;
        if (from<0 || from>list.size()) throw new IllegalArgumentException("Invalid start index "+from+"; list is size "+list.size());
        if (to==null) to = list.size();
        if (to<0) to = list.size() + to;
        if (to<0 || to>list.size()) throw new IllegalArgumentException("Invalid end index "+to+"; list is size "+list.size());

        if (to<from) throw new IllegalArgumentException("Invalid end index "+to+"; should be greater than or equal to start index "+from);
        return list.subList(from, to);
    }

    public static <T> T resolveAs(Object expression, WorkflowExecutionContext context, String errorPrefix, boolean failIfNull, Class<T> type, String typeName) {
        T result = null;
        try {
            if (expression!=null) result = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, expression, type);
        } catch (Exception e) {
            throw Exceptions.propagate(errorPrefix + " must be "+typeName+" ("+expression+")", e);
        }
        if (failIfNull && result==null) throw new IllegalArgumentException(errorPrefix+" is required");
        return result;
    }

}
