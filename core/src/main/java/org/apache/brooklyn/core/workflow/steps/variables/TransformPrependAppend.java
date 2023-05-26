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

import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;

import java.util.List;

public class TransformPrependAppend extends WorkflowTransformDefault {

    boolean atEnd;
    String expression;

    public TransformPrependAppend(boolean atEnd) {
        this.atEnd = atEnd;
    }

    @Override
    protected void initCheckingDefinition() {
        List<String> d = MutableList.copyOf(definition.subList(1, definition.size()));
        if (d.size()!=1) throw new IllegalArgumentException("Transform requires a single argument being the item to prepend or append");
        expression = d.get(0);
    }

    @Override
    public Object apply(Object v) {
        Object expressionResolved = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, expression, Object.class);
        if (v instanceof String || Boxing.isPrimitiveOrBoxedObject(v)) {
            if (!(expressionResolved instanceof String || Boxing.isPrimitiveOrBoxedObject(expressionResolved))) {
                throw new IllegalStateException("Argument must be a string or primitive to prepend/append");
            }
            if (atEnd) return Strings.toString(v) + expressionResolved;
            else return expressionResolved + Strings.toString(v);
        } else if (v instanceof Iterable) {
            List list = MutableList.copyOf((Iterable)v);
            if (atEnd) list.add(expressionResolved);
            else list.add(0, expressionResolved);
            return list;
        } else {
            throw new IllegalStateException("Input must be a list or a string/primitive to prepend/append");
        }
    }

}
