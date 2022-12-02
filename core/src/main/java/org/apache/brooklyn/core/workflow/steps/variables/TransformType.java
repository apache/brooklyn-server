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
import org.apache.brooklyn.util.collections.MutableSet;

import java.util.List;
import java.util.Set;

public class TransformType extends WorkflowTransformDefault {
    private TypeToken<?> type;

    @Override
    public void init(WorkflowExecutionContext context, List<String> definition) {
        super.init(context, null);
        Set<String> d = MutableSet.copyOf(definition.subList(1, definition.size()));
        if (d.isEmpty()) throw new IllegalArgumentException("Transform 'type' requires a type argument");
        if (d.size() > 1)
            throw new IllegalArgumentException("Transform 'type' requires a single arugment being the type; not " + d);
        type = context.lookupType(Iterables.getOnlyElement(d), () -> null);
    }

    @Override
    public Object apply(Object v) {
        return context.resolveCoercingOnly(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, v, type);
    }
}
