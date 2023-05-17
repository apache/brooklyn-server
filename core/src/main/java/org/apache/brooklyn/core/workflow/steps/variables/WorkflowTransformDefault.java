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

import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;

import java.util.List;

public abstract class WorkflowTransformDefault implements WorkflowTransformWithContext {
    protected WorkflowExecutionContext context;

    @Override
    public void init(WorkflowExecutionContext context, List<String> definition, String transformDef) {
        init(context, definition);
    }

    @Override
    public void init(WorkflowExecutionContext context, List<String> definition) {
        this.context = context;
        if (definition != null) checkDefinitionSize1(definition);
    }

    @Override
    public boolean isResolver() {
        return false;
    }

    static void checkDefinitionSize1(List<String> definition) {
        if (definition.size()>1) throw new IllegalArgumentException("Transform '"+ definition.get(0)+"' does not accept args: " + definition.subList(1, definition.size()));
    }
}
