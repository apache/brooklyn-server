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
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;

import java.util.List;

import static org.apache.brooklyn.core.workflow.WorkflowExecutionContext.STEP_TARGET_NAME_FOR_END;

public abstract class WorkflowTransformDefault implements WorkflowTransformWithContext {
    protected WorkflowExecutionContext context;
    protected WorkflowStepInstanceExecutionContext stepContext;
    protected List<String> definition;
    protected String transformDef;

    @Override
    public void init(WorkflowExecutionContext context, WorkflowStepInstanceExecutionContext stepContext, List<String> definition, String transformDef) {
        this.context = context;
        this.stepContext = stepContext;
        this.definition = definition;
        this.transformDef = transformDef;
        initCheckingDefinition();
        initCheckingTransformPermitted();
    }

    protected void initCheckingTransformPermitted() {
        if (stepContext.next == STEP_TARGET_NAME_FOR_END) {
            throw new IllegalStateException("Cannot perform transform after 'return'");
        }
    }

    @Override public Boolean resolvedValueRequirement() { return true; }
    @Override public Boolean resolvedValueReturned() { return null; }

    /** default is to fail if any arguments (size>1) but subclasses can put in different constraints, eg requiring arguments or inspecting the first word */
    protected void initCheckingDefinition() {
        if (definition!=null && definition.size()>1) throw new IllegalArgumentException("Transform '"+ definition.get(0)+"' does not accept args: " + definition.subList(1, definition.size()));
    }
}
