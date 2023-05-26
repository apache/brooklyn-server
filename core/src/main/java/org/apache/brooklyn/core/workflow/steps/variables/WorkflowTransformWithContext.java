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

public interface WorkflowTransformWithContext extends WorkflowTransform {

    void init(WorkflowExecutionContext context, WorkflowStepInstanceExecutionContext stepContext, List<String> definition, String transformDef);

    /** returns true, false, or null (don't care) as to whether the resolver
     *  expects the value to be resolved;
     *  if true, the value will be resolved before being passed to the transform;
     *  if false, it will be an error if it has already been resolved;
     *  if null, no resolution will be done and it is assumed the transformer doesn't care */
    Boolean resolvedValueRequirement();
    /** whether the resolver returns a resolved value;
     *  null (almost always the default) means no change is required by the caller,
     *  but if it expected an unresolved expression and resolved it, it should return true;
     *  or if it has replaced it by an unresolved expression, it should return false */
    Boolean resolvedValueReturned();
}
