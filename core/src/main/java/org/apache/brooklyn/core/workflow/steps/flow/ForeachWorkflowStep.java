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
package org.apache.brooklyn.core.workflow.steps.flow;

import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.util.collections.MutableMap;

import java.util.Map;

public class ForeachWorkflowStep extends CustomWorkflowStep {

    public static final String SHORTHAND = "${target_var_name} [ \" in \" ${target...} ]";

    public static final String SHORTHAND_TYPE_NAME_DEFAULT = "foreach";

    public ForeachWorkflowStep() {}

    public ForeachWorkflowStep(CustomWorkflowStep base) {
        super(base);
    }

    public void setTarget(Object x) { this.target = x; }
    public void setTargetVarName(Object x) { this.target_var_name = x; }

    @Override
    public void populateFromShorthand(String value) {
        if (input==null) input = MutableMap.of();
        populateFromShorthandTemplate(SHORTHAND, value);

        if (input.containsKey("target")) target = input.remove("target");
        target_var_name = input.remove("target_var_name");
    }

    protected Iterable checkTarget(Object targetR) {
        if (targetR instanceof Iterable) return (Iterable)targetR;

        throw new IllegalArgumentException("Target of foreach must be a list or an expression that resolves to a list, not "+targetR);
    }

    protected boolean isPermittedToSetSteps(String typeBestGuess) {
        return typeBestGuess==null || SHORTHAND_TYPE_NAME_DEFAULT.equals(typeBestGuess) || ForeachWorkflowStep.class.getName().equals(typeBestGuess);
    }

    protected void initializeSubWorkflowForTarget(WorkflowStepInstanceExecutionContext context, Object target, WorkflowExecutionContext nestedWorkflowContext) {
        if (target_var_name instanceof String) {
            String tvn = ((String) target_var_name).trim();
            if (tvn.startsWith("{") && tvn.endsWith("}")) {
                String[] spreadVars = tvn.substring(1, tvn.length() - 1).split(",");
                if (!(target instanceof Map)) throw new IllegalStateException("Spread vars indicated in foreach but target is not a map");
                nestedWorkflowContext.getWorkflowScratchVariables().put(TARGET_VAR_NAME_DEFAULT, target);
                for (String spreadVar: spreadVars) {
                    String svt = spreadVar.trim();
                    nestedWorkflowContext.getWorkflowScratchVariables().put(svt, ((Map)target).get(svt));
                }
                return;
            }
        }

        super.initializeSubWorkflowForTarget(context, target, nestedWorkflowContext);
    }

    public void setIdempotent(String idempotent) {
        this.idempotent = idempotent;
    }

    public String getIdempotent() {
        return idempotent;
    }

    public void setConcurrency(Object concurrency) {
        this.concurrency = concurrency;
    }

    public Object getConcurrency() {
        return concurrency;
    }

    public void setWorkflowOutput(Object x) {
        this.workflowOutput = x;
    }

}
