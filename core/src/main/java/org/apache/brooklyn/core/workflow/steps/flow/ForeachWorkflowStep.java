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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;

public class ForeachWorkflowStep extends CustomWorkflowStep {

    public static final String SHORTHAND = "${target_var_name} [ \" in \" ${target...} ]";

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
        return "foreach".equals(typeBestGuess);
    }

}
