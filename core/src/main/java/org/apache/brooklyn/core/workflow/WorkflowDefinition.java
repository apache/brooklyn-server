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
package org.apache.brooklyn.core.workflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.brooklyn.api.mgmt.ManagementContext;

import java.util.Map;

// TODO do we even need this class?
public class WorkflowDefinition {

    Map<String, Object> steps;

    public void setSteps(Map<String,Object> steps) {
        this.steps = steps;
    }

    public void validate(ManagementContext mgmt) {
        getStepsResolved(mgmt);
    }

    @JsonIgnore
    public Map<String, WorkflowStepDefinition> getStepsResolved(ManagementContext mgmt) {
        return WorkflowStepResolution.resolveSteps(mgmt, steps);
    }

//    public Task<?> newTask() {
//        coerceSteps();
//    }



}