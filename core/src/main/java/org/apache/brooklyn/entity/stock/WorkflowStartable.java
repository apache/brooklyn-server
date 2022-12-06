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
package org.apache.brooklyn.entity.stock;

import com.google.common.base.Predicates;
import org.apache.brooklyn.api.catalog.CatalogConfig;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

import java.util.Arrays;

/**
 * Provides a Startable entity that runs workflow to start/stop and optionally restart
 */
@ImplementedBy(WorkflowStartableImpl.class)
public interface WorkflowStartable extends BasicStartable {

    @CatalogConfig(label = "Start Workflow", priority=5)
    ConfigKey<CustomWorkflowStep> START_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "start")
            .description("workflow to start the entity")
            .runtimeInheritance(BasicConfigInheritance.NEVER_INHERITED)
            .constraint(Predicates.notNull())
            .build();

    @CatalogConfig(label = "Stop Workflow", priority=1)
    ConfigKey<CustomWorkflowStep> STOP_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "stop")
            .description("workflow to stop the entity")
            .runtimeInheritance(BasicConfigInheritance.NEVER_INHERITED)
            .constraint(Predicates.notNull())
            .build();

    static final CustomWorkflowStep DEFAULT_RESTART_WORKFLOW = new CustomWorkflowStep("Restart (default workflow)", Arrays.asList("invoke-effector stop", "invoke-effector start"));

    @CatalogConfig(label = "Restart Workflow", priority=1)
    ConfigKey<CustomWorkflowStep> RESTART_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "restart")
            .description("workflow to restart the entity; default is to stop then start")
            .runtimeInheritance(BasicConfigInheritance.NEVER_INHERITED)
            .defaultValue(DEFAULT_RESTART_WORKFLOW)
            .build();

}
