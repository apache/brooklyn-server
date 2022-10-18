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
package org.apache.brooklyn.entity.software.base;

import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.catalog.CatalogConfig;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;

/** 
 * Similar to {@link VanillaSoftwareProcess} but takes workflow objects for install, customize, launch, stop, and checkRunning
 */
@Catalog(name="Vanilla Software Process", description="A software process configured with workflow, e.g. for launch, check-running and stop")
@ImplementedBy(WorkflowSoftwareProcessImpl.class)
public interface WorkflowSoftwareProcess extends SoftwareProcess {

    @CatalogConfig(label = "Install Command", priority=5)
    ConfigKey<CustomWorkflowStep> INSTALL_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "install.workflow")
            .description("command to run during the install phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Customize command", priority=4)
    ConfigKey<CustomWorkflowStep> CUSTOMIZE_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "customize.workflow")
            .description("command to run during the customization phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Launch Command", priority=3)
    ConfigKey<CustomWorkflowStep> LAUNCH_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "launch.workflow")
            .description("command to run to launch the process")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Check-running Command", priority=2)
    ConfigKey<CustomWorkflowStep> CHECK_RUNNING_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "checkRunning.workflow")
            .description("command to determine whether the process is running")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Stop Command", priority=1)
    ConfigKey<CustomWorkflowStep> STOP_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "stop.workflow")
            .description("command to run to stop the process")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    ConfigKey<Boolean> USE_SSH_MONITORING = ConfigKeys.newConfigKey(
            "sshMonitoring.enabled", 
            "SSH monitoring enabled", 
            Boolean.TRUE);

    ConfigKey<Boolean> USE_PID_FILE = ConfigKeys.newConfigKey(
            WorkflowSoftwareProcessSshDriver.USE_PID_FILE,
            "Use a PID file to check running",
            Boolean.FALSE);
}
