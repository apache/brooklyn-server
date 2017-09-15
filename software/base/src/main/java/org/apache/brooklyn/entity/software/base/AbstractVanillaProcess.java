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

import org.apache.brooklyn.api.catalog.CatalogConfig;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;

public interface AbstractVanillaProcess extends SoftwareProcess {
    AttributeSensorAndConfigKey<String, String> DOWNLOAD_URL = SoftwareProcess.DOWNLOAD_URL;

    ConfigKey<String> SUGGESTED_VERSION = ConfigKeys.newConfigKeyWithDefault(SoftwareProcess.SUGGESTED_VERSION, "0.0.0");

    @CatalogConfig(label = "Install Command", priority=5)
    ConfigKey<String> INSTALL_COMMAND = ConfigKeys.builder(String.class, "install.command")
            .description("command to run during the install phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();
    
    @CatalogConfig(label = "Customize command", priority=4)
    ConfigKey<String> CUSTOMIZE_COMMAND = ConfigKeys.builder(String.class, "customize.command")
            .description("command to run during the customization phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();
    
    @CatalogConfig(label = "Launch Command", priority=3)
    ConfigKey<String> LAUNCH_COMMAND = ConfigKeys.builder(String.class, "launch.command")
            .description("command to run to launch the process")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();
    
    @CatalogConfig(label = "Check-running Command", priority=2)
    ConfigKey<String> CHECK_RUNNING_COMMAND = ConfigKeys.builder(String.class, "checkRunning.command")
            .description("command to determine whether the process is running")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();
    
    @CatalogConfig(label = "Stop Command", priority=1)
    ConfigKey<String> STOP_COMMAND = ConfigKeys.builder(String.class, "stop.command")
            .description("command to run to stop the process")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();
}
