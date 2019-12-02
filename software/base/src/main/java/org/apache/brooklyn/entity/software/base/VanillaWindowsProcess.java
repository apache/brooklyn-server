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

import java.util.Collection;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableSet;

import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.catalog.CatalogConfig;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.TemplatedStringAttributeSensorAndConfigKey;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.time.Duration;

@Catalog(name="Vanilla Windows Process", description="A basic Windows entity configured with scripts, e.g. for launch, check-running and stop")
@ImplementedBy(VanillaWindowsProcessImpl.class)
public interface VanillaWindowsProcess extends AbstractVanillaProcess {

    @SetFromFlag("installDir")
    AttributeSensorAndConfigKey<String,String> INSTALL_DIR = new TemplatedStringAttributeSensorAndConfigKey(
        "install.dir", 
        "Directory in which this software will be installed (if downloading/unpacking artifacts explicitly); uses FreeMarker templating format",
        "${" +
        "config['"+BrooklynConfigKeys.ONBOX_BASE_DIR.getName()+"']!" +
        "config['"+BrooklynConfigKeys.BROOKLYN_DATA_DIR.getName()+"']!" +
        "'ERROR-ONBOX_BASE_DIR-not-set'" +
        "}" +
        "\\" +
        "installs\\" +
        // the  var??  tests if it exists, passing value to ?string(if_present,if_absent)
        // the ! provides a default value afterwards, which is never used, but is required for parsing
        // when the config key is not available;
        // thus the below prefers the install.unique_label, but falls back to simple name
        // plus a version identifier *if* the version is explicitly set
        "${(config['install.unique_label']??)?string(config['install.unique_label']!'X'," +
        "(entity.entityType.simpleName)+" +
        "((config['install.version']??)?string('_'+(config['install.version']!'X'),''))" +
        ")}");

    @SetFromFlag("runDir")
    AttributeSensorAndConfigKey<String,String> RUN_DIR = new TemplatedStringAttributeSensorAndConfigKey(
        "run.dir", 
        "Directory from which this software to be run; uses FreeMarker templating format",
        "${" +
        "config['"+BrooklynConfigKeys.ONBOX_BASE_DIR.getName()+"']!" +
        "config['"+BrooklynConfigKeys.BROOKLYN_DATA_DIR.getName()+"']!" +
        "'ERROR-ONBOX_BASE_DIR-not-set'" +
        "}" +
        "\\" +
        "apps\\${entity.applicationId}\\" +
        "entities\\${entity.entityType.simpleName}_" +
        "${entity.id}");

    
    // 3389 is RDP; 5985 is WinRM (3389 isn't used by Brooklyn, but useful for the end-user subsequently)
    ConfigKey<Collection<Integer>> REQUIRED_OPEN_LOGIN_PORTS = ConfigKeys.newConfigKeyWithDefault(
            SoftwareProcess.REQUIRED_OPEN_LOGIN_PORTS,
            ImmutableSet.of(5986, 5985, 3389));

    @CatalogConfig(label = "Install PowerShell command", priority=5.5)
    ConfigKey<String> INSTALL_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "install.powershell.command")
            .description("powershell command to run during the install phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Install Command", priority=5)
    ConfigKey<String> INSTALL_COMMAND = VanillaSoftwareProcess.INSTALL_COMMAND;

    @CatalogConfig(label = "Customize PowerShell command", priority=4.5)
    ConfigKey<String> CUSTOMIZE_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "customize.powershell.command")
            .description("powershell command to run during the customization phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Customize command", priority=4)
    ConfigKey<String> CUSTOMIZE_COMMAND = VanillaSoftwareProcess.CUSTOMIZE_COMMAND;

    @CatalogConfig(label = "Launch PowerShell command", priority=3.5)
    ConfigKey<String> LAUNCH_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "launch.powershell.command")
            .description("command to run to launch the process")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Launch Command", priority=3)
    ConfigKey<String> LAUNCH_COMMAND = ConfigKeys.newConfigKeyWithDefault(VanillaSoftwareProcess.LAUNCH_COMMAND, null);

    @CatalogConfig(label = "Check-running PowerShell Command", priority=2.5)
    ConfigKey<String> CHECK_RUNNING_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "checkRunning.powershell.command")
            .description("command to determine whether the process is running")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Check-running Command", priority=2)
    ConfigKey<String> CHECK_RUNNING_COMMAND = VanillaSoftwareProcess.CHECK_RUNNING_COMMAND;

    @CatalogConfig(label = "Stop PowerShell Command", priority=1.5)
    ConfigKey<String> STOP_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "stop.powershell.command")
            .description("command to run to stop the process")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    @CatalogConfig(label = "Stop Command", priority=1)
    ConfigKey<String> STOP_COMMAND = VanillaSoftwareProcess.STOP_COMMAND;

    ConfigKey<String> PRE_INSTALL_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "pre.install.powershell.command")
            .description("powershell command to run during the pre-install phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    ConfigKey<String> POST_INSTALL_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "post.install.powershell.command")
            .description("powershell command to run during the post-install phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    ConfigKey<String> PRE_CUSTOMIZE_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "pre.customize.powershell.command")
            .description("powershell command to run during the pre-customize phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    ConfigKey<String> POST_CUSTOMIZE_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "post.customize.powershell.command")
            .description("powershell command to run during the post-customize phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    ConfigKey<String> PRE_LAUNCH_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "pre.launch.powershell.command")
            .description("powershell command to run during the pre-launch phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    ConfigKey<String> POST_LAUNCH_POWERSHELL_COMMAND = ConfigKeys.builder(String.class, "post.launch.powershell.command")
            .description("powershell command to run during the post-launch phase")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    ConfigKey<Boolean> PRE_INSTALL_REBOOT_REQUIRED = ConfigKeys.newBooleanConfigKey("pre.install.reboot.required",
            "indicates that a reboot should be performed after the pre-install command is run", false);

    @Beta
    ConfigKey<Boolean> INSTALL_REBOOT_REQUIRED = ConfigKeys.newBooleanConfigKey("install.reboot.required",
            "indicates that a reboot should be performed after the install command is run." +
            "When running the install command and the reboot command this parameter adds computername when authenticating.",
            false);
    
    ConfigKey<Boolean> CUSTOMIZE_REBOOT_REQUIRED = ConfigKeys.newBooleanConfigKey("customize.reboot.required",
            "indicates that a reboot should be performed after the customize command is run", false);

    ConfigKey<Duration> REBOOT_BEGUN_TIMEOUT = ConfigKeys.newDurationConfigKey("reboot.begun.timeout",
            "duration to wait whilst waiting for a machine to begin rebooting, and thus become unavailable", Duration.TWO_MINUTES);
    
    // TODO If automatic updates are enabled and there are updates waiting to be installed, thirty minutes may not be sufficient...
    ConfigKey<Duration> REBOOT_COMPLETED_TIMEOUT = ConfigKeys.newDurationConfigKey("reboot.completed.timeout",
            "duration to wait whilst waiting for a machine to finish rebooting, and thus to become available again", Duration.minutes(30));

    AttributeSensor<Integer> RDP_PORT = Sensors.newIntegerSensor("rdp.port", "RDP port used by the machine of this entity.");
    AttributeSensor<Integer> WINRM_PORT = Sensors.newIntegerSensor("winrm.port", "WinRM port used by the machine of this entity.");

    /**
     * @deprecated since 0.9.0; use {@link #RDP_PORT} instead.
     */
    @Deprecated
    AttributeSensor<Integer> RDP_PORT_CAMEL_CASE = Sensors.newIntegerSensor("rdpPort", "[DEPRECATED] instead use 'rdp.port'");

    /**
     * @deprecated since 0.9.0; use {@link #WINRM_PORT} instead.
     */
    @Deprecated
    AttributeSensor<Integer> WINRM_PORT_CAMEL_CASE = Sensors.newIntegerSensor("winrmPort", "[DEPRECATED] instead use 'winrm.port'");

    /**
     * @deprecated since 0.9.0; use {@link #WINRM_PORT} instead.
     */
    @Deprecated
    AttributeSensor<Integer> WINRM_PORT_SHORTEN = Sensors.newIntegerSensor("port", "[DEPRECATED] instead use 'winrm.port'");
}
