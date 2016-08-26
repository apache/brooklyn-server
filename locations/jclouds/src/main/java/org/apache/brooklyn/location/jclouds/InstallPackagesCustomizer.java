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
package org.apache.brooklyn.location.jclouds;

import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import org.jclouds.compute.ComputeService;

import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.ssh.BashCommands;

/**
 * Wraps {@link BashCommands#installPackages} in a {@link JcloudsLocationCustomizer} for easy consumption
 * in YAML blueprints:
 *
 * <pre>
 *   brooklyn.initializers:
 *   - type: org.apache.brooklyn.location.jclouds.InstallPackagesCustomizer
 *     brooklyn.config:
 *       onlyIfMissing: curl
 *       default: curl
 *       packages
 *         apt: curl=2.3.4
 *         yum:
 *         - ssl-utils
 *         - curl
 * </pre>
 */
public class InstallPackagesCustomizer extends BasicJcloudsLocationCustomizer {

    public static final ConfigKey<Map<String, List<String>>> PACKAGES = ConfigKeys.newConfigKey(
            new TypeToken<Map<String, List<String>>>() { },
            "packages", 
            "Map of packages to install, keyed on package manager",
            ImmutableMap.<String, List<String>>of());

    public static final ConfigKey<String> ONLY_IF_MISSING = ConfigKeys.newStringConfigKey(
            "onlyIfMissing", 
            "Only install packages if this executable cannot be found");

    public static final ConfigKey<List<String>> DEFAULT_PACKAGES = ConfigKeys.newConfigKey(
            new TypeToken<List<String>>() { },
            "default", 
            "List of packages to install (if no package manager specified)");

    public InstallPackagesCustomizer(Map<String, String> params) {
        super(params);
    }

    public InstallPackagesCustomizer(ConfigBag params) {
        super(params);
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
        Preconditions.checkArgument(machine instanceof SshMachineLocation, "machine must be SshMachineLocation, but is %s", machine.getClass());

        // Setup the bash command for package installation
        Map<String, List<String>> packages = MutableMap.copyOf(params.get(PACKAGES));
        Map<String, String> installFlags = MutableMap.<String, String>of();
        for (String packageManager : packages.keySet()) {
            installFlags.put(packageManager, Joiner.on(' ').join(packages.get(packageManager)));
        }
        if (params.containsKey(ONLY_IF_MISSING)) {
            installFlags.put("onlyIfMissing", params.get(ONLY_IF_MISSING));
        }
        List<String> defaultPackages = MutableList.copyOf(params.get(DEFAULT_PACKAGES));
        String installCommand = BashCommands.installPackage(installFlags, defaultPackages.isEmpty() ? null : Joiner.on(' ').join(defaultPackages));

        // Execute SSH task to install packages
        Task<?> task = SshEffectorTasks.ssh((SshMachineLocation) machine)
                .add(BashCommands.sudo(installCommand))
                .summary("Installing packages")
                .newTask()
                .asTask();
        DynamicTasks.queueIfPossible(task).orSubmitAndBlock();
        DynamicTasks.waitForLast();
    }
}
