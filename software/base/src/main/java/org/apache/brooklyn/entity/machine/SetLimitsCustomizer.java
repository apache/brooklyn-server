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
package org.apache.brooklyn.entity.machine;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.brooklyn.util.ssh.BashCommands.sudo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.brooklyn.api.location.BasicMachineLocationCustomizer;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks.SshEffectorTaskFactory;
import org.apache.brooklyn.core.objs.BasicConfigurableObject;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

/**
 * Sets the limits (such as 'nofile' and 'nproc') on an ssh'able machine. Currently only CentOS and RHEL are supported.
 * <p>
 * For example:
 * <pre>
 * {@code
 * brooklyn.catalog:
 *   ...
 *   item:
 *     type: org.apache.brooklyn.entity.machine.MachineEntity
 *     brooklyn.parameters:
 *     - name: ulimits
 *       type: java.util.List
 *       description: |
 *         Contents to add to the limits config file
 *       default:
 *         - "* soft nofile 16384"
 *         - "* hard nofile 16384"
 *         - "* soft nproc 16384"
 *         - "* hard nproc 16384"
 *     brooklyn.config:
 *       provisioning.properties:
 *         machineCustomizers:
 *           - $brooklyn:object:
 *               type: org.apache.brooklyn.entity.machine.SetLimitsCustomizer
 *               brooklyn.config:
 *                 contents: $brooklyn:config("ulimits")
 * }
 * </pre>
 */
@Beta
public class SetLimitsCustomizer extends BasicMachineLocationCustomizer implements Configurable {

    public static final Logger log = LoggerFactory.getLogger(SetLimitsCustomizer.class);

    public static final ConfigKey<String> FILE_NAME = ConfigKeys.newStringConfigKey(
            "file",
            "The limits conf file to append to (and to create if necessary)",
            "/etc/security/limits.d/50-brooklyn.conf");

    @SuppressWarnings("serial")
    public static final ConfigKey<List<String>> CONTENTS = ConfigKeys.newConfigKey(
            new TypeToken<List<String>>() {},
            "contents",
            "The contents to be appended to the limits file",
            ImmutableList.<String>of());

    private final BasicConfigurableObject.BasicConfigurationSupport config;

    public SetLimitsCustomizer() {
        config = new BasicConfigurableObject.BasicConfigurationSupport();
    }

    @Override
    public ConfigurationSupport config() {
        return config;
    }

    @Override
    public <T> T getConfig(ConfigKey<T> key) {
        return config().get(key);
    }

    @Override
    public void customize(MachineLocation machine) {
        if (!(machine instanceof SshMachineLocation)) {
            throw new IllegalStateException("Machine must be a SshMachineLocation, but got "+machine);
        }
        String file = config.get(FILE_NAME);
        List<String> contents = config.get(CONTENTS);
        checkArgument(Strings.isNonBlank(config.get(FILE_NAME)), "File must be non-empty");
        
        log.info("SetLimitsCustomizer setting limits on "+machine+" in file "+file+" to: "+Joiner.on("; ").join(contents));

        try {
            List<String> cmds = new ArrayList<>();
            for (String content : contents) {
                cmds.add(sudo(String.format("echo \"%s\" | tee -a %s", content, file)));
            }
            exec((SshMachineLocation)machine, true, cmds.toArray(new String[cmds.size()]));
        } catch (Exception e) {
            log.info("SetLimitsCustomizer failed to set limits on "+machine+" (rethrowing)", e);
            throw e;
        }
    }

    protected ProcessTaskWrapper<Integer> exec(SshMachineLocation machine, boolean asRoot, String... cmds) {
        SshEffectorTaskFactory<Integer> taskFactory = SshEffectorTasks.ssh(machine, cmds).configure(SshMachineLocation.CLOSE_CONNECTION, true);
        if (asRoot) taskFactory.runAsRoot();
        ProcessTaskWrapper<Integer> result = DynamicTasks.queue(taskFactory).block();
        if (result.get() != 0) {
            throw new IllegalStateException("SetLimitsCustomizer got exit code "+result.get()+" executing on machine "+machine
                    +"; cmds="+Arrays.asList(cmds)+"; stdout="+result.getStdout()+"; stderr="+result.getStderr());
        }
        return result;
    }
}
