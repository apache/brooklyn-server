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
package org.apache.brooklyn.feed.ssh;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.feed.CommandPollConfig;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.ssh.SshTasks;
import org.apache.brooklyn.util.core.task.ssh.internal.PlainSshExecTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskStub.ScriptReturnType;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Provides a feed of attribute values, by polling over ssh.
 * 
 * Example usage (e.g. in an entity that extends SoftwareProcessImpl):
 * <pre>
 * {@code
 * private SshFeed feed;
 * 
 * //@Override
 * protected void connectSensors() {
 *   super.connectSensors();
 *   
 *   feed = SshFeed.builder()
 *       .entity(this)
 *       .machine(mySshMachineLachine)
 *       .poll(new SshPollConfig<Boolean>(SERVICE_UP)
 *           .command("rabbitmqctl -q status")
 *           .onSuccess(new Function<SshPollValue, Boolean>() {
 *               public Boolean apply(SshPollValue input) {
 *                 return (input.getExitStatus() == 0);
 *               }}))
 *       .build();
 * }
 * 
 * {@literal @}Override
 * protected void disconnectSensors() {
 *   super.disconnectSensors();
 *   if (feed != null) feed.stop();
 * }
 * }
 * </pre>
 * 
 * @author aled
 */
public class SshFeed extends org.apache.brooklyn.feed.AbstractCommandFeed {
    public static final Logger log = LoggerFactory.getLogger(SshFeed.class);

    public static class Builder extends org.apache.brooklyn.feed.AbstractCommandFeed.Builder<SshFeed, Builder> {
        private List<CommandPollConfig<?>> polls = Lists.newArrayList();

        @Override
        public Builder poll(CommandPollConfig<?> config) {
            polls.add(config);
            return self();
        }

        @Override
        public List<CommandPollConfig<?>> getPolls() {
            return polls;
        }

        @Override
        protected Builder self() {
           return this;
        }
   
        @Override
        protected SshFeed instantiateFeed() {
           return new SshFeed(this);
        }
     }

    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * For rebind; do not call directly; use builder
     */
    public SshFeed() {
    }

    public SshFeed(final Builder builder) {
        super(builder);
    }

    @Override
    protected SshPollValue exec(String command, Map<String,String> env) throws IOException {
        SshMachineLocation machine = (SshMachineLocation)getMachine();
        if (log.isTraceEnabled()) log.trace("Ssh polling for {}, executing {} with env {}", new Object[] {machine, command, env});
        ProcessTaskFactory<String> tf = new PlainSshExecTaskFactory<String>(machine, command)
                .environmentVariables(env)
                .summary("ssh-feed")
                .<String>returning(ScriptReturnType.STDOUT_STRING)
                .allowingNonZeroExitCode()
                .configure(SshTool.PROP_NO_EXTRA_OUTPUT, true);

        Boolean execAsCommand = config().get(EXEC_AS_COMMAND);
        if (Boolean.TRUE.equals(execAsCommand)) {
            tf.runAsCommand();
        } else {
            tf.runAsScript();
        }
        tf.configure(config().getBag().getAllConfig());

        ProcessTaskWrapper<String> task = tf.newTask();
        DynamicTasks.queueIfPossible(task).orSubmitAndBlock(entity).andWaitForSuccess();

        return new SshPollValue(machine, task.getExitCode(), task.getStdout(), task.getStderr());
    }

    protected SshPollValue installAndExec(String commandUrl, Map<String,String> env) throws IOException {
        String commandUrlCopiedAs = config().get(COMMAND_URL_COPIED_AS);
        if (commandUrlCopiedAs==null) {
            synchronized (this) {
                commandUrlCopiedAs = config().get(COMMAND_URL_COPIED_AS);
                if (commandUrlCopiedAs==null) {
                    String installDir = getEntity().sensors().get(BrooklynConfigKeys.INSTALL_DIR);
                    if (installDir == null) {
                        commandUrlCopiedAs = "brooklyn-ssh-command-url-" + entity.getApplicationId() + "-" + entity.getId() + "-" + Identifiers.makeRandomId(4) + ".sh";
                        log.debug("Install dir not available at " + getEntity() + "; will use default/home directory for "+this+", in "+commandUrlCopiedAs);
                    } else {
                        commandUrlCopiedAs = Os.mergePathsUnix(installDir, "command-url-" + Identifiers.makeRandomId(4) + ".sh");
                    }

                    // Look for SshMachineLocation and install remote command script.
                    Maybe<SshMachineLocation> locationMaybe = Locations.findUniqueSshMachineLocation(entity.getLocations());
                    if (locationMaybe.isPresent()) {
                        TaskFactory<?> install = SshTasks.installFromUrl(locationMaybe.get(), commandUrl, commandUrlCopiedAs);
                        DynamicTasks.queueIfPossible(install.newTask()).orSubmitAsync(entity).andWaitForSuccess();
                        log.debug("Installed from "+commandUrl+" to "+commandUrlCopiedAs+" at "+getEntity());
                    } else {
                        throw new IllegalStateException("Ssh machine location not available at " + getEntity() + "; skipping run of " + this);
                    }

                    config().set(COMMAND_URL_COPIED_AS, commandUrlCopiedAs);
                }
            }
        }
        return exec("bash "+commandUrlCopiedAs, env);
    }

    protected <T> void doReconfigureConfig(ConfigKey<T> key, T val) {
        if (key.getName().equals(COMMAND_URL.getName())) {
            config().set(COMMAND_URL_COPIED_AS, (String)null);
            return;
        }
        if (key.getName().equals(COMMAND_URL_COPIED_AS.getName())) {
            // allowed
            return;
        }

        super.doReconfigureConfig(key, val);
    }
}
