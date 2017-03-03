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
import org.apache.brooklyn.feed.CommandPollConfig;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
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
        Boolean execAsCommand = config().get(EXEC_AS_COMMAND);
        if (log.isTraceEnabled()) log.trace("Ssh polling for {}, executing {} with env {}", new Object[] {machine, command, env});
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

        int exitStatus;
        ConfigBag flags = ConfigBag.newInstanceExtending(config().getBag())
            .configure(SshTool.PROP_NO_EXTRA_OUTPUT, true)
            .configure(SshTool.PROP_OUT_STREAM, stdout)
            .configure(SshTool.PROP_ERR_STREAM, stderr);
        if (Boolean.TRUE.equals(execAsCommand)) {
            exitStatus = machine.execCommands(flags.getAllConfig(),
                    "ssh-feed", ImmutableList.of(command), env);
        } else {
            exitStatus = machine.execScript(flags.getAllConfig(),
                    "ssh-feed", ImmutableList.of(command), env);
        }

        return new SshPollValue(machine, exitStatus, new String(stdout.toByteArray()), new String(stderr.toByteArray()));
    }
}
