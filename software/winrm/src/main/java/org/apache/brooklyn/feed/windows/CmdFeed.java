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
package org.apache.brooklyn.feed.windows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.brooklyn.feed.AbstractCommandFeed;
import org.apache.brooklyn.feed.CommandPollConfig;
import org.apache.brooklyn.feed.ssh.SshPollValue;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.winrm.WinRmTool;
import org.apache.brooklyn.util.core.internal.winrm.WinRmToolResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CmdFeed extends AbstractCommandFeed {
    public static class Builder extends org.apache.brooklyn.feed.AbstractCommandFeed.Builder<CmdFeed, CmdFeed.Builder> {
        private List<CommandPollConfig<?>> polls = Lists.newArrayList();

        @Override
        public CmdFeed.Builder poll(CommandPollConfig<?> config) {
            polls.add(config);
            return self();
        }

        @Override
        public List<CommandPollConfig<?>> getPolls() {
            return polls;
        }

        @Override
        protected CmdFeed.Builder self() {
            return this;
        }

        @Override
        protected CmdFeed instantiateFeed() {
            return new CmdFeed(this);
        }
    }

    public static CmdFeed.Builder builder() {
        return new CmdFeed.Builder();
    }

    /**
     * For rebind; do not call directly; use builder
     */
    public CmdFeed() {
    }

    protected CmdFeed(final Builder builder) {
        super(builder);
    }
    @Override
    protected SshPollValue exec(String command, Map<String,String> env) throws IOException {
        WinRmMachineLocation machine = (WinRmMachineLocation)getMachine();
        if (log.isTraceEnabled()) log.trace("WinRm polling for {}, executing {} with env {}", new Object[] {machine, command, env});

        WinRmToolResponse winRmToolResponse;
        int exitStatus;
        ConfigBag flags = ConfigBag.newInstanceExtending(config().getBag())
                .configure(WinRmTool.ENVIRONMENT, env);
        winRmToolResponse = machine.executeCommand(flags.getAllConfig(),
                ImmutableList.of(command));
        exitStatus = winRmToolResponse.getStatusCode();

        return new SshPollValue(null, exitStatus, winRmToolResponse.getStdOut(), winRmToolResponse.getStdErr());
    }
}
