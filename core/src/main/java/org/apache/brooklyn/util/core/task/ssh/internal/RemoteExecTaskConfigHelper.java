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
package org.apache.brooklyn.util.core.task.ssh.internal;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.objs.BasicConfigurableObject;
import org.apache.brooklyn.util.core.task.ssh.ConnectionDefinition;

import java.util.List;
import java.util.Map;

public class RemoteExecTaskConfigHelper {

    public interface RemoteExecCapability {
        String getConnectionSummary();

        Integer execScript(Map<String, Object> allConfig, String summary, List<String> commands, Map<String, String> shellEnvironment);

        Integer execCommands(Map<String, Object> allConfig, String summary, List<String> commands, Map<String, String> shellEnvironment);
        //void apply();

        Configurable.ConfigurationSupport getExtraConfiguration();

        ManagementContext getManagementContext();
    }

    public static class RemoteExecCapabilityFromLocation implements RemoteExecCapability {
        MachineLocation machine;
        public RemoteExecCapabilityFromLocation(MachineLocation machine) {
            this.machine = machine;
        }

        @Override
        public String getConnectionSummary() {
            return machine.getUser()+"@"+machine.getAddress().getHostName();  // would be nice to include port but we don't here know if it is ssh or winrm or other
        }

        @Override
        public Integer execScript(Map<String, Object> allConfig, String summary, List<String> commands, Map<String, String> shellEnvironment) {
            return machine.execScript(allConfig, summary, commands, shellEnvironment);
        }

        @Override
        public Integer execCommands(Map<String, Object> allConfig, String summary, List<String> commands, Map<String, String> shellEnvironment) {
            return machine.execCommands(allConfig, summary, commands, shellEnvironment);
        }

        @Override
        public Configurable.ConfigurationSupport getExtraConfiguration() {
            return machine.config();
        }

        @Override
        public ManagementContext getManagementContext() {
            return (machine instanceof AbstractLocation) ? ((AbstractLocation) machine).getManagementContext() : null;
        }
    }

    public static class RemoteExecCapabilityFromDefinition implements RemoteExecCapability {
        private final Entity entity;
        private final ConnectionDefinition definition;
        ManagementContext mgmt;
        public RemoteExecCapabilityFromDefinition(ManagementContext mgmt, Entity entity, ConnectionDefinition definition) {
            this.mgmt = mgmt;
            this.entity = entity;
            this.definition = definition;
        }

        @Override
        public String getConnectionSummary() {
            // TODO resolve
            return definition.getUser().get() + "@" + definition.getHost();
        }

        @Override
        public Integer execScript(Map<String, Object> allConfig, String summary, List<String> commands, Map<String, String> shellEnvironment) {
            throw new IllegalStateException("TODO");
        }

        @Override
        public Integer execCommands(Map<String, Object> allConfig, String summary, List<String> commands, Map<String, String> shellEnvironment) {
            throw new IllegalStateException("TODO");
        }

        @Override
        public Configurable.ConfigurationSupport getExtraConfiguration() {
            return new BasicConfigurableObject().config();
        }

        @Override
        public ManagementContext getManagementContext() {
            return mgmt;
        }
    }

}
