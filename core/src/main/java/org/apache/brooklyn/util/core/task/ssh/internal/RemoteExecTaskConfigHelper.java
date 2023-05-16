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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.objs.BasicConfigurableObject;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.BrooklynNetworkUtils;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.ShellTool;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.ssh.sshj.SshjTool;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ssh.ConnectionDefinition;
import org.apache.brooklyn.util.core.task.system.internal.ExecWithLoggingHelpers;
import org.apache.brooklyn.util.net.Networking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
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
            return machine.getUser()+"@"+machine.getAddress().getHostName();  // would be nice to include port, but we don't here know if it is ssh or winrm or other
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

        private static final Logger logSsh = LoggerFactory.getLogger(BrooklynLogging.SSH_IO);
        private static final Logger LOG = LoggerFactory.getLogger(RemoteExecCapabilityFromDefinition.class);

        private final ConnectionDefinition definition;
        ManagementContext mgmt;
        protected InetAddress address;
        protected Map<String, Object> config;

        public InetAddress getAddress() {
            return address;
        }

        public static RemoteExecCapabilityFromDefinition  of(ManagementContext mgmt,ConnectionDefinition definition) {
            return new RemoteExecCapabilityFromDefinition(mgmt, definition);
        }

        public static RemoteExecCapabilityFromDefinition  of(Entity entity,ConnectionDefinition definition) {
            return new RemoteExecCapabilityFromDefinition(((EntityInternal)entity).getManagementContext(), definition);
        }

        protected RemoteExecCapabilityFromDefinition(ManagementContext mgmt,ConnectionDefinition definition) {
            this.mgmt = mgmt;
            this.definition = definition;

            if ("ssh".equals(definition.getType())) {
                // TODO SSH client class should be injected in the 'config' map here too,  get it from ManagementContext.getTypeRegistry() , allow users to register their own types 'ssh' and 'winrm'
                config = resolveConnectionSettings();
                if(config.get("host").equals("localhost")) {
                    address = BrooklynNetworkUtils.getLocalhostInetAddress();
                } else {
                    address = Networking.resolve(config.get("host").toString());
                }
            } else {
                // TODO add support for 'winrm'
            }
        }

        @Override
        public String getConnectionSummary() {
            return definition.getUser().get() + "@" + definition.getHost();
        }

        private Map<String, Object> resolveConnectionSettings() {
            MutableMap<String, Object> config = MutableMap.of();
            definition.getOther()
                    .forEach((k, v) -> config.put(k, v instanceof WrappedValue ? ((WrappedValue<Object>) v).get() : v));
            config.put("user", WrappedValue.getMaybe(definition.getUser()).or(() -> System.getProperty("user.name")));
            config.put("password",  WrappedValue.get(definition.getPassword()));
            config.put("host", WrappedValue.get(definition.getHost()));
            config.put("port", WrappedValue.getMaybe(definition.getPort()).or("22"));
            return config;
        }

        @Override
        public Integer execScript(Map<String, Object> allConfig, String summary, List<String> commands, Map<String, String> shellEnvironment) {
            return newExecWithLoggingHelpers().execScript(allConfig, summary, commands, shellEnvironment);
        }

        @Override
        public Integer execCommands(Map<String, Object> allConfig, String summary, List<String> commands, Map<String, String> shellEnvironment) {
            return newExecWithLoggingHelpers().execCommands(allConfig, summary, commands, shellEnvironment);
        }

        @Override
        public Configurable.ConfigurationSupport getExtraConfiguration() {
            return new BasicConfigurableObject().config();
        }

        @Override
        public ManagementContext getManagementContext() {
            return mgmt;
        }

        protected <T> T execSsh(final Map<String, ?> props, final Function<ShellTool, T> task) {
            String sshToolClass = SshjTool.class.getName(); // TODO hardcoded for now, @see SshMachineLocation#connectSsh(..)
            try {
                SshTool ssh = (SshTool) new ClassLoaderUtils(this, getManagementContext())
                        .loadClass(sshToolClass)
                        .getConstructor(Map.class)
                        .newInstance(config);
                if (LOG.isTraceEnabled()) LOG.trace("using ssh-tool {} (of type {}); props ", ssh, sshToolClass);
                try {
                    Tasks.setBlockingDetails("Opening ssh connection");
                    ssh.connect();
                    return task.apply(ssh);
                } finally {
                    try {
                        ssh.disconnect();
                    } catch (Exception e) {
                        if (logSsh.isDebugEnabled()) logSsh.debug("ssh-disconnect failed from {}", this.getConnectionSummary(), e);
                    }
                    Tasks.setBlockingDetails(null);
                }
            } catch (Exception e) {
                String rootCause = Throwables.getRootCause(e).getMessage();
                throw new IllegalStateException("Cannot establish ssh connection to " + this.getConnectionSummary() +
                        (rootCause!=null && !rootCause.isEmpty() ? " ("+rootCause+")" : "")+". \n"+
                        "Ensure that passwordless and passphraseless ssh access is enabled using standard keys from ~/.ssh or " +
                        "as configured in brooklyn.properties. " +
                        "Check that the target host is accessible, " +
                        "that credentials are correct (location and permissions if using a key), " +
                        "that the SFTP subsystem is available on the remote side, " +
                        "and that there is sufficient random noise in /dev/random on both ends. " +
                        "To debug less common causes, see the original error in the trace or log, and/or enable 'net.schmizz' (sshj) logging."
                        , e);
            }
        }

        protected ExecWithLoggingHelpers newExecWithLoggingHelpers() {
            return new ExecWithLoggingHelpers("SSH") {
                @Override
                protected <T> T execWithTool(MutableMap<String, Object> props, Function<ShellTool, T> function) {
                    return execSsh(props, function);
                }

                @Override
                protected void preExecChecks() {
                    Preconditions.checkNotNull(address, "host address must be specified for ssh");
                }

                @Override
                protected String getTargetName() {
                    return "" + this;
                }

                @Override
                protected String constructDefaultLoggingPrefix(ConfigBag execFlags) {
                    String hostname = address.getHostName();
                    return (config.get("user") != null ? config.get("user")+"@" : "") + hostname + (config.get("port") != null ? ":"+config.get("port") : "");
                }
            }.logger(logSsh);
        }
    }
}
