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
package org.apache.brooklyn.location.byon;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationRegistry;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.AbstractLocationResolver;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.LocalLocationManager;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.net.UserAndHostAndPort;
import org.apache.brooklyn.util.text.WildcardGlobs;
import org.apache.brooklyn.util.text.WildcardGlobs.PhraseTreatment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;

/**
 * Examples of valid specs:
 *   <ul>
 *     <li>byon:(hosts=myhost)
 *     <li>byon:(hosts=myhost,myhost2)
 *     <li>byon:(hosts="myhost, myhost2")
 *     <li>byon:(hosts=myhost,myhost2, name=abc)
 *     <li>byon:(hosts="myhost, myhost2", name="my location name")
 *   </ul>
 * 
 * @author aled
 */
@SuppressWarnings({"rawtypes"})
public class ByonLocationResolver extends AbstractLocationResolver {

    public static final Logger log = LoggerFactory.getLogger(ByonLocationResolver.class);
    
    public static final String BYON = "byon";

    public static final ConfigKey<String> OS_FAMILY = ConfigKeys.newStringConfigKey("osFamily", "OS Family of the machine, either windows or linux", "linux");

    /**
     * @todo Reimplement via a registry:
     * org.apache.brooklyn.location.winrm.WinRmMachineLocation
     * {@link org.apache.brooklyn.location.ssh.SshMachineLocation}
     */
    public static final Map<String, String> OS_TO_MACHINE_LOCATION_TYPE = ImmutableMap.of(
            "windows", "org.apache.brooklyn.location.winrm.WinRmMachineLocation",
            "linux", "org.apache.brooklyn.location.ssh.SshMachineLocation");

    @Override
    public String getPrefix() {
        return BYON;
    }

    @Override
    protected Class<? extends Location> getLocationType() {
        return FixedListMachineProvisioningLocation.class;
    }

    @Override
    protected SpecParser getSpecParser() {
        return new AbstractLocationResolver.SpecParser(getPrefix()).setExampleUsage("\"byon(hosts='addr1,addr2')\"");
    }

    @Override
    protected ConfigBag extractConfig(Map<?,?> locationFlags, String spec, LocationRegistry registry) {
        ConfigBag config = super.extractConfig(locationFlags, spec, registry);
        new MachineSpecsExtractor(spec, config).setConfigKey(this);
        return config;
    }

    protected static class MachineSpecsExtractor implements DeferredSupplier<List<LocationSpec<? extends MachineLocation>>> {
        String spec;
        ConfigBag config;

        public MachineSpecsExtractor(String spec, ConfigBag config) {
            this.spec = spec;
            this.config = config;
        }

        /**
         * the return value could theoretically change, which could cause unusual behaviour, but it is created unmanaged, so it shouldn't cause leaks;
         * and any other unusual behaviour the caller shouldn't be too surprised about; we don't expect it to change often in any case
         */
        @Override
        public List<LocationSpec<? extends MachineLocation>> get() {
            Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
            if (entity == null) {
                throw new IllegalStateException("BYON dynamic config can only be resolved in the context of an entity task");
            }
            return extractConfigMachineSpecs(entity, ((EntityInternal) entity).getManagementContext(), spec, config, false);
        }

        public void setConfigKey(ByonLocationResolver resolver) {
            ConfigBag configCopy = ConfigBag.newInstanceCopying(config);
            try {
                List<LocationSpec<? extends MachineLocation>> machineSpecsResolvableEarly = extractConfigMachineSpecs(resolver, resolver.managementContext, spec, config, true);
                config.put(FixedListMachineProvisioningLocation.MACHINE_SPECS, machineSpecsResolvableEarly);

            } catch (UsesDeferredSupplier e) {
                // dynamic - update the config the location gets
                config.put((ConfigKey) FixedListMachineProvisioningLocation.MACHINE_SPECS, this);
                // but restore our local copy to the original one
                config = configCopy;
            }
        }

        protected static class UsesDeferredSupplier extends RuntimeException {}

        protected <T> T resolveOrThrow(boolean dynamicAllowed, String key, Class<T> type) {
            Object v = config.getStringKey(key);
            v = resolveOrThrow(dynamicAllowed, v);
            return TypeCoercions.coerce(v, type);
        }
        protected <T> T resolveRemoveOrThrow(boolean dynamicAllowed, Map<String, Object> map, String key, Class<T> type) {
            Object v = map.get(key);
            v = resolveOrThrow(dynamicAllowed, v);
            T result = TypeCoercions.coerce(v, type);
            map.remove(key);
            return result;
        }
        protected Object resolveOrThrow(boolean dynamicAllowed, Object v) {
            if (v instanceof DeferredSupplier) {
                if (dynamicAllowed) throw new UsesDeferredSupplier();
                v = ((DeferredSupplier)v).get();
            }
            return v;
        }

        // if dynamicAllowed, returns null if anything is can't be resolved;
        // if not dynamicAllowed, throws if anything can't be resolved.
        protected List<LocationSpec<? extends MachineLocation>> extractConfigMachineSpecs(Object context, ManagementContext mgmt, String spec, ConfigBag config, boolean dynamicAllowed) {
            Object hosts = resolveOrThrow(dynamicAllowed, "hosts", Object.class);

            Class<? extends MachineLocation> locationClass;
            MutableMap<String, Object> defaultProps = MutableMap.of();
            String user = resolveOrThrow(dynamicAllowed, "user", String.class);
            Integer port = resolveOrThrow(dynamicAllowed, "port", Integer.class);
            resolveOrThrow(dynamicAllowed, OS_FAMILY.getName(), Object.class);
            locationClass = getLocationClass(mgmt, context, config.get(OS_FAMILY));

            defaultProps.addIfNotNull("user", user);
            defaultProps.addIfNotNull("port", port);

            List<?> hostAddresses;

            if (hosts instanceof String) {
                if (((String) hosts).isEmpty()) {
                    hostAddresses = ImmutableList.of();
                } else {
                    hostAddresses = WildcardGlobs.getGlobsAfterBraceExpansion("{" + hosts + "}",
                            true /* numeric */, /* no quote support though */ PhraseTreatment.NOT_A_SPECIAL_CHAR, PhraseTreatment.NOT_A_SPECIAL_CHAR);
                }
            } else if (hosts instanceof Iterable) {
                hostAddresses = ImmutableList.copyOf((Iterable<?>) hosts);
            } else {
                throw new IllegalArgumentException("Invalid location '" + spec + "'; hosts must be defined, as a string of a single host or list of hosts");
            }
            if (hostAddresses.isEmpty()) {
                throw new IllegalArgumentException("Invalid location '" + spec + "'; at least one host must be defined");
            }

            List<LocationSpec<? extends MachineLocation>> machineSpecs = Lists.newArrayList();
            for (Object host : hostAddresses) {
                host = resolveOrThrow(dynamicAllowed, host);

                LocationSpec<? extends MachineLocation> machineSpec;
                if (host instanceof String) {
                    machineSpec = parseMachine((String) host, locationClass, defaultProps, spec);
                } else if (host instanceof Map) {
                    machineSpec = parseMachine(mgmt, context, (Map<String, ?>) host, locationClass, defaultProps, spec, dynamicAllowed);
                } else {
                    throw new IllegalArgumentException("Expected machine to be String or Map, but was " + host.getClass().getName() + " (" + host + ")");
                }
                machineSpec.configureIfNotNull(LocalLocationManager.CREATE_UNMANAGED, config.get(LocalLocationManager.CREATE_UNMANAGED));
                machineSpecs.add(machineSpec);
            }
            config.remove("hosts");
            return machineSpecs;
        }

        private LocationSpec<? extends MachineLocation> parseMachine(ManagementContext mgmt, Object context, Map<String, ?> vals, Class<? extends MachineLocation> locationClass, Map<String, ?> defaults, String specForErrMsg, boolean dynamicAllowed) {
            Map<String, Object> valSanitized = Sanitizer.sanitize(vals);
            Map<String, Object> machineConfig = MutableMap.copyOf(vals);

            String osFamily = resolveRemoveOrThrow(dynamicAllowed, machineConfig, OS_FAMILY.getName(), String.class);
            String ssh = resolveRemoveOrThrow(dynamicAllowed, machineConfig, "ssh", String.class);
            String winrm = resolveRemoveOrThrow(dynamicAllowed, machineConfig, "winrm", String.class);

            // ensure other items are resolved when creating the machine spec
            MutableMap.copyOf(machineConfig).forEach((k,v)->{
                        if (v instanceof DeferredSupplier) machineConfig.put(k, resolveOrThrow(dynamicAllowed, v));
                    });

            Map<Integer, String> tcpPortMappings = (Map<Integer, String>) machineConfig.get("tcpPortMappings");

            checkArgument(ssh != null ^ winrm != null, "Must specify exactly one of 'ssh' or 'winrm' for machine: %s", valSanitized);

            UserAndHostAndPort userAndHostAndPort;
            String host;
            int port;
            if (ssh != null) {
                userAndHostAndPort = parseUserAndHostAndPort(ssh, 22);
            } else {
                // TODO set to null and rely on the MachineLocation. If not then make a dependency to WinRmMachineLocation and use its config key name.
                userAndHostAndPort = parseUserAndHostAndPort(winrm, vals.get("winrm.useHttps") != null && (Boolean) vals.get("winrm.useHttps") ? 5986 : 5985);
            }

            // If there is a tcpPortMapping defined for the connection-port, then use that for ssh/winrm machine
            port = userAndHostAndPort.getHostAndPort().getPort();
            if (tcpPortMappings != null && tcpPortMappings.containsKey(port)) {
                String override = tcpPortMappings.get(port);
                HostAndPort hostAndPortOverride = HostAndPort.fromString(override);
                if (!hostAndPortOverride.hasPort()) {
                    throw new IllegalArgumentException("Invalid portMapping ('" + override + "') for port " + port + " in " + specForErrMsg);
                }
                port = hostAndPortOverride.getPort();
                host = hostAndPortOverride.getHost().trim();
            } else {
                host = userAndHostAndPort.getHostAndPort().getHost().trim();
            }

            machineConfig.put("address", host);
            try {
                InetAddress.getByName(host);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid host '" + host + "' specified in '" + specForErrMsg + "': " + e);
            }

            if (userAndHostAndPort.getUser() != null) {
                checkArgument(!vals.containsKey("user"), "Must not specify user twice for machine: %s", valSanitized);
                machineConfig.put("user", userAndHostAndPort.getUser());
            }
            if (userAndHostAndPort.getHostAndPort().hasPort()) {
                checkArgument(!vals.containsKey("port"), "Must not specify port twice for machine: %s", valSanitized);
                machineConfig.put("port", port);
            }
            for (Map.Entry<String, ?> entry : defaults.entrySet()) {
                if (!machineConfig.containsKey(entry.getKey())) {
                    machineConfig.put(entry.getKey(), entry.getValue());
                }
            }

            Class<? extends MachineLocation> locationClassHere = locationClass;
            if (osFamily != null) {
                locationClassHere = getLocationClass(mgmt, context, osFamily);
            }

            return LocationSpec.create(locationClassHere).configure(machineConfig);
        }

        private Class<? extends MachineLocation> getLocationClass(ManagementContext mgmt, Object context, String osFamily) {
            try {
                if (osFamily != null) {
                    String className = OS_TO_MACHINE_LOCATION_TYPE.get(osFamily.toLowerCase(Locale.ENGLISH));
                    return new ClassLoaderUtils(context, mgmt).loadClass(className).asSubclass(MachineLocation.class);
                }
            } catch (ClassNotFoundException ex) {
            }
            return null;
        }

        private LocationSpec<? extends MachineLocation> parseMachine(String val, Class<? extends MachineLocation> locationClass, Map<String, ?> defaults, String specForErrMsg) {
            Map<String, Object> machineConfig = Maps.newLinkedHashMap();

            UserAndHostAndPort userAndHostAndPort = parseUserAndHostAndPort(val);

            String host = userAndHostAndPort.getHostAndPort().getHost().trim();
            machineConfig.put("address", host);
            try {
                InetAddress.getByName(host.trim());
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid host '" + host + "' specified in '" + specForErrMsg + "': " + e);
            }

            if (userAndHostAndPort.getUser() != null) {
                machineConfig.put("user", userAndHostAndPort.getUser());
            }
            if (userAndHostAndPort.getHostAndPort().hasPort()) {
                machineConfig.put("port", userAndHostAndPort.getHostAndPort().getPort());
            }
            for (Map.Entry<String, ?> entry : defaults.entrySet()) {
                if (!machineConfig.containsKey(entry.getKey())) {
                    machineConfig.put(entry.getKey(), entry.getValue());
                }
            }

            return LocationSpec.create(locationClass).configure(machineConfig);
        }

        private UserAndHostAndPort parseUserAndHostAndPort(String val) {
            String userPart = null;
            String hostPart = val;
            if (val.contains("@")) {
                userPart = val.substring(0, val.indexOf("@"));
                hostPart = val.substring(val.indexOf("@") + 1);
            }
            return UserAndHostAndPort.fromParts(userPart, HostAndPort.fromString(hostPart));
        }

        private UserAndHostAndPort parseUserAndHostAndPort(String val, int defaultPort) {
            UserAndHostAndPort result = parseUserAndHostAndPort(val);
            if (!result.getHostAndPort().hasPort()) {
                result = UserAndHostAndPort.fromParts(result.getUser(), result.getHostAndPort().getHost(), defaultPort);
            }
            return result;
        }
    }
}
