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

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationRegistry;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.location.AbstractLocationResolver;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.WildcardGlobs;
import org.apache.brooklyn.util.text.WildcardGlobs.PhraseTreatment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Examples of valid specs:
 *   <ul>
 *     <li>byon:(hosts=myhost)
 *     <li>byon:(hosts="myhost, myhost2")
 *     <li>byon:(hosts="myhost, myhost2", name="my location name")
 *   </ul>
 * 
 * @author aled
 */
@SuppressWarnings({"unchecked"})
public class JcloudsByonLocationResolver extends AbstractLocationResolver implements LocationResolver {

    public static final Logger log = LoggerFactory.getLogger(JcloudsByonLocationResolver.class);
    
    public static final String BYON = "jcloudsByon";

    @Override
    public boolean isEnabled() {
        return LocationConfigUtils.isResolverPrefixEnabled(managementContext, getPrefix());
    }
    
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
        return new AbstractLocationResolver.SpecParser(getPrefix()).setExampleUsage("\"jcloudsByon(provider='aws-ec2',region='us-east-1',hosts='i-12345678,i-90123456')\"");
    }

    @Override
    protected Map<String, Object> getFilteredLocationProperties(String provider, String namedLocation, Map<String, ?> prioritisedProperties, Map<String, ?> globalProperties) {
        String providerOrApi = (String) prioritisedProperties.get("provider");
        String regionName = (String) prioritisedProperties.get("region");
        return new JcloudsPropertiesFromBrooklynProperties().getJcloudsProperties(providerOrApi, regionName, namedLocation, globalProperties);
    }

    @Override
    protected ConfigBag extractConfig(Map<?,?> locationFlags, String spec, final LocationRegistry registry) {
        ConfigBag config = super.extractConfig(locationFlags, spec, registry);
        
        String providerOrApi = (String) config.getStringKey("provider");
        String regionName = (String) config.getStringKey("region");
        String endpoint = (String) config.getStringKey("endpoint");
        config.remove(LocationInternal.NAMED_SPEC_NAME.getName());

        Object hosts = config.getStringKey("hosts");
        config.remove("hosts");

        if (Strings.isEmpty(providerOrApi)) {
            throw new IllegalArgumentException("Invalid location '"+spec+"'; provider must be defined");
        }
        if (hosts == null || (hosts instanceof String && Strings.isBlank((String)hosts))) {
            throw new IllegalArgumentException("Invalid location '"+spec+"'; at least one host must be defined");
        }

        final String jcloudsSpec = "jclouds:"+providerOrApi + (regionName != null ? ":"+regionName : "") + (endpoint != null ? ":"+endpoint : "");
        final Maybe<LocationSpec<? extends Location>> jcloudsLocationSpec = registry.getLocationSpec(jcloudsSpec, config.getAllConfig());
        if (jcloudsLocationSpec.isAbsentOrNull()) {
            throw new IllegalArgumentException("Invalid location '"+spec+"'; referrenced jclouds spec '"+jcloudsSpec+"' cannot be resolved");
        } else if (!JcloudsLocation.class.isAssignableFrom(jcloudsLocationSpec.get().getType())) {
            throw new IllegalArgumentException("Invalid location '"+spec+"'; referrenced spec '"+jcloudsSpec+"' of type "+jcloudsLocationSpec.get().getType()+" rather than JcloudsLocation");
        }

        List<?> hostIds;
        
        if (hosts instanceof String) {
            if (((String) hosts).isEmpty()) {
                hostIds = ImmutableList.of();
            } else {
                hostIds = WildcardGlobs.getGlobsAfterBraceExpansion("{"+hosts+"}",
                        true /* numeric */, /* no quote support though */ PhraseTreatment.NOT_A_SPECIAL_CHAR, PhraseTreatment.NOT_A_SPECIAL_CHAR);
            }
        } else if (hosts instanceof Iterable) {
            hostIds = ImmutableList.copyOf((Iterable<?>)hosts);
        } else {
            throw new IllegalArgumentException("Invalid location '"+spec+"'; at least one host must be defined");
        }
        if (hostIds.isEmpty()) {
            throw new IllegalArgumentException("Invalid location '"+spec+"'; at least one host must be defined");
        }

        final List<Map<?,?>> machinesFlags = Lists.newArrayList();
        for (Object hostId : hostIds) {
            Map<?, ?> machineFlags;
            if (hostId instanceof String) {
                machineFlags = parseMachineFlags((String)hostId, config);
            } else if (hostId instanceof Map) {
                machineFlags = parseMachineFlags((Map<String,?>)hostId, config);
            } else {
                throw new IllegalArgumentException("Invalid host type '"+(hostId == null ? null : hostId.getClass().getName())+", referrenced in spec '"+spec);
            }
            machinesFlags.add(machineFlags);
        }
        
        Supplier<List<JcloudsMachineLocation>> machinesFactory = new Supplier<List<JcloudsMachineLocation>>() {
            @Override
            public List<JcloudsMachineLocation> get() {
                List<JcloudsMachineLocation> result = Lists.newArrayList();
                JcloudsLocation jcloudsLocation = (JcloudsLocation) managementContext.getLocationManager().createLocation(jcloudsLocationSpec.get());
                for (Map<?,?> machineFlags : machinesFlags) {
                    try {
                        jcloudsLocation.config().set(machineFlags);
                        JcloudsMachineLocation machine = jcloudsLocation.registerMachine(jcloudsLocation.config().getBag());
                        result.add(machine);
                    } catch (NoMachinesAvailableException e) {
                        Map<?,?> sanitizedMachineFlags = Sanitizer.sanitize(machineFlags);
                        log.warn("Error rebinding to jclouds machine "+sanitizedMachineFlags+" in "+jcloudsLocation, e);
                        Exceptions.propagate(e);
                    }
                }
                
                log.debug("Created machines for jclouds BYON location: "+result);
                return result;
            }
        };
        
        config.put(FixedListMachineProvisioningLocation.INITIAL_MACHINES_FACTORY, machinesFactory);

        return config;
    }
    
    
    protected Map<?, ?> parseMachineFlags(Map<String, ?> vals, ConfigBag config) {
        if (!(vals.get("id") instanceof String) || Strings.isBlank((String)vals.get("id"))) {
            Map<String, Object> valSanitized = Sanitizer.sanitize(vals);
            throw new IllegalArgumentException("In jcloudsByon, machine "+valSanitized+" is missing String 'id'");
        }
        return MutableMap.builder()
                .putAll(config.getAllConfig())
                .putAll(vals)
                .build();
    }

    protected Map<?, ?> parseMachineFlags(String hostIdentifier, ConfigBag config) {
        return MutableMap.builder()
                .putAll(config.getAllConfig())
                .put("id", hostIdentifier)
                .build();
    }
}
