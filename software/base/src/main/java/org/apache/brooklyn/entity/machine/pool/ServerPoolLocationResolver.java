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
package org.apache.brooklyn.entity.machine.pool;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationRegistry;
import org.apache.brooklyn.api.location.LocationResolver;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.BasicLocationRegistry;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.apache.brooklyn.core.objs.proxy.SpecialBrooklynObjectConstructor;
import org.apache.brooklyn.core.objs.proxy.SpecialBrooklynObjectConstructor.Config;
import org.apache.brooklyn.util.text.KeyValueParser;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class ServerPoolLocationResolver implements LocationResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ServerPoolLocationResolver.class);
    private static final String PREFIX = "pool";
    public static final String POOL_SPEC = PREFIX + ":%s";
    private static final Pattern PATTERN = Pattern.compile("("+PREFIX+"|"+PREFIX.toUpperCase()+")" +
            ":([a-zA-Z0-9]+)" + // pool Id
            "(:\\((.*)\\))?$"); // arguments, e.g. displayName

    private static final Set<String> ACCEPTABLE_ARGS = ImmutableSet.of("name");

    private ManagementContext managementContext;

    @Override
    public boolean isEnabled() {
        return LocationConfigUtils.isResolverPrefixEnabled(managementContext, getPrefix());
    }

    @Override
    public void init(ManagementContext managementContext) {
        this.managementContext = checkNotNull(managementContext, "managementContext");
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    public boolean accepts(String spec, LocationRegistry registry) {
        return BasicLocationRegistry.isResolverPrefixForSpec(this, spec, true);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public LocationSpec newLocationSpecFromString(String spec, Map locationFlags, LocationRegistry registry) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Resolving location '" + spec + "' with flags " + Joiner.on(",").withKeyValueSeparator("=").join(locationFlags));
        }

        Matcher matcher = PATTERN.matcher(spec);
        if (!matcher.matches()) {
            String m = String.format("Invalid location '%s'; must specify either %s:entityId or %s:entityId:(key=argument)",
                    spec, PREFIX, PREFIX);
            throw new IllegalArgumentException(m);
        }

        // TODO Could validate that the namePart matches the existing poolLoc
        String argsPart = matcher.group(4);
        Map<String, String> argsMap = (argsPart != null) ? KeyValueParser.parseMap(argsPart) : Collections.<String,String>emptyMap();
        @SuppressWarnings("unused")
        String namePart = argsMap.get("name");

        if (!ACCEPTABLE_ARGS.containsAll(argsMap.keySet())) {
            Set<String> illegalArgs = Sets.difference(argsMap.keySet(), ACCEPTABLE_ARGS);
            throw new IllegalArgumentException("Invalid location '"+spec+"'; illegal args "+illegalArgs+"; acceptable args are "+ACCEPTABLE_ARGS);
        }

        String poolLocId = matcher.group(2);
        if (Strings.isBlank(poolLocId)) {
            throw new IllegalArgumentException("Invalid location '"+spec+"'; pool's location id must be non-empty");
        }

        Location poolLoc = managementContext.getLocationManager().getLocation(poolLocId);
        if (poolLoc == null) {
            throw new IllegalArgumentException("Unknown ServerPool location id "+poolLocId+", spec "+spec);
        } else if (!(poolLoc instanceof ServerPoolLocation)) {
            throw new IllegalArgumentException("Invalid location id for ServerPool, spec "+spec+"; instead matches "+poolLoc);
        }

        return LocationSpec.create(ServerPoolLocation.class)
                .configure(LocationConstructor.LOCATION, poolLoc)
                .configure(Config.SPECIAL_CONSTRUCTOR, LocationConstructor.class);
    }
    
    @Beta
    public static class LocationConstructor implements SpecialBrooklynObjectConstructor {
        public static ConfigKey<Location> LOCATION = ConfigKeys.newConfigKey(Location.class, "serverpoolresolver.location");
        
        @SuppressWarnings("unchecked")
        @Override
        public <T> T create(ManagementContext mgmt, Class<T> type, AbstractBrooklynObjectSpec<?, ?> spec) {
            return (T) checkNotNull(spec.getConfig().get(LOCATION), LOCATION.getName());
        }
    }
}
