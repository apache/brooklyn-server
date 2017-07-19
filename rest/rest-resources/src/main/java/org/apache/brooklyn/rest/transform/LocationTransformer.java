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
package org.apache.brooklyn.rest.transform;

import static org.apache.brooklyn.rest.util.WebResourceUtils.serviceUriBuilder;

import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.location.BasicLocationDefinition;
import org.apache.brooklyn.core.location.CatalogLocationResolver;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.rest.api.LocationApi;
import org.apache.brooklyn.rest.domain.CatalogLocationSummary;
import org.apache.brooklyn.rest.domain.LocationSummary;
import org.apache.brooklyn.rest.util.BrooklynRestResourceUtils;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class LocationTransformer {

    private static final Logger log = LoggerFactory.getLogger(LocationTransformer.LocationDetailLevel.class);
    
    public static enum LocationDetailLevel { NONE, LOCAL_EXCLUDING_SECRET, FULL_EXCLUDING_SECRET, FULL_INCLUDING_SECRET }
    
    /** @deprecated since 0.7.0 use method taking management context and detail specifier */
    @Deprecated
    public static LocationSummary newInstance(String id, org.apache.brooklyn.rest.domain.LocationSpec locationSpec, UriBuilder ub) {
        return newInstance(null, id, locationSpec, LocationDetailLevel.LOCAL_EXCLUDING_SECRET, ub);
    }
    
    private static LocationSummary newInstance(ManagementContext mgmt, 
            LocationSpec<? extends Location> spec, ConfigBag explicitConfig, 
            String optionalExplicitId, String name, String specString, 
            LocationDetailLevel level, UriBuilder ub) {
        // mgmt not actually needed
        Map<String, Object> config = MutableMap.copyOf(explicitConfig==null ? null : explicitConfig.getAllConfig());
        if (spec!=null && (level==LocationDetailLevel.FULL_EXCLUDING_SECRET || level==LocationDetailLevel.FULL_INCLUDING_SECRET)) {
            // full takes from any resolved spec ie inherited, AND explicit config and flags;
            // would be nice to distinguish flags from config, but more important to make sure flags are supplied
            // (ideally would return yaml, using yoml)
            config = ConfigBag.newInstance(spec.getFlags()).putAll(spec.getConfig()).putAll(config).getAllConfig();
        } else if (level==LocationDetailLevel.LOCAL_EXCLUDING_SECRET) {
            // in local mode, make sure any inherited display name is set, but otherwise _no_ inherited
            // NB this may not include provider, region, and endpoint -- use 'full' for that
            if (spec!=null && !explicitConfig.containsKey(LocationConfigKeys.DISPLAY_NAME) ) {
                if (Strings.isNonBlank((String) spec.getFlags().get(LocationConfigKeys.DISPLAY_NAME.getName()))){
                    config.put(LocationConfigKeys.DISPLAY_NAME.getName(), spec.getFlags().get(LocationConfigKeys.DISPLAY_NAME.getName()));
                } else if ( Strings.isNonBlank(spec.getDisplayName()) ) {
                    config.put(LocationConfigKeys.DISPLAY_NAME.getName(), spec.getDisplayName());
                }
            }
        }

        String id = Strings.isNonBlank(optionalExplicitId) ? optionalExplicitId : spec!=null && Strings.isNonBlank(spec.getCatalogItemId()) ? spec.getCatalogItemId() : null;
        URI selfUri = serviceUriBuilder(ub, LocationApi.class, "get").build(id);
        
        CatalogLocationSummary catalogSummary = null;
        if (CatalogLocationResolver.isLegacyWrappedReference(specString)) {
            RegisteredType ci = mgmt.getTypeRegistry().get(CatalogLocationResolver.unwrapLegacyWrappedReference(specString));
            if (ci!=null) {
                BrooklynRestResourceUtils br = new BrooklynRestResourceUtils(mgmt);
                catalogSummary = CatalogTransformer.catalogLocationSummary(br, ci, ub);
            }
        }
        return new LocationSummary(
                id,
                Strings.isNonBlank(name) ? name : spec!=null ? spec.getDisplayName() : null,
                Strings.isNonBlank(specString) ? specString : spec!=null ? spec.getCatalogItemId() : null,
                null,
                copyConfig(config, level),
                catalogSummary,
                ImmutableMap.of("self", selfUri));
    }
    
    @SuppressWarnings("deprecation") 
    public static LocationSummary newInstance(ManagementContext mgmt, String id, org.apache.brooklyn.rest.domain.LocationSpec locationSpec, LocationDetailLevel level, UriBuilder ub) {
        LocationDefinition ld = new BasicLocationDefinition(id, locationSpec.getName(), locationSpec.getSpec(), locationSpec.getConfig());
        return newInstance(mgmt, ld, level, ub);
    }

    /** @deprecated since 0.7.0 use method taking management context and detail specifier */
    @Deprecated
    public static LocationSummary newInstance(LocationDefinition l, UriBuilder ub) {
        return newInstance(null, l, LocationDetailLevel.LOCAL_EXCLUDING_SECRET, ub);
    }

    public static LocationSummary newInstance(ManagementContext mgmt, LocationDefinition ld, LocationDetailLevel level, UriBuilder ub) {
        return newInstance(mgmt, mgmt.getLocationRegistry().getLocationSpec(ld).orNull(), ConfigBag.newInstance(ld.getConfig()),
            ld.getId(), ld.getName(), ld.getSpec(), level, ub);
    }

    private static Map<String, ?> copyConfig(Map<String,?> entries, LocationDetailLevel level) {
        MutableMap.Builder<String, Object> builder = MutableMap.builder();
        if (level!=LocationDetailLevel.NONE) {
            for (Map.Entry<String,?> entry : entries.entrySet()) {
                if (level==LocationDetailLevel.FULL_INCLUDING_SECRET || !Sanitizer.IS_SECRET_PREDICATE.apply(entry.getKey())) {
                    builder.put(entry.getKey(), WebResourceUtils.getValueForDisplay(entry.getValue(), true, false));
                }
            }
        }
        return builder.build();
    }

    private static boolean LEGACY_SPEC_WARNING = false;
    // used when user is looking up a location instance by ID, or listing an entity's locations
    public static LocationSummary newInstance(ManagementContext mgmt, Location l, LocationDetailLevel level, UriBuilder ub) {
        String spec = null;
        String specId = null;
        Location lp = l;
        while (lp!=null && (spec==null || specId==null)) {
            // walk parent locations
            // TODO not sure this is the best strategy, or if it's needed, as the spec config is inherited anyway... 
            if (spec==null) {
                Maybe<Object> originalSpec = ((LocationInternal)lp).config().getRaw(LocationInternal.ORIGINAL_SPEC);
                if (originalSpec.isPresent())
                    spec = Strings.toString(originalSpec.get());
            }
            if (specId==null) {
                LocationDefinition ld = null;
                // prefer looking it up by name as this loads the canonical definition
                if (spec!=null) ld = mgmt.getLocationRegistry().getDefinedLocationByName(spec);
                if (ld==null && spec!=null && spec.startsWith("named:")) 
                    ld = mgmt.getLocationRegistry().getDefinedLocationByName(Strings.removeFromStart(spec, "named:"));
                if (ld==null) ld = mgmt.getLocationRegistry().getDefinedLocationById(lp.getId());
                if (ld!=null) {
                    if (spec==null) spec = ld.getSpec();
                    specId = ld.getId();
                }
            }
            lp = lp.getParent();
        }
        if (specId==null && spec!=null) {
            // fall back to attempting to lookup it
            // TODO remove this section unless we get this warning
            if (LEGACY_SPEC_WARNING==false) {
                log.warn("Legacy spec lookup required for rest summary of "+l);
                LEGACY_SPEC_WARNING = true;
            }
            @SuppressWarnings("deprecation")
            Location ll = mgmt.getLocationRegistry().resolve(spec, false, null).orNull();
            if (ll!=null) specId = ll.getId();
        }
        
        ConfigBag configOrig = ConfigBag.newInstance();
        if (level == LocationDetailLevel.LOCAL_EXCLUDING_SECRET) {
            configOrig.putAll( ((LocationInternal)l).config().getInternalConfigMap().getAllConfigLocalRaw() );
        } else {
            configOrig.putAll( ((LocationInternal)l).config().getInternalConfigMap().getAllConfigInheritedRawValuesIgnoringErrors() );
        }
        if (level==LocationDetailLevel.LOCAL_EXCLUDING_SECRET) {
            // for LOCAL, also get an inherited display name
            if (!configOrig.containsKey(LocationConfigKeys.DISPLAY_NAME)) {
                Maybe<Object> dn = ((LocationInternal)l).config().getRaw(LocationConfigKeys.DISPLAY_NAME);
                if (dn.isPresent()) configOrig.configure(LocationConfigKeys.DISPLAY_NAME, l.config().get(LocationConfigKeys.DISPLAY_NAME));
            }
        }
        Map<String, ?> config = level==LocationDetailLevel.NONE ? null : copyConfig(configOrig.getAllConfig(), level);
        
        URI selfUri = serviceUriBuilder(ub, LocationApi.class, "get").build(l.getId());
        URI parentUri = l.getParent() == null ? null : serviceUriBuilder(ub, LocationApi.class, "get").build(l.getParent().getId());
        URI specUri = specId == null ? null : serviceUriBuilder(ub, LocationApi.class, "get").build(specId);
        return new LocationSummary(
            l.getId(),
            l.getDisplayName(),
            spec,
            l.getClass().getName(),
            config,
            null,
            MutableMap.of("self", selfUri)
                .addIfNotNull("parent", parentUri)
                .addIfNotNull("spec", specUri)
                .asUnmodifiable() );
    }
    
    public static LocationSummary newInstance(ManagementContext mgmt, String id, LocationDetailLevel level, UriBuilder uriBuilder) {
        // if it's an ID of a deployed location
        Location l1 = mgmt.getLocationManager().getLocation(id);
        if (l1!=null) {
            return newInstance(mgmt, l1, level, uriBuilder);
        }
        
        // a catalog item OR a legacy properties-based -- but currently goes via registry
        LocationDefinition l2 = mgmt.getLocationRegistry().getDefinedLocationById(id);
        if (l2!=null) {
            return LocationTransformer.newInstance(mgmt, l2, level, uriBuilder);
        }
        
        // not recognised
        return null;
    }

}
