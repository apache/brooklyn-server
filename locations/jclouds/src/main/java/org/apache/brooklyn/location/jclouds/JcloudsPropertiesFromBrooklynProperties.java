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

import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.DeprecatedKeysMappingBuilder;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.LocationPropertiesFromBrooklynProperties;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.StringPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * <p>
 * The properties to use for a jclouds location, loaded from brooklyn.properties file
 * </p>
 * 
 * Preferred format is:
 * 
 * <ul>
 * <li>
 * brooklyn.location.named.NAME.key 
 * </li>
 * <li>
 * brooklyn.location.jclouds.PROVIDER.key
 * </li>
 * </ul>
 * 
 * <p>
 * A number of properties are also supported, listed in the {@code JcloudsLocationConfig}
 * </p>
 * 
 * @author andrea
 **/
public class JcloudsPropertiesFromBrooklynProperties extends LocationPropertiesFromBrooklynProperties {

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsPropertiesFromBrooklynProperties.class);

    @SuppressWarnings("deprecation")
    private static final Map<String, String> DEPRECATED_JCLOUDS_KEYS_MAPPING = new DeprecatedKeysMappingBuilder(LOG)
            .putAll(LocationPropertiesFromBrooklynProperties.DEPRECATED_KEYS_MAPPING)
            .camelToHyphen(JcloudsLocation.IMAGE_ID)
            .camelToHyphen(JcloudsLocation.IMAGE_NAME_REGEX)
            .camelToHyphen(JcloudsLocation.IMAGE_DESCRIPTION_REGEX)
            .camelToHyphen(JcloudsLocation.HARDWARE_ID)
            .build();

    @Override
    public Map<String, Object> getLocationProperties(String provider, String namedLocation, Map<String, ?> properties) {
        throw new UnsupportedOperationException("Instead use getJcloudsProperties(String,String,String,Map)");
    }
    
    /**
     * @see LocationPropertiesFromBrooklynProperties#getLocationProperties(String, String, Map)
     */
    public Map<String, Object> getJcloudsProperties(String providerOrApi, String regionOrEndpoint, String namedLocation, Map<String, ?> properties) {
        if(Strings.isNullOrEmpty(namedLocation) && Strings.isNullOrEmpty(providerOrApi)) {
            throw new IllegalArgumentException("Neither cloud provider/API nor location name have been specified correctly");
        }

        ConfigBag jcloudsProperties = ConfigBag.newInstance();
        String provider = getProviderName(providerOrApi, namedLocation, properties);
        
        // named properties are preferred over providerOrApi properties
        jcloudsProperties.put(LocationConfigKeys.CLOUD_PROVIDER, provider);
        jcloudsProperties.putAll(transformDeprecated(getGenericLocationSingleWordProperties(properties)));
        jcloudsProperties.putAll(transformDeprecated(getGenericLocationKnownProperties(properties)));
        jcloudsProperties.putAll(transformDeprecated(getGenericJcloudsSingleWordProperties(providerOrApi, properties)));
        jcloudsProperties.putAll(transformDeprecated(getGenericJcloudsKnownProperties(properties)));
        jcloudsProperties.putAll(transformDeprecated(getGenericJcloudsPropertiesPrefixedJclouds(providerOrApi, properties)));
        jcloudsProperties.putAll(transformDeprecated(getProviderOrApiJcloudsProperties(providerOrApi, properties)));
        jcloudsProperties.putAll(transformDeprecated(getRegionJcloudsProperties(providerOrApi, regionOrEndpoint, properties)));
        if (!Strings.isNullOrEmpty(namedLocation)) jcloudsProperties.putAll(transformDeprecated(getNamedJcloudsProperties(namedLocation, properties)));
        setLocalTempDir(properties, jcloudsProperties);

        return jcloudsProperties.getAllConfigRaw();
    }

    protected String getProviderName(String providerOrApi, String namedLocationName, Map<String, ?> properties) {
        String provider = providerOrApi;
        if (!Strings.isNullOrEmpty(namedLocationName)) {
            String providerDefinition = (String) properties.get(String.format("brooklyn.location.named.%s", namedLocationName));
            if (providerDefinition!=null) {
                String provider2 = getProviderFromDefinition(providerDefinition);
                if (provider==null) {
                    // 0.7.0 25 Feb -- is this even needed?
                    LOG.warn(JavaClassNames.niceClassAndMethod()+" NOT set with provider, inferring from locationName "+namedLocationName+" as "+provider2);
                    provider = provider2;
                } else if (!provider.equals(provider2)) {
                    // 0.7.0 25 Feb -- previously we switched to provider2 in this case, but that was wrong when
                    // working with chains of names; not sure why this case would ever occur (apart from tests which have been changed)
                    // 28 Mar seen this warning many times but only in cases when NOT changing is the right behaviour 
                    LOG.trace(JavaClassNames.niceClassAndMethod()+" NOT changing provider from "+provider+" to candidate "+provider2);
                }
            }
        }
        return provider;
    }
    
    protected String getProviderFromDefinition(String definition) {
        return Iterables.get(Splitter.on(":").split(definition), 1);
    }
    
    private Map<String, ?> getGenericLocationKnownProperties(Map<String, ?> properties) {
        return getMatchingPropertiesWithoutPrefixInSet("brooklyn.location.", ConfigUtils.getStaticKeysOnClass(JcloudsLocationConfig.class), properties);
    }

    private Map<String, ?> getGenericJcloudsKnownProperties(Map<String, ?> properties) {
        return getMatchingPropertiesWithoutPrefixInSet("brooklyn.location.jclouds.", ConfigUtils.getStaticKeysOnClass(JcloudsLocationConfig.class), properties);
    }

    private Map<String, ?> getMatchingPropertiesWithoutPrefixInSet(String prefix, Set<HasConfigKey<?>> keysToKeep, Map<String, ?> properties) {
        BrooklynProperties filteredProperties = ConfigUtils.filterForPrefixAndStrip(properties, prefix);
        Set<String> keysToKeepStrings = MutableSet.of();
        for (HasConfigKey<?> key: keysToKeep) keysToKeepStrings.add(key.getConfigKey().getName());
        return ConfigUtils.filterFor(filteredProperties, StringPredicates.equalToAny(keysToKeepStrings)).asMapWithStringKeys();
    }

    protected Map<String, Object> getGenericJcloudsSingleWordProperties(String providerOrApi, Map<String, ?> properties) {
        if (Strings.isNullOrEmpty(providerOrApi)) return Maps.newHashMap();
        String deprecatedPrefix = "brooklyn.jclouds.";
        String preferredPrefix = "brooklyn.location.jclouds.";
        return getMatchingSingleWordProperties(preferredPrefix, deprecatedPrefix, properties);
    }

    protected Map<String, Object> getGenericJcloudsPropertiesPrefixedJclouds(String providerOrApi, Map<String, ?> properties) {
        if (Strings.isNullOrEmpty(providerOrApi)) return Maps.newHashMap();
        String prefixToStrip = "brooklyn.location.jclouds.";
        String prefixToKeep = "jclouds.";
        return getMatchingConcatenatedPrefixesPropertiesFirstPrefixRemoved(prefixToStrip, prefixToKeep, properties);
    }

    protected Map<String, Object> getProviderOrApiJcloudsProperties(String providerOrApi, Map<String, ?> properties) {
        if (Strings.isNullOrEmpty(providerOrApi)) return Maps.newHashMap();
        String preferredPrefix = String.format("brooklyn.location.jclouds.%s.", providerOrApi);
        String deprecatedPrefix = String.format("brooklyn.jclouds.%s.", providerOrApi);
        
        return getMatchingProperties(preferredPrefix, deprecatedPrefix, properties);
    }

    protected Map<String, Object> getRegionJcloudsProperties(String providerOrApi, String regionName, Map<String, ?> properties) {
        if (Strings.isNullOrEmpty(providerOrApi) || Strings.isNullOrEmpty(regionName)) return Maps.newHashMap();
        String preferredPrefix = String.format("brooklyn.location.jclouds.%s@%s.", providerOrApi, regionName);
        String deprecatedPrefix = String.format("brooklyn.jclouds.%s@%s.", providerOrApi, regionName);
        
        return getMatchingProperties(preferredPrefix, deprecatedPrefix, properties);
    }

    protected Map<String, Object> getNamedJcloudsProperties(String locationName, Map<String, ?> properties) {
        if(locationName == null) return Maps.newHashMap();
        String prefix = String.format("brooklyn.location.named.%s.", locationName);
        return ConfigUtils.filterForPrefixAndStrip(properties, prefix).asMapWithStringKeys();
    }

    @Override
    protected Map<String, String> getDeprecatedKeysMapping() {
        return DEPRECATED_JCLOUDS_KEYS_MAPPING;
    }
}
