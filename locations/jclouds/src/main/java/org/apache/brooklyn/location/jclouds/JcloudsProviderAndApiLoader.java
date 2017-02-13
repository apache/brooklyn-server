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
import java.util.ServiceLoader;

import org.apache.brooklyn.core.mgmt.persist.DeserializingJcloudsRenamesProvider;
import org.apache.brooklyn.util.text.Strings;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.ApiPredicates;
import org.jclouds.osgi.ApiRegistry;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.ProviderPredicates;
import org.jclouds.providers.Providers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

/**
 * Retrieves a given jclouds provider or API. It stores the service-loader providers so as to 
 * avoid repeatedly calling the {@link java.util.ServiceLoader}. This is fairly safe to do, 
 * as they are normally only available if using a classic "java main" rather than an OSGi
 * container. For the latter, we query jclouds each time so that the list can be dynamic as
 * bundles are added/removed.
 */
@Beta
public class JcloudsProviderAndApiLoader {

    public static final Logger log = LoggerFactory.getLogger(JcloudsProviderAndApiLoader.class);
    
    private enum LazyServiceLoader {
        INSTANCE;

        public final Map<String,ProviderMetadata> providers;
        public final Map<String,ApiMetadata> apis;
        
        LazyServiceLoader() {
            Map<String,ProviderMetadata> providersTmp = Maps.newLinkedHashMap();
            for (ProviderMetadata p: Providers.fromServiceLoader()) {
                providersTmp.put(p.getId(), p);
            }
            providers = ImmutableMap.copyOf(providersTmp);
            
            // Unfortunately Apis.fromServiceLoader() is private
            Map<String,ApiMetadata> apisTmp = Maps.newLinkedHashMap();
            ServiceLoader.load(ApiMetadata.class);
            for (ApiMetadata p: ServiceLoader.load(ApiMetadata.class)) {
                apisTmp.put(p.getId(), p);
            }
            apis = ImmutableMap.copyOf(apisTmp);
        }
    }
    
    public static boolean isProvider(String id) {
        if (Strings.isBlank(id)) return false;
        return getProvider(id).isPresent();
    }

    public static boolean isApi(String id) {
        if (Strings.isBlank(id)) return false;
        return getApi(id).isPresent();
    }
    
    public static Optional<ProviderMetadata> getProvider(String id) {
        id = DeserializingJcloudsRenamesProvider.INSTANCE.applyJcloudsRenames(id);
        if (LazyServiceLoader.INSTANCE.providers.containsKey(id)) {
            return Optional.of(LazyServiceLoader.INSTANCE.providers.get(id));
        }
        return Iterables.tryFind(ProviderRegistry.fromRegistry(), ProviderPredicates.id(id));
    }

    public static Optional<ApiMetadata> getApi(String id) {
        id = DeserializingJcloudsRenamesProvider.INSTANCE.applyJcloudsRenames(id);
        if (LazyServiceLoader.INSTANCE.apis.containsKey(id)) {
            return Optional.of(LazyServiceLoader.INSTANCE.apis.get(id));
        }
        return Iterables.tryFind(ApiRegistry.fromRegistry(), ApiPredicates.id(id));
    }
}
