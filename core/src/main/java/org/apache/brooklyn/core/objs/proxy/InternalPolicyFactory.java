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
package org.apache.brooklyn.core.objs.proxy;

import java.util.Map;

import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;

/**
 * Creates policies of required types.
 * 
 * This is an internal class for use by core-brooklyn. End-users are strongly discouraged from
 * using this class directly.
 * 
 * Note that calling policies by their constructors has not been "deprecated" (yet!). We just
 * support both mechanisms, so one can supply PolicySpec in an EntitySpec.
 * 
 * @author aled
 */
public class InternalPolicyFactory extends InternalFactory {

    public InternalPolicyFactory(ManagementContextInternal managementContext) {
        super(managementContext);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T extends Policy> T createPolicy(PolicySpec<T> spec) {
        if (spec.getFlags().containsKey("parent")) {
            throw new IllegalArgumentException("Spec's flags must not contain parent; use spec.parent() instead for "+spec);
        }

        try {
            Class<? extends T> clazz = spec.getType();

            T pol = construct(clazz, spec, null);

            final AbstractPolicy policy = (AbstractPolicy) pol;
            if (spec.getDisplayName()!=null) {
                policy.setDisplayName(spec.getDisplayName());
            }
            if (spec.getCatalogItemId()!=null) {
                policy.setCatalogItemIdAndSearchPath(spec.getCatalogItemId(), spec.getCatalogItemIdSearchPath());
            } else {
                policy.addSearchPath(spec.getCatalogItemIdSearchPath());
            }

            pol.tags().addTags(spec.getTags());
            
            if (isNewStyle(clazz)) {
                policy.setManagementContext(managementContext);
                Map<String, Object> config = ConfigBag.newInstance().putAll(spec.getFlags()).putAll(spec.getConfig()).getAllConfig();
                policy.configure(MutableMap.copyOf(config)); // TODO AbstractPolicy.configure modifies the map
            }
            
            // TODO Can we avoid this for "new-style policies"? Should we just trust the configure() method, 
            // which the user may have overridden? 
            // Also see InternalLocationFactory for same issue, which this code is based on.
            for (Map.Entry<ConfigKey<?>, Object> entry : spec.getConfig().entrySet()) {
                pol.config().set((ConfigKey)entry.getKey(), entry.getValue());
            }
            policy.init();
            
            return pol;
            
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T extends Enricher> T createEnricher(EnricherSpec<T> spec) {
        if (spec.getFlags().containsKey("parent")) {
            throw new IllegalArgumentException("Spec's flags must not contain parent; use spec.parent() instead for "+spec);
        }
        
        try {
            Class<? extends T> clazz = spec.getType();
            
            T enricher = construct(clazz, spec, null);

            final AbstractEnricher theEnricher = (AbstractEnricher) enricher;
            if (spec.getDisplayName()!=null)
                theEnricher.setDisplayName(spec.getDisplayName());
            
            if (spec.getCatalogItemId()!=null) {
                theEnricher.setCatalogItemIdAndSearchPath(spec.getCatalogItemId(), spec.getCatalogItemIdSearchPath());
            } else {
                theEnricher.addSearchPath(spec.getCatalogItemIdSearchPath());
            }
            
            enricher.tags().addTags(spec.getTags());
            
            if (isNewStyle(clazz)) {
                theEnricher.setManagementContext(managementContext);
                Map<String, Object> config = ConfigBag.newInstance().putAll(spec.getFlags()).putAll(spec.getConfig()).getAllConfig();
                theEnricher.configure(MutableMap.copyOf(config)); // TODO AbstractEnricher.configure modifies the map
            }
            
            // TODO Can we avoid this for "new-style policies"? Should we just trust the configure() method, 
            // which the user may have overridden? 
            // Also see InternalLocationFactory for same issue, which this code is based on.
            for (Map.Entry<ConfigKey<?>, Object> entry : spec.getConfig().entrySet()) {
                enricher.config().set((ConfigKey)entry.getKey(), entry.getValue());
            }
            theEnricher.init();
            
            return enricher;
            
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    /**
     * Constructs a new-style policy (fails if no no-arg constructor).
     */
    public <T extends Policy> T constructPolicy(Class<T> clazz) {
        return super.constructNewStyle(clazz);
    }
    
    /**
     * Constructs a new-style enricher (fails if no no-arg constructor).
     */
    public <T extends Enricher> T constructEnricher(Class<T> clazz) {
        return super.constructNewStyle(clazz);
    }
    
    /**
     * Constructs a new-style feed (fails if no no-arg constructor).
     */
    public <T extends Feed> T constructFeed(Class<T> clazz) {
        return super.constructNewStyle(clazz);
    }
}
