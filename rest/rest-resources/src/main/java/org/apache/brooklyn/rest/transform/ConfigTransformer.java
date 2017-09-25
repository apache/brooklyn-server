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

import java.lang.reflect.Field;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.UriBuilder;

import org.apache.brooklyn.api.catalog.CatalogConfig;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.render.RendererHints;
import org.apache.brooklyn.rest.api.AdjunctApi;
import org.apache.brooklyn.rest.api.EntityConfigApi;
import org.apache.brooklyn.rest.domain.ConfigSummary;
import org.apache.brooklyn.util.collections.MutableMap;

import com.google.common.collect.Iterables;

public class ConfigTransformer {

    private final ConfigKey<?> key;

    UriBuilder ub;
    boolean includeContextLinks, includeActionLinks;
    Entity entity;
    EntityAdjunct adjunct;
    
    String label;
    Double priority;
    Boolean pinned;
    
    public static ConfigTransformer of(ConfigKey<?> key) {
        return new ConfigTransformer(key);
    }

    public static ConfigTransformer of(SpecParameter<?> param) {
        ConfigTransformer result = of(param.getConfigKey());
        result.label = param.getLabel();
        result.pinned = param.isPinned();
        return result;
    }

    private ConfigTransformer(ConfigKey<?> key) {
        this.key = key;
    }
    
    public ConfigTransformer on(Entity entity) {
        this.entity = entity;
        return this;
    }
    
    public ConfigTransformer on(Entity entity, EntityAdjunct adjunct) {
        this.entity = entity;
        this.adjunct = adjunct;
        return this;
    }
    
    public ConfigTransformer includeLinks(UriBuilder ub, boolean includeContextLinks, boolean includeActionLinks) {
        this.ub = ub;
        this.includeContextLinks = includeContextLinks;
        this.includeActionLinks = includeActionLinks;
        return this;
    }
    
    public ConfigTransformer uiMetadata(String label, Double priority, Boolean pinned) {
        this.label = label;
        this.priority = priority;
        this.pinned = pinned;
        return this;
    }
    
    public ConfigTransformer uiMetadata(String label, Boolean pinned) {
        return uiMetadata(label, Boolean.TRUE.equals(pinned) ? 1.0d : 0, pinned);
    }
    
    public ConfigTransformer uiIncrementAndSetPriorityIfPinned(AtomicInteger lastPriority) {
        if (Boolean.TRUE.equals(pinned)) {
            this.priority = (double) lastPriority.incrementAndGet();
        }
        return this;
    }

    public ConfigTransformer uiMetadata(Field keyField) {
        if (keyField==null) return this;
        return uiMetadata(keyField.getDeclaredAnnotation(CatalogConfig.class));
    }
        
    public ConfigTransformer uiMetadata(CatalogConfig annotation) {
        if (annotation==null) return this;
        return uiMetadata(annotation.label(), annotation.priority(), annotation.pinned());
    }
    
    public ConfigSummary transform() {
        MutableMap.Builder<String, URI> lb = new MutableMap.Builder<String, URI>();
        
        if (ub!=null && entity!=null) {
            URI self;
            if (adjunct!=null) {
                self = serviceUriBuilder(ub, AdjunctApi.class, "getConfig").build(entity.getApplicationId(), entity.getId(), adjunct.getId(), key.getName());
            } else {
                self = serviceUriBuilder(ub, EntityConfigApi.class, "get").build(entity.getApplicationId(), entity.getId(), key.getName());
            }
            lb.put("self", self);
            
            if (includeContextLinks) {
                // TODO wasteful including these
                lb.put("application", EntityTransformer.applicationUri(entity.getApplication(), ub) );
                lb.put("entity", EntityTransformer.entityUri(entity, ub) );
                if (adjunct!=null) {
                    lb.put("adjunct", AdjunctTransformer.adjunctUri(entity, adjunct, ub) );
                }
            }
            if (includeActionLinks) {
                // TODO is this json or a display value?
                lb.put("action:json", self);
                
                Iterable<RendererHints.NamedAction> hints = Iterables.filter(RendererHints.getHintsFor(key), RendererHints.NamedAction.class);
                BrooklynObject context = adjunct!=null ? adjunct : entity;
                for (RendererHints.NamedAction na : hints) {
                    SensorTransformer.addNamedAction(lb, na, context.getConfig(key), key, context);
                }
            }
            
        }

        // TODO if ui metadata not set try to infer or get more info from caller ?
        
        return new ConfigSummary(key, label, priority, pinned, lb.build());
    }

    @Deprecated
    public org.apache.brooklyn.rest.domain.EntityConfigSummary transformLegacyEntityConfig() {
        ConfigSummary v2 = transform();
        return new org.apache.brooklyn.rest.domain.EntityConfigSummary(key, v2.getLabel(), v2.getPriority(), v2.isPinned(), v2.getLinks()); 
    }

    @Deprecated
    public org.apache.brooklyn.rest.domain.EnricherConfigSummary transformLegacyEnricherConfig() {
        ConfigSummary v2 = transform();
        return new org.apache.brooklyn.rest.domain.EnricherConfigSummary(key, v2.getLabel(), v2.getPriority(), v2.getLinks()); 
    }

    @Deprecated
    public org.apache.brooklyn.rest.domain.PolicyConfigSummary transformLegacyPolicyConfig() {
        ConfigSummary v2 = transform();
        return new org.apache.brooklyn.rest.domain.PolicyConfigSummary(key, v2.getLabel(), v2.getPriority(), v2.getLinks()); 
    }

}
