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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.policy.Policies;
import org.apache.brooklyn.rest.api.AdjunctApi;
import org.apache.brooklyn.rest.api.ApplicationApi;
import org.apache.brooklyn.rest.api.EntityApi;
import org.apache.brooklyn.rest.domain.AdjunctConfigSummary;
import org.apache.brooklyn.rest.domain.AdjunctDetail;
import org.apache.brooklyn.rest.domain.AdjunctSummary;
import org.apache.brooklyn.rest.domain.ApplicationSummary;
import org.apache.brooklyn.rest.domain.Status;
import org.apache.brooklyn.rest.util.BrooklynRestResourceUtils;
import org.apache.brooklyn.util.collections.MutableMap;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;

/**
 * Converts from Brooklyn entities to restful API summary objects
 */
public class AdjunctTransformer {

    public static AdjunctSummary adjunctSummary(Entity entity, EntityAdjunct adjunct, UriBuilder ub) {
        return embellish(new AdjunctSummary(adjunct), entity, adjunct, ub);
    }

    @SuppressWarnings("unchecked")
    private static <T extends AdjunctSummary> T embellish(T adjunctSummary, Entity entity, EntityAdjunct adjunct, UriBuilder ub) {
        return (T) adjunctSummary.state(inferStatus(adjunct)).links( buildLinks(entity, adjunct, ub, adjunctSummary instanceof AdjunctDetail) );
    }

    public static AdjunctDetail adjunctDetail(BrooklynRestResourceUtils utils, Entity entity, EntityAdjunct adjunct, UriBuilder ub) {
        AdjunctDetail result = embellish(new AdjunctDetail(adjunct), entity, adjunct, ub);
        for (ConfigKey<?> key: adjunct.config().findKeysDeclared(Predicates.alwaysTrue())) {
            result.parameter(configSummary(utils, entity, adjunct, key, ub));
        }
        result.config(EntityTransformer.getConfigValues(utils, adjunct));
        return result;
    }

    protected static Map<String, URI> buildLinks(Entity entity, EntityAdjunct adjunct, UriBuilder ub, boolean detail) {
        MutableMap<String,URI> links = MutableMap.of();

        links.put("self", serviceUriBuilder(ub, AdjunctApi.class, "get").build(entity.getApplicationId(), entity.getId(), adjunct.getId()));
        
        if (detail) {
            links.put("application", serviceUriBuilder(ub, ApplicationApi.class, "get").build(entity.getApplicationId()));
            links.put("entity", serviceUriBuilder(ub, EntityApi.class, "get").build(entity.getApplicationId(), entity.getId()));
            links.put("config", serviceUriBuilder(ub, AdjunctApi.class, "listConfig").build(entity.getApplicationId(), entity.getId(), adjunct.getId()));
            links.put("status", serviceUriBuilder(ub, AdjunctApi.class, "getStatus").build(entity.getApplicationId(), entity.getId(), adjunct.getId()));
            if (adjunct instanceof Policy || adjunct instanceof Feed) {
                links.put("start", serviceUriBuilder(ub, AdjunctApi.class, "start").build(entity.getApplicationId(), entity.getId(), adjunct.getId()));
                links.put("stop", serviceUriBuilder(ub, AdjunctApi.class, "stop").build(entity.getApplicationId(), entity.getId(), adjunct.getId()));
            }
            links.put("destroy", serviceUriBuilder(ub, AdjunctApi.class, "destroy").build(entity.getApplicationId(), entity.getId(), adjunct.getId()));
        }
        
        return links.asUnmodifiable();
    }

    public static Status inferStatus(EntityAdjunct adjunct) {
        return ApplicationTransformer.statusFromLifecycle( Policies.inferAdjunctStatus(adjunct) );
    }

    public static AdjunctConfigSummary configSummary(BrooklynRestResourceUtils utils, ApplicationSummary application, Entity entity, EntityAdjunct adjunct, ConfigKey<?> config, UriBuilder ub) {
        return configSummary(utils, entity, adjunct, config, ub);
    }

    public static AdjunctConfigSummary configSummary(BrooklynRestResourceUtils utils, Entity entity, EntityAdjunct adjunct, ConfigKey<?> config, UriBuilder ub) {
        URI applicationUri = serviceUriBuilder(ub, ApplicationApi.class, "get").build(entity.getApplicationId());
        URI entityUri = serviceUriBuilder(ub, EntityApi.class, "get").build(entity.getApplicationId(), entity.getId());
        URI adjunctUri = serviceUriBuilder(ub, AdjunctApi.class, "get").build(entity.getApplicationId(), entity.getId(), adjunct.getId());
        URI configUri = serviceUriBuilder(ub, AdjunctApi.class, "getConfig").build(entity.getApplicationId(), entity.getId(), adjunct.getId(), config.getName());

        Map<String, URI> links = ImmutableMap.<String, URI>builder()
                .put("self", configUri)
                .put("application", applicationUri)
                .put("entity", entityUri)
                .put("policy", adjunctUri)
                .build();

        return new AdjunctConfigSummary(config.getName(), config.getTypeName(), config.getDescription(), 
                utils.getStringValueForDisplay(config.getDefaultValue()), 
                config.isReconfigurable(), 
                links);
    }
}
