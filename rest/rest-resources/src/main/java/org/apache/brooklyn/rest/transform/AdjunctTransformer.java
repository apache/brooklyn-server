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

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.policy.Policies;
import org.apache.brooklyn.rest.api.AdjunctApi;
import org.apache.brooklyn.rest.api.ApplicationApi;
import org.apache.brooklyn.rest.api.EntityApi;
import org.apache.brooklyn.rest.domain.AdjunctDetail;
import org.apache.brooklyn.rest.domain.AdjunctSummary;
import org.apache.brooklyn.rest.domain.ConfigSummary;
import org.apache.brooklyn.rest.domain.Status;
import org.apache.brooklyn.rest.util.BrooklynRestResourceUtils;
import org.apache.brooklyn.util.collections.MutableMap;

import com.google.common.base.Predicates;

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
            result.parameter(configSummary(utils, ub, entity, adjunct, key));
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

    public static ConfigSummary configSummary(BrooklynRestResourceUtils utils, UriBuilder ub, @Nullable Entity entity, @Nullable EntityAdjunct adjunct, SpecParameter<?> input) {
        Double priority = input.isPinned() ? Double.valueOf(1d) : null;
        return configSummary(utils, ub, entity, adjunct, input.getConfigKey(), input.getLabel(), priority, input.isPinned());
    }

    public static ConfigSummary configSummary(BrooklynRestResourceUtils utils, UriBuilder ub, Entity entity, EntityAdjunct adjunct, ConfigKey<?> config) {
        // TODO get catalog info from other sources?
        // see EntityTransformer.configSummary
        return configSummary(utils, ub, entity, adjunct, config, null, null, null);
    }
    public static ConfigSummary configSummary(BrooklynRestResourceUtils utils, UriBuilder ub, @Nullable Entity entity, @Nullable EntityAdjunct adjunct, ConfigKey<?> config, String label, Double priority, Boolean pinned) {
        URI configUri = entity==null ? null : serviceUriBuilder(ub, AdjunctApi.class, "getConfig").build(entity.getApplicationId(), entity.getId(), adjunct.getId(), config.getName());
        Map<String, URI> links = MutableMap.<String, URI>builder()
                .putIfNotNull("self", configUri)
                // no point in including app/entity on every summary shown in a list
                // (this is only ever used in a list, as self points at the value)
                .build();

        // TODO get actions, see EntityTransformer.configSummary
        return new ConfigSummary(config, label, priority, pinned, links);
    }
}
