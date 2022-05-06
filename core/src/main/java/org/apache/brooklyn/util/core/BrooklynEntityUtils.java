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
package org.apache.brooklyn.util.core;


import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class BrooklynEntityUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynEntityUtils.class);

    public static String parseBlueprintYaml(Object yaml, String blueprintType) {
        String newBlueprint = null;

        if (yaml instanceof Map) {
            newBlueprint = toJson((Map<?, ?>) yaml);
        } else if (yaml instanceof String) {
            newBlueprint = (String) yaml;
        } else if (yaml != null) {
            throw new IllegalArgumentException("Map or string is required in blueprint YAML; not " + yaml.getClass() + " (" + yaml + ")");
        }

        if (blueprintType != null) {
            if (newBlueprint != null) {
                throw new IllegalArgumentException("Cannot take both blueprint type and blueprint YAML");
            }
            newBlueprint = "services: [ { type: " + blueprintType + " } ]";
        }

        if (newBlueprint == null) {
            throw new IllegalArgumentException("Either blueprint type or blueprint YAML is required");
        }

        return newBlueprint;
    }

    @SuppressWarnings("unchecked")
    public static List<String> addChildrenToEntity(Entity entity, ConfigBag entityParams, String blueprint, Boolean autostart) {

        if (!entityParams.isEmpty()) {
            Map<?, ?> m = ((Map<?, ?>) Iterables.getOnlyElement(Yamls.parseAll(blueprint)));
            if (m.containsKey("brooklyn.config")) {
                Map<?, ?> cfg1 = (Map<?, ?>) m.get("brooklyn.config");
                Map<Object, Object> cfgMergeFlat = MutableMap.<Object, Object>copyOf(cfg1).add(entityParams.getAllConfig());
                if (cfgMergeFlat.size() < cfg1.size() + entityParams.size()) {
                    // there are quite complex merging strategies, but we need type info to apply them
                    LOG.warn("Adding blueprint where same config key is supplied in blueprint and as parameters;" +
                            "preferring parameter (no merge), but behaviour may change. Recommended to use distinct keys.");
                }
                ((Map<Object, Object>) m).put("brooklyn.config", cfgMergeFlat);
                blueprint = toJson(m);
            } else {
                if (isJsonNotYaml(blueprint)) {
                    ((Map<Object, Object>) m).put("brooklyn.config", entityParams.getAllConfig());
                    blueprint = toJson(m);
                } else {
                    blueprint = blueprint + "\n" + "brooklyn.config: " + toJson(entityParams.getAllConfig());
                }
            }
        }

        LOG.debug("Adding children to " + entity + ":\n" + blueprint);
        EntityManagementUtils.CreationResult<List<Entity>, List<String>> result = EntityManagementUtils.addChildren(entity, blueprint, autostart);
        LOG.debug("Added children to " + entity + ": " + result.get());

        return result.task().getUnchecked();
    }

    private static String toJson(Map<?,?> x) {
        // was:
        // return new Gson().toJson(x);
        // but GSON does funny things with DSL, whereas toString is the right thing to do
        return Jsonya.newInstance().add(x).toString();
    }

    private static boolean isJsonNotYaml(String x) {
        return x.trim().startsWith("{");
    }
}