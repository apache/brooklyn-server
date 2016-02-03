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
package org.apache.brooklyn.cm.salt.impl;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Utility for setting a Salt highstate description on entity sensors.
 */
public class SaltHighstate {

    private static final Logger LOG = LoggerFactory.getLogger(SaltHighstate.class);

    public static final String HIGHSTATE_SENSOR_PREFIX = "salt.state";

    public static TypeToken<Map<String, Object>> STATE_FUNCTION_TYPE =
        new TypeToken<Map<String, Object>>() {};

    private SaltHighstate() {}

    public static void applyHighstate(String contents, Entity entity) {

        final String adaptedYaml = adaptForSaltYamlTypes(contents);
        LOG.debug("Parsing Salt highstate yaml:\n{}", adaptedYaml);
        final List<Object> objects = Yamls.parseAll(adaptedYaml);

        for (Object entry: objects) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> scopeMap = Yamls.getAs(entry, Map.class);
            applyStatesInScope(entity, scopeMap);
        }
    }

    private static void applyStatesInScope(Entity entity, Map<String, Object> scopeMap) {
        for (String scope: scopeMap.keySet()) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> stateMap = Yamls.getAs(scopeMap.get(scope), Map.class);
            for (String id: stateMap.keySet()) {
                applyStateSensor(id, stateMap.get(id), entity);
            }
        }
    }


    private static String adaptForSaltYamlTypes(String description) {
        return description.replaceAll("!!python/unicode", "!!java.lang.String");
    }

    @SuppressWarnings("unchecked")
    private static void applyStateSensor(String id, Object stateData, Entity entity) {
        if (isSaltInternal(id)) {
            return;
        }
        addStateSensor(id, entity);
        try {
            Map<String, List<Object>> stateInfo = (Map<String, List<Object>>)stateData;
            for (String stateModule : stateInfo.keySet()) {
                addStateModuleValue(id, entity, stateInfo, stateModule);
            }
        } catch (ClassCastException e) {
            LOG.info("Unexpected structure for {} state, skipping ({})", id, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static void addStateModuleValue(String id, Entity entity, Map<String, List<Object>> stateInfo,
        String stateModule) {

        if (isSaltInternal(stateModule)) {
            return;
        }
        try {
            final List<Object> stateEntries = stateInfo.get(stateModule);
            String stateFunction = "";
            Map<String, Object> moduleSettings = MutableMap.of();
            for (Object entry : stateEntries) {
                if (entry instanceof Map) {
                    moduleSettings.putAll((Map<String, Object>)entry);
                } else {
                    stateFunction = entry.toString();
                }
            }

            final String name = sensorName(id, stateModule, stateFunction);
            final AttributeSensor<Map<String, Object>> newSensor =
                Sensors.newSensor(STATE_FUNCTION_TYPE, HIGHSTATE_SENSOR_PREFIX + "." + name, name);
            entity.sensors().set(newSensor, moduleSettings);

            LOG.debug("Sensor set for: {}", moduleSettings);
        } catch (ClassCastException e) {
            LOG.info("Unexpected structure for state module {}, skipping ({})", id + "." + stateModule, e.getMessage());
        }
    }

    private static String sensorName(String... parts) {
        return Strings.join(parts, ".");
    }


    private static void addStateSensor(String state, Entity entity) {
        List<String> states = entity.sensors().get(SaltEntityImpl.STATES);
        if (null == states || !states.contains(state)) {
            if (null == states) {
                states = MutableList.of();
            }
            states.add(state);
            entity.sensors().set(SaltEntityImpl.STATES, states);
        }
    }

    private static boolean isSaltInternal(String module) {
        return module.startsWith("__");
    }
}
