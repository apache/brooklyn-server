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
package org.apache.brooklyn.enricher.stock;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.collections.MutableMap;

/**
 * Building on {@link AbstractMultipleSensorAggregator} for a pair of source sensors(on multiple children and/or members)
 * that are used as key-value pairs in a generated Map.
 */
@Catalog(name = "Map Aggregator", description = "Aggregates a pair of sensors on multiple children and/or members that are used as key-value pairs in a generated Map")
@SuppressWarnings("serial")
public class MapAggregator<U> extends AbstractMultipleSensorAggregator<U> {

    public static final ConfigKey<Sensor<?>> KEY_SENSOR = ConfigKeys.newConfigKey(new TypeToken<Sensor<?>>() {}, "enricher.keySensor");
    public static final ConfigKey<Sensor<?>> VALUE_SENSOR = ConfigKeys.newConfigKey(new TypeToken<Sensor<?>>() {}, "enricher.valueSensor");

    private Sensor<?> keySensor;
    private Sensor<?> valueSensor;

    public MapAggregator() { }

    @Override
    protected Object compute() {
        Map<Entity, Object> ks = MutableMap.copyOf(Maps.filterValues(getValues(keySensor), valueFilter));
        Map<Entity, Object> vs = MutableMap.copyOf(Maps.filterValues(getValues(valueSensor), valueFilter));
        MutableMap<Object, Object> result = MutableMap.of();
        for (Entity entity : ks.keySet()) {
            if (vs.containsKey(entity)) {
                result.put(ks.get(entity), vs.get(entity));
            }
        }
        return result;
    }

    @Override
    protected Collection<Sensor<?>> getSourceSensors() {
        keySensor = config().get(KEY_SENSOR);
        valueSensor = config().get(VALUE_SENSOR);
        return ImmutableList.<Sensor<?>>of(keySensor, valueSensor);
    }

}
