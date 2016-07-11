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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.render.RendererHints;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.math.MathFunctions;

/**
 * Enricher that is configured with two numerical sensors; a current and a total.
 * The Enricher subscribes to events from these sensors, and will emit the ratio
 * of current to total as a target sensor.
 */
public class PercentageEnricher extends AbstractEnricher implements SensorEventListener<Number> {

    private static final Logger LOG = LoggerFactory.getLogger(PercentageEnricher.class);

    @SuppressWarnings("serial")
    public static final ConfigKey<AttributeSensor<? extends Number>> SOURCE_CURRENT_SENSOR = ConfigKeys.newConfigKey(
            new TypeToken<AttributeSensor<? extends Number>>() {},
            "enricher.sourceCurrentSensor",
            "The sensor from which to take the current value");

    @SuppressWarnings("serial")
    public static final ConfigKey<AttributeSensor<? extends Number>> SOURCE_TOTAL_SENSOR = ConfigKeys.newConfigKey(
            new TypeToken<AttributeSensor<? extends Number>>() {},
            "enricher.sourceTotalSensor",
            "The sensor from which to take the total value");

    @SuppressWarnings("serial")
    public static final ConfigKey<AttributeSensor<Double>> TARGET_SENSOR = ConfigKeys.newConfigKey(
            new TypeToken<AttributeSensor<Double>>() {},
            "enricher.targetSensor",
            "The sensor on which to emit the ratio");

    public static final ConfigKey<Entity> PRODUCER = ConfigKeys.newConfigKey(Entity.class, "enricher.producer");

    protected AttributeSensor<? extends Number> sourceCurrentSensor;
    protected AttributeSensor<? extends Number> sourceTotalSensor;
    protected AttributeSensor<Double> targetSensor;
    protected Entity producer;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        sourceCurrentSensor = Preconditions.checkNotNull(getConfig(SOURCE_CURRENT_SENSOR), "Can't add Enricher %s to entity %s as it has no %s", JavaClassNames.simpleClassName(this), entity, SOURCE_CURRENT_SENSOR.getName());
        sourceTotalSensor = Preconditions.checkNotNull(getConfig(SOURCE_TOTAL_SENSOR), "Can't add Enricher %s to entity %s as it has no %s", JavaClassNames.simpleClassName(this), entity, SOURCE_TOTAL_SENSOR.getName());
        targetSensor = Preconditions.checkNotNull(getConfig(TARGET_SENSOR), "Can't add Enricher %s to entity %s as it has no %s", JavaClassNames.simpleClassName(this), entity, TARGET_SENSOR.getName());
        producer = getConfig(PRODUCER) == null ? entity : getConfig(PRODUCER);

        if (targetSensor.equals(sourceCurrentSensor) && entity.equals(producer)) {
            throw new IllegalArgumentException("Can't add Enricher " + JavaClassNames.simpleClassName(this) + " to entity "+entity+" as cycle detected with " + SOURCE_CURRENT_SENSOR.getName());
        }
        if (targetSensor.equals(sourceTotalSensor) && entity.equals(producer)) {
            throw new IllegalArgumentException("Can't add Enricher " + JavaClassNames.simpleClassName(this) + " to entity "+entity+" as cycle detected with " + SOURCE_TOTAL_SENSOR.getName());
        }

        subscriptions().subscribe(MutableMap.of("notifyOfInitialValue", true), producer, sourceCurrentSensor, this);
        subscriptions().subscribe(MutableMap.of("notifyOfInitialValue", true), producer, sourceTotalSensor, this);

        if (RendererHints.getHintsFor(targetSensor).isEmpty()) {
            RendererHints.register(targetSensor, RendererHints.displayValue(MathFunctions.percent(2)));
        }
    }

    @Override
    public void onEvent(SensorEvent<Number> event) {
        Number current = Preconditions.checkNotNull(producer.sensors().get(sourceCurrentSensor), "Can't calculate Enricher %s value for entity %s as current from producer %s is null", JavaClassNames.simpleClassName(this), entity, producer);
        Number total = Preconditions.checkNotNull(producer.sensors().get(sourceTotalSensor),     "Can't calculate Enricher %s value for entity %s as total from producer %s is null", JavaClassNames.simpleClassName(this), entity, producer);
        Double currentDouble = current.doubleValue();
        Double totalDouble = total.doubleValue();

        if (totalDouble.compareTo(0d) == 0) {
            LOG.debug("Can't calculate Enricher ({}) value for entity ({}) as total from producer ({}) is zero", new Object[]{JavaClassNames.simpleClassName(this), entity, producer});
            return;
        }

        if (currentDouble < 0 || totalDouble < 0) {
            LOG.debug("Can't calculate Enricher ({}) value for entity ({}) as Current ({})  or total ({}) value from producer ({}) is negative, returning", new Object[]{JavaClassNames.simpleClassName(this), entity, currentDouble, totalDouble, producer});
            return;
        }

        Double result = currentDouble / totalDouble;

        emit(targetSensor, result);
    }
}
