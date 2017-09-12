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

import org.apache.brooklyn.api.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;
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
import org.apache.brooklyn.util.math.MathFunctions;

/**
 * Enricher that is configured with two numerical sensors; a current and a total.
 * The Enricher subscribes to events from these sensors, and will emit the ratio
 * of current to total as a target sensor.
 */
@Catalog(name = "Percentage Transformer", description = "Computes and advertises the percentage based on a current and total values")
public class PercentageEnricher extends AbstractEnricher implements SensorEventListener<Number> {

    private static final Logger LOG = LoggerFactory.getLogger(PercentageEnricher.class);

    private static final double EPSILON = 1e-12d; // For zero comparision

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

    public static final ConfigKey<Entity> PRODUCER = ConfigKeys.newConfigKey(
            Entity.class, 
            "enricher.producer",
            "The entity with the trigger sensor (defaults to the enricher's entity)");

    protected AttributeSensor<? extends Number> sourceCurrentSensor;
    protected AttributeSensor<? extends Number> sourceTotalSensor;
    protected AttributeSensor<Double> targetSensor;
    protected Entity producer;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        sourceCurrentSensor = Preconditions.checkNotNull(config().get(SOURCE_CURRENT_SENSOR), "Can't add percentage enricher to entity %s as it has no %s", entity, SOURCE_CURRENT_SENSOR.getName());
        sourceTotalSensor = Preconditions.checkNotNull(config().get(SOURCE_TOTAL_SENSOR), "Can't add percentage enricher to entity %s as it has no %s", entity, SOURCE_TOTAL_SENSOR.getName());
        targetSensor = Preconditions.checkNotNull(config().get(TARGET_SENSOR), "Can't add percentage enricher to entity %s as it has no %s", entity, TARGET_SENSOR.getName());
        producer = Objects.firstNonNull(config().get(PRODUCER), entity);

        if (targetSensor.equals(sourceCurrentSensor) && entity.equals(producer)) {
            throw new IllegalArgumentException("Can't add percentage enricher to entity " + entity + " as cycle detected with " + SOURCE_CURRENT_SENSOR.getName());
        }
        if (targetSensor.equals(sourceTotalSensor) && entity.equals(producer)) {
            throw new IllegalArgumentException("Can't add percentage enricher to entity " + entity + " as cycle detected with " + SOURCE_TOTAL_SENSOR.getName());
        }

        subscriptions().subscribe(MutableMap.of("notifyOfInitialValue", true), producer, sourceCurrentSensor, this);
        subscriptions().subscribe(MutableMap.of("notifyOfInitialValue", true), producer, sourceTotalSensor, this);

        if (RendererHints.getHintsFor(targetSensor).isEmpty()) {
            RendererHints.register(targetSensor, RendererHints.displayValue(MathFunctions.percent(2)));
        }
    }

    @Override
    public void onEvent(SensorEvent<Number> event) {
        Number current = producer.sensors().get(sourceCurrentSensor);
        if (current == null) {
            LOG.trace("Can't calculate percentage value for entity {} as current from producer {} is null", entity, producer);
            return;
        }
        Number total = producer.sensors().get(sourceTotalSensor);
        if (total == null) {
            LOG.trace("Can't calculate percentage value for entity {} as total from producer {} is null",  entity, producer);
            return;
        }

        Double currentDouble = current.doubleValue();
        Double totalDouble = total.doubleValue();

        if (DoubleMath.fuzzyEquals(totalDouble, 0d, EPSILON)) {
            LOG.trace("Can't calculate percentage value for entity {} as total from producer {} is zero", entity, producer);
            return;
        }
        if (currentDouble < 0d || totalDouble < 0d) {
            LOG.trace("Can't calculate percentage value for entity {} as current ({}) or total ({}) from producer {} is negative",
                    new Object[] { entity, currentDouble, totalDouble, producer });
            return;
        }

        Double result = currentDouble / totalDouble;

        emit(targetSensor, result);
    }
}
