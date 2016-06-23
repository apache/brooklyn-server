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

import com.google.common.reflect.TypeToken;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.javalang.JavaClassNames;

public class PercentageEnricher extends AbstractEnricher implements SensorEventListener<Number> {

    private static final Logger LOG = LoggerFactory.getLogger(PercentageEnricher.class);

    public static final ConfigKey<AttributeSensor<? extends Number>> SOURCE_CURRENT_SENSOR = ConfigKeys.newConfigKey(new TypeToken<AttributeSensor<? extends Number>>() {
    }, "enricher.sourceCurrentSensor");

    public static final ConfigKey<AttributeSensor<? extends Number>> SOURCE_TOTAL_SENSOR = ConfigKeys.newConfigKey(new TypeToken<AttributeSensor<? extends Number>>() {
    }, "enricher.sourceTotalSensor");

    public static final ConfigKey<AttributeSensor<Double>> TARGET_SENSOR = ConfigKeys.newConfigKey(new TypeToken<AttributeSensor<Double>>() {
    }, "enricher.targetSensor");

    public static final ConfigKey<Entity> PRODUCER = ConfigKeys.newConfigKey(Entity.class, "enricher.producer");

    protected AttributeSensor<? extends Number> sourceCurrentSensor;
    protected AttributeSensor<? extends Number> sourceTotalSensor;
    protected AttributeSensor<Double> targetSensor;
    protected Entity producer;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        this.sourceCurrentSensor = getConfig(SOURCE_CURRENT_SENSOR);
        this.sourceTotalSensor = getConfig(SOURCE_TOTAL_SENSOR);
        this.targetSensor = getConfig(TARGET_SENSOR);
        this.producer = getConfig(PRODUCER) == null ? entity : getConfig(PRODUCER);

        if (sourceCurrentSensor == null) {
            throw new IllegalArgumentException("Enricher " + JavaClassNames.simpleClassName(this) + " has no " + SOURCE_CURRENT_SENSOR.getName());
        }
        if (sourceTotalSensor == null) {
            throw new IllegalArgumentException("Enricher " + JavaClassNames.simpleClassName(this) + " has no " + SOURCE_TOTAL_SENSOR.getName());
        }
        if (targetSensor == null) {
            throw new IllegalArgumentException("Enricher " + JavaClassNames.simpleClassName(this) + " has no " + TARGET_SENSOR.getName());
        }

        if (targetSensor.equals(sourceCurrentSensor)) {
            throw new IllegalArgumentException("Enricher " + JavaClassNames.simpleClassName(this) + " detect cycle with " + SOURCE_CURRENT_SENSOR.getName());
        }
        if (targetSensor.equals(sourceTotalSensor)) {
            throw new IllegalArgumentException("Enricher " + JavaClassNames.simpleClassName(this) + " detect cycle with " + SOURCE_TOTAL_SENSOR.getName());
        }

        subscriptions().subscribe(MutableMap.of("notifyOfInitialValue", true), producer, sourceCurrentSensor, this);
        subscriptions().subscribe(MutableMap.of("notifyOfInitialValue", true), producer, sourceTotalSensor, this);

    }

    @Override
    public void onEvent(SensorEvent<Number> event) {
        Number current = producer.sensors().get(sourceCurrentSensor);
        Number total = producer.sensors().get(sourceTotalSensor);
        Double result = null;

        if (current == null) {
            LOG.debug("Current is null, returning");
            return;
        }

        if (total == null || total.equals(0)) {
            LOG.debug("total {" + total + "} is null or zero, returning");
            return;
        }

        Double currentDouble = current.doubleValue();
        Double totalDouble = total.doubleValue();

        if (currentDouble > totalDouble) {
            LOG.debug("Current is greater than total, returning");
            return;
        }

        if (currentDouble < 0 || totalDouble < 0) {
            LOG.debug("Current {"+currentDouble+"}  or total {"+totalDouble+"} is negative, returning");
            return;
        }

        if (current.equals(0)) {
            LOG.debug("current is zero, setting percent to zero");
            result = 0d;
        } else {
            result = currentDouble / totalDouble;
        }

        emit(targetSensor, result);
    }
}
