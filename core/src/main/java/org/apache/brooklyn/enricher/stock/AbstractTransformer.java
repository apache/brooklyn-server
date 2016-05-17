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

import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

@SuppressWarnings("serial")
public abstract class AbstractTransformer<T,U> extends AbstractEnricher implements SensorEventListener<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTransformer.class);

    public static final ConfigKey<Entity> PRODUCER = ConfigKeys.newConfigKey(Entity.class, "enricher.producer");

    public static final ConfigKey<Sensor<?>> SOURCE_SENSOR = ConfigKeys.newConfigKey(new TypeToken<Sensor<?>>() {}, "enricher.sourceSensor");

    public static final ConfigKey<Sensor<?>> TARGET_SENSOR = ConfigKeys.newConfigKey(new TypeToken<Sensor<?>>() {}, "enricher.targetSensor");
    
    public static final ConfigKey<List<? extends Sensor<?>>> TRIGGER_SENSORS = ConfigKeys.newConfigKey(
            new TypeToken<List<? extends Sensor<?>>>() {}, 
            "enricher.triggerSensors",
            "Sensors that will trigger re-evaluation",
            ImmutableList.<Sensor<?>>of());

    protected Entity producer;
    protected Sensor<T> sourceSensor;
    protected Sensor<U> targetSensor;

    public AbstractTransformer() {
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        this.producer = getConfig(PRODUCER) == null ? entity: getConfig(PRODUCER);
        this.sourceSensor = (Sensor<T>) getConfig(SOURCE_SENSOR);
        Sensor<?> targetSensorSpecified = getConfig(TARGET_SENSOR);
        List<? extends Sensor<?>> triggerSensorsSpecified = getConfig(TRIGGER_SENSORS);
        List<? extends Sensor<?>> triggerSensors = triggerSensorsSpecified != null ? triggerSensorsSpecified : ImmutableList.<Sensor<?>>of();
        this.targetSensor = targetSensorSpecified!=null ? (Sensor<U>) targetSensorSpecified : (Sensor<U>) this.sourceSensor;
        if (targetSensor == null) {
            throw new IllegalArgumentException("Enricher "+JavaClassNames.simpleClassName(this)+" has no "+TARGET_SENSOR.getName()+", and it cannot be inferred as "+SOURCE_SENSOR.getName()+" is also not set");
        }
        if (sourceSensor == null && triggerSensors.isEmpty()) {
            throw new IllegalArgumentException("Enricher "+JavaClassNames.simpleClassName(this)+" has no "+SOURCE_SENSOR.getName()+" and no "+TRIGGER_SENSORS.getName());
        }
        if (producer.equals(entity) && (targetSensor.equals(sourceSensor) || triggerSensors.contains(targetSensor))) {
            // We cannot call getTransformation() here to log the tranformation, as it will attempt
            // to resolve the transformation, which will cause the entity initialization thread to block
            LOG.error("Refusing to add an enricher which reads and publishes on the same sensor: "+
                producer+"->"+targetSensor+" (computing transformation with "+JavaClassNames.simpleClassName(this)+")");
            // we don't throw because this error may manifest itself after a lengthy deployment, 
            // and failing it at that point simply because of an enricher is not very pleasant
            // (at least not until we have good re-run support across the board)
            return;
        }
        
        if (sourceSensor != null) {
            subscriptions().subscribe(MutableMap.of("notifyOfInitialValue", true), producer, sourceSensor, this);
        }
        
        if (triggerSensors.size() > 0) {
            SensorEventListener<Object> triggerListener = new SensorEventListener<Object>() {
                @Override public void onEvent(SensorEvent<Object> event) {
                    if (sourceSensor != null) {
                        // Simulate an event, as though our sourceSensor changed
                        Object value = producer.getAttribute((AttributeSensor<?>)sourceSensor);
                        AbstractTransformer.this.onEvent(new BasicSensorEvent(sourceSensor, producer, value, event.getTimestamp()));
                    } else {
                        // Assume the transform doesn't care about the value - otherwise it would 
                        // have declared a sourceSensor!
                        AbstractTransformer.this.onEvent(null);
                    }
                }
            };
            for (Object sensor : triggerSensors) {
                if (sensor instanceof String) {
                    Sensor<?> resolvedSensor = entity.getEntityType().getSensor((String)sensor);
                    if (resolvedSensor == null) { 
                        resolvedSensor = Sensors.newSensor(Object.class, (String)sensor);
                    }
                    sensor = resolvedSensor;
                }
                subscriptions().subscribe(MutableMap.of("notifyOfInitialValue", true), producer, (Sensor<?>)sensor, triggerListener);
            }
        }
    }

    /** returns a function for transformation, for immediate use only (not for caching, as it may change) */
    protected abstract Function<SensorEvent<T>, U> getTransformation();

    @Override
    public void onEvent(SensorEvent<T> event) {
        emit(targetSensor, compute(event));
    }

    protected Object compute(SensorEvent<T> event) {
        // transformation is not going to change, but this design makes it easier to support changing config in future. 
        // if it's an efficiency hole we can switch to populate the transformation at start.
        U result = getTransformation().apply(event);
        if (LOG.isTraceEnabled())
            LOG.trace("Enricher "+this+" computed "+result+" from "+event);
        return result;
    }
}
