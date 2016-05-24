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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.sensor.SensorPredicates;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

@SuppressWarnings("serial")
//@Catalog(name="Propagator", description="Propagates attributes from one entity to another; see Enrichers.builder().propagating(...)")
public class Propagator extends AbstractEnricher implements SensorEventListener<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(Propagator.class);

    public static final Set<Sensor<?>> SENSORS_NOT_USUALLY_PROPAGATED = ImmutableSet.<Sensor<?>>of(
        Attributes.SERVICE_UP, Attributes.SERVICE_NOT_UP_INDICATORS, 
        Attributes.SERVICE_STATE_ACTUAL, Attributes.SERVICE_STATE_EXPECTED, Attributes.SERVICE_PROBLEMS);

    @SetFromFlag("producer")
    public static ConfigKey<Entity> PRODUCER = ConfigKeys.newConfigKey(Entity.class, "enricher.producer");

    @SetFromFlag("propagatingAllBut")
    public static ConfigKey<Collection<? extends Sensor<?>>> PROPAGATING_ALL_BUT = ConfigKeys.newConfigKey(
            new TypeToken<Collection<? extends Sensor<?>>>() {}, 
            "enricher.propagating.propagatingAllBut");

    @SetFromFlag("propagatingAll")
    public static ConfigKey<Boolean> PROPAGATING_ALL = ConfigKeys.newBooleanConfigKey("enricher.propagating.propagatingAll");

    @SetFromFlag("propagating")
    public static ConfigKey<Collection<? extends Sensor<?>>> PROPAGATING = ConfigKeys.newConfigKey(new TypeToken<Collection<? extends Sensor<?>>>() {}, "enricher.propagating.inclusions");

    @SetFromFlag("sensorMapping")
    public static ConfigKey<Map<? extends Sensor<?>, ? extends Sensor<?>>> SENSOR_MAPPING = ConfigKeys.newConfigKey(new TypeToken<Map<? extends Sensor<?>, ? extends Sensor<?>>>() {}, "enricher.propagating.sensorMapping");

    protected Entity producer;
    protected Map<Sensor<?>, Sensor<?>> sensorMapping;
    protected boolean propagatingAll;
    protected Collection<Sensor<?>> propagatingAllBut;
    protected Predicate<? super Sensor<?>> sensorFilter;

    public Propagator() {
    }

    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);
        
        producer = getConfig(PRODUCER);
        sensorMapping = resolveSensorMappings(getConfig(SENSOR_MAPPING));
        propagatingAllBut = resolveSensorCollection(getConfig(PROPAGATING_ALL_BUT));
        propagatingAll = Boolean.TRUE.equals(getConfig(PROPAGATING_ALL)) || propagatingAllBut.size() > 0;
        Collection<Sensor<?>> propagating = resolveSensorCollection(getConfig(PROPAGATING));
        
        if (producer == null) {
            throw new IllegalStateException("Propagator enricher "+this+" missing config '"+PRODUCER.getName());
        }
        if (propagating.isEmpty() && sensorMapping.isEmpty() && !propagatingAll) {
            throw new IllegalStateException("Propagator enricher "+this+" must have 'propagating' and/or 'sensorMapping', or 'propagatingAll' or 'propagatingAllBut' set");
        }
        if (entity.equals(producer)) {
            if (propagatingAll) {
                throw new IllegalStateException("Propagator enricher "+this+" must not have "+PROPAGATING_ALL.getName()+" or "+PROPAGATING_ALL_BUT.getName()+", when publishing to own entity (to avoid infinite loop)");
            } else if (propagating.size() > 0) {
                throw new IllegalStateException("Propagator enricher "+this+" must not have "+PROPAGATING.getName()+", when publishing to own entity (to avoid infinite loop)");
            } else if (filterForKeyEqualsValue(sensorMapping).size() > 0) {
                Map<? extends Sensor<?>, ? extends Sensor<?>> selfPublishingSensors = filterForKeyEqualsValue(sensorMapping);
                throw new IllegalStateException("Propagator enricher "+this+" must not publish to same sensor in config "+SENSOR_MAPPING.getName()+" ("+selfPublishingSensors.keySet()+"), when publishing to own entity (to avoid infinite loop)");
            }
        }
        if ((propagating.size() > 0 || sensorMapping.size() > 0) && propagatingAll) {
            throw new IllegalStateException("Propagator enricher "+this+" must not have 'propagating' or 'sensorMapping' set at same time as either 'propagatingAll' or 'propagatingAllBut'");
        }
        
        if (propagating.size() > 0) {
            for (Sensor<?> sensor : propagating) {
                if (!sensorMapping.containsKey(sensor)) {
                    sensorMapping.put(sensor, sensor);
                }
            }
            sensorMapping = ImmutableMap.copyOf(sensorMapping);
            sensorFilter = Predicates.alwaysTrue();
            new Predicate<Sensor<?>>() {
                @Override public boolean apply(Sensor<?> input) {
                    // TODO kept for deserialization of inner classes, but shouldn't be necessary, as with other inner classes (qv);
                    // NB: previously this did this check:
//                    return input != null && sensorMapping.keySet().contains(input);
                    // but those clauses seems wrong (when would input be null?) and unnecessary (we are doing an explicit subscribe in this code path) 
                    return true;
                }
            };
        } else if (sensorMapping.size() > 0) {
            sensorMapping = ImmutableMap.copyOf(sensorMapping);
            sensorFilter = Predicates.alwaysTrue();
        } else {
            Preconditions.checkState(propagatingAll, "Impossible case: propagatingAll=%s; propagating=%s; "
                    + "sensorMapping=%s", propagatingAll, propagating, sensorMapping);
            sensorMapping = ImmutableMap.of();
            sensorFilter = new Predicate<Sensor<?>>() {
                @Override public boolean apply(Sensor<?> input) {
                    return input != null && !propagatingAllBut.contains(input);
                }
            };
        }
            
        Preconditions.checkState(propagatingAll ^ sensorMapping.size() > 0,
                "Nothing to propagate; detected: propagatingAll (%s, excluding %s), sensorMapping (%s)", propagatingAll, getConfig(PROPAGATING_ALL_BUT), sensorMapping);

        if (propagatingAll) {
            subscriptions().subscribe(producer, null, this);
        } else {
            for (Sensor<?> sensor : sensorMapping.keySet()) {
                subscriptions().subscribe(producer, sensor, this);
            }
        }
        
        emitAllAttributes();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void onEvent(SensorEvent<Object> event) {
        // propagate upwards
        Sensor<?> sourceSensor = event.getSensor();
        Sensor<?> destinationSensor = getDestinationSensor(sourceSensor);

        if (!sensorFilter.apply(sourceSensor)) {
            return; // ignoring excluded sensor
        }
        
        if (LOG.isTraceEnabled()) LOG.trace("enricher {} got {}, propagating via {}{}", 
                new Object[] {this, event, entity, (sourceSensor == destinationSensor ? "" : " (as "+destinationSensor+")")});
        
        emit((Sensor)destinationSensor, event.getValue());
    }

    /** useful once sensors are added to emit all values */
    public void emitAllAttributes() {
        emitAllAttributes(false);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void emitAllAttributes(boolean includeNullValues) {
        Iterable<? extends Sensor<?>> sensorsToPopulate = propagatingAll 
                ? Iterables.filter(producer.getEntityType().getSensors(), sensorFilter)
                : sensorMapping.keySet();

        for (Sensor<?> s : sensorsToPopulate) {
            if (s instanceof AttributeSensor) {
                AttributeSensor destinationSensor = (AttributeSensor<?>) getDestinationSensor(s);
                Object v = producer.getAttribute((AttributeSensor<?>)s);
                // TODO we should keep a timestamp for the source sensor and echo it 
                // (this pretends timestamps are current, which probably isn't the case when we are propagating)
                if (v != null || includeNullValues) entity.sensors().set(destinationSensor, v);
            }
        }
    }

    private Sensor<?> getDestinationSensor(final Sensor<?> sourceSensor) {
        // sensor equality includes the type; we want just name-equality so will use predicate.
        Optional<? extends Sensor<?>> mappingSensor = Iterables.tryFind(sensorMapping.keySet(), 
                SensorPredicates.nameEqualTo(sourceSensor.getName()));

        return mappingSensor.isPresent() ? sensorMapping.get(mappingSensor.get()) : sourceSensor;
    }

    private Map<Sensor<?>, Sensor<?>> resolveSensorMappings(Map<?,?> mapping) {
        if (mapping == null) {
            return MutableMap.of();
        }
        Map<Sensor<?>, Sensor<?>> result = MutableMap.of();
        for (Map.Entry<?,?> entry : mapping.entrySet()) {
            Object keyO = entry.getKey();
            Object valueO = entry.getValue();
            Sensor<?> key = Tasks.resolving(keyO).as(Sensor.class).timeout(ValueResolver.REAL_QUICK_WAIT).context(producer).get();
            Sensor<?> value = Tasks.resolving(valueO).as(Sensor.class).timeout(ValueResolver.REAL_QUICK_WAIT).context(producer).get();
            result.put(key, value);
        }
        return result;
    }
    
    private List<Sensor<?>> resolveSensorCollection(Iterable<?> sensors) {
        if (sensors == null) {
            return MutableList.of();
        }
        List<Sensor<?>> result = MutableList.of();
        for (Object sensorO : sensors) {
            Sensor<?> sensor = Tasks.resolving(sensorO).as(Sensor.class).timeout(ValueResolver.REAL_QUICK_WAIT).context(producer).get();
            result.add(sensor);
        }
        return result;
    }
    
    private <K,V> Map<K,V> filterForKeyEqualsValue(Map<K,V> map) {
        Map<K,V> result = Maps.newLinkedHashMap();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (Objects.equal(entry.getKey(), entry.getValue())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}
