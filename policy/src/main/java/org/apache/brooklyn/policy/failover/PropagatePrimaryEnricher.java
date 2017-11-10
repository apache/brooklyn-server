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
package org.apache.brooklyn.policy.failover;

import java.util.Collection;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.enricher.stock.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Objects;

/** Makes selected sensors mirrored from the primary to this node. */ 
@Beta
public class PropagatePrimaryEnricher extends AbstractEnricher implements SensorEventListener<Object> {

    private static final Logger log = LoggerFactory.getLogger(PropagatePrimaryEnricher.class);
    
    public static final ConfigKey<String> PRIMARY_SENSOR_NAME = ElectPrimaryConfig.PRIMARY_SENSOR_NAME;
    
    public static final ConfigKey<Entity> CURRENT_PROPAGATED_PRODUCER = ConfigKeys.newConfigKey(Entity.class, "propagate.primary.enricher.current.producer");
    // persistence of references to adjuncts not supported, so use the ID
    public static final ConfigKey<String> CURRENT_PROPAGATOR_ID = ConfigKeys.newStringConfigKey("propagate.primary.enricher.current.propagatorId");
    
    public static final ConfigKey<Collection<? extends Sensor<?>>> PROPAGATING = Propagator.PROPAGATING;
    // the above is the only one currently supported - explicitly named sensors
    
    // NB: old code in history had much of the below working, but not for rebind
    
    // other propagator fields (eg "all but") are not
//    public static final ConfigKey<Boolean> PROPAGATING_ALL = Propagator.PROPAGATING_ALL;
//    public static final ConfigKey<Collection<? extends Sensor<?>>> PROPAGATING_ALL_BUT = Propagator.PROPAGATING_ALL_BUT;
//    public static final ConfigKey<Map<? extends Sensor<?>, ? extends Sensor<?>>> SENSOR_MAPPING = Propagator.SENSOR_MAPPING;

    // also no longer support effectors
//    public static final ConfigKey<Boolean> PROPAGATE_EFFECTORS = ConfigKeys.newBooleanConfigKey("propagate.effectors",
//        "Whether to propagate effectors, default true (effectors already defined here will not be propagated)",
//        true);
    
    public void setEntity(@SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity) {
        super.setEntity(entity);
        
        subscriptions().subscribe(entity, Sensors.newSensor(Entity.class, config().get(PRIMARY_SENSOR_NAME)), this);
        onEvent(null);
    }

    @Override
    public synchronized void onEvent(SensorEvent<Object> event) {
        Entity primary = entity.getAttribute( Sensors.newSensor(Entity.class, config().get(PRIMARY_SENSOR_NAME)) );
        final Entity lastPrimary = config().get(CURRENT_PROPAGATED_PRODUCER);
        if (!Objects.equal(primary, lastPrimary)) {
            log.debug("Removing propagated items from "+lastPrimary+" at "+entity);
            
            final Propagator propagator = getManagementContext().lookup(config().get(CURRENT_PROPAGATOR_ID), Propagator.class);
            // remove propagator
            if (propagator!=null) {
                entity.enrichers().remove(propagator);
                config().set(CURRENT_PROPAGATOR_ID, (String)null);
            }
            
            // remove propagated sensors
            Collection<? extends Sensor<?>> sensorsToRemove = config().get(PROPAGATING);
            if (sensorsToRemove!=null) {
                for (Sensor<?> s: sensorsToRemove) {
                    if (s instanceof AttributeSensor) {
                        ((EntityInternal)entity).sensors().remove((AttributeSensor<?>)s);
                    }
                }
            }            
            
            if (primary!=null) {
                config().set(CURRENT_PROPAGATED_PRODUCER, primary);
                
                // add propagator
                addPropagatorEnricher(primary);
            }
        }
    }
    
    @Override
    protected <T> void doReconfigureConfig(ConfigKey<T> key, T val) {
        if (CURRENT_PROPAGATED_PRODUCER.equals(key)) return;
        if (CURRENT_PROPAGATOR_ID.equals(key)) return;
        // disallow anything else
        super.doReconfigureConfig(key, val);
    }

    protected void addPropagatorEnricher(Entity primary) {
        EnricherSpec<Propagator> spec = EnricherSpec.create(Propagator.class);
        spec.configure(Propagator.PRODUCER, primary);
        
        Collection<? extends Sensor<?>> sensorsToPropagate = getConfig(PROPAGATING);
        if (sensorsToPropagate==null || sensorsToPropagate.isEmpty()) {
            log.warn("");
            return;
        }
        spec.configure(Propagator.PROPAGATING, sensorsToPropagate);
        
        // note - history 
        
        log.debug("Adding propagator "+spec+" to "+entity+" to track "+primary);
        Propagator propagator = entity.enrichers().add(spec);
        config().set(CURRENT_PROPAGATOR_ID, propagator.getId()); 
    }

}
