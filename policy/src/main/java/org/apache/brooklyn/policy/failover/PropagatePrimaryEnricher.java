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
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.EffectorTasks.EffectorTaskFactory;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityInternal.SensorSupportInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.enricher.stock.Propagator;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Objects;

/** Makes all sensors/effectors available on primary mirrored at this node,
 * apart from those already present here. */ 
@Beta
public class PropagatePrimaryEnricher extends AbstractEnricher implements SensorEventListener<Object> {

    private static final Logger log = LoggerFactory.getLogger(PropagatePrimaryEnricher.class);
    
    public static final ConfigKey<String> PRIMARY_SENSOR_NAME = ElectPrimaryConfig.PRIMARY_SENSOR_NAME;
    
    public static final ConfigKey<Boolean> PROPAGATE_EFFECTORS = ConfigKeys.newBooleanConfigKey("propagate.effectors",
        "Whether to propagate effectors, default true (effectors already defined here will not be propagated)",
        true);

    public static final ConfigKey<Boolean> PROPAGATING_ALL = Propagator.PROPAGATING_ALL;
    public static final ConfigKey<Collection<? extends Sensor<?>>> PROPAGATING_ALL_BUT = Propagator.PROPAGATING_ALL_BUT;
    public static final ConfigKey<Collection<? extends Sensor<?>>> PROPAGATING = Propagator.PROPAGATING;
    public static final ConfigKey<Map<? extends Sensor<?>, ? extends Sensor<?>>> SENSOR_MAPPING = Propagator.SENSOR_MAPPING;
    
    Entity lastPrimary;
    Propagator propagator;
    
    Set<String> effectorsAddedForPrimary;
    Set<String> blacklistedEffectors = MutableSet.of("start", "stop", "restart", "promote", "demote",
        getConfig(ElectPrimaryConfig.PROMOTE_EFFECTOR_NAME), getConfig(ElectPrimaryConfig.DEMOTE_EFFECTOR_NAME));
    Set<String> blacklistedSensors = MutableSet.of();
    
    public void setEntity(@SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity) {
        super.setEntity(entity);
        
        blacklistedEffectors.addAll(((EntityInternal)entity).getMutableEntityType().getEffectors().keySet());
        blacklistedSensors.addAll(((EntityInternal)entity).getMutableEntityType().getSensors().keySet());
        for (Sensor<?> s: Propagator.SENSORS_NOT_USUALLY_PROPAGATED) {
            blacklistedSensors.add(s.getName());
        }
        blacklistedSensors.addAll(MutableSet.of(getConfig(PRIMARY_SENSOR_NAME), getConfig(ElectPrimaryConfig.PRIMARY_WEIGHT_NAME)));
        
        subscriptions().subscribe(entity, Sensors.newSensor(Entity.class, config().get(PRIMARY_SENSOR_NAME)), this);
        onEvent(null);
    }

    @Override
    public synchronized void onEvent(SensorEvent<Object> event) {
        Entity primary = entity.getAttribute( Sensors.newSensor(Entity.class, config().get(PRIMARY_SENSOR_NAME)) );
        if (!Objects.equal(primary, lastPrimary)) {
            log.debug("Removing propagated items from "+lastPrimary+" at "+entity);
            
            // remove propagator
            if (propagator!=null) {
                entity.enrichers().remove(propagator);
                propagator = null;
            }

            // remove propagated effectors
            if (effectorsAddedForPrimary!=null) {
                log.debug("Removing propagated effectors from "+lastPrimary+" at "+entity+": "+effectorsAddedForPrimary);
                for (String effN: effectorsAddedForPrimary) {
                    Effector<?> effE = ((EntityInternal)entity).getMutableEntityType().getEffector(effN);
                    if (effE!=null) {
                        ((EntityInternal)entity).getMutableEntityType().removeEffector(effE);
                    }
                }
                effectorsAddedForPrimary = null;
            }
            
            // remove all but blacklisted sensors
            Set<AttributeSensor<?>> sensorsToRemove = MutableSet.of();
            for (AttributeSensor<?> s: ((SensorSupportInternal)entity.sensors()).getAll().keySet()) {
                if (!blacklistedSensors.contains(s.getName())) {
                    sensorsToRemove.add(s);
                }
            }
            if (!sensorsToRemove.isEmpty()) {
                log.debug("Removing propagated sensors from "+lastPrimary+" at "+entity+": "+sensorsToRemove);
                for (AttributeSensor<?> s: sensorsToRemove) {
                    ((SensorSupportInternal)entity.sensors()).remove(s);
                }
            }
            
            
            if (primary!=null) {
                // add effectors
                propagateEffectors(primary);
                
                // add propagator
                addPropagatorEnricher(primary);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected void addPropagatorEnricher(Entity primary) {
        EnricherSpec<Propagator> spec = EnricherSpec.create(Propagator.class);
        if (Boolean.TRUE.equals( getConfig(PROPAGATING_ALL) )) {
            spec.configure(PROPAGATING_ALL, true);
        }
        
        Collection<Sensor<?>> allBut = (Collection<Sensor<?>>) getConfig(PROPAGATING_ALL_BUT);
        if (allBut!=null) {
            allBut = MutableSet.copyOf(allBut);
            for (String s: blacklistedSensors) {
                allBut.add(Sensors.newSensor(Object.class, s));
            }
            spec.configure(PROPAGATING_ALL_BUT, allBut);
        }
        
        if (getConfig(PROPAGATING)!=null) {
            spec.configure(PROPAGATING, getConfig(PROPAGATING));
        }
        
        if (getConfig(SENSOR_MAPPING)!=null) {
            spec.configure(SENSOR_MAPPING, getConfig(SENSOR_MAPPING));
        }
        
        spec.configure(Propagator.PRODUCER, primary);
        
        log.debug("Adding propagator "+spec+" to "+entity+" to track "+primary);
        propagator = entity.enrichers().add(spec);
    }

    @SuppressWarnings("unchecked")
    protected void propagateEffectors(Entity primary) {
        log.debug("Adding effectors to "+entity+" to track "+primary);
        if (effectorsAddedForPrimary==null) {
            effectorsAddedForPrimary = MutableSet.of();
        }
        if (getConfig(PROPAGATE_EFFECTORS)) {
            for (Effector<?> e: ((EntityInternal)primary).getMutableEntityType().getEffectors().values()) {
                if (!blacklistedEffectors.contains( e.getName() )) {
                    effectorsAddedForPrimary.add(e.getName());
                    ((EntityInternal)entity).getMutableEntityType().addEffector(
                        Effectors.effector((Effector<Object>)e).impl(new CallOtherEffector(primary)).build() );
                }
            }
        }
        log.debug("Added effectors "+effectorsAddedForPrimary+" to "+entity+" to track "+primary);
    }

    public static class CallOtherEffector implements EffectorTaskFactory<Object> {

        final Entity target;
        
        public CallOtherEffector(Entity target) {
            this.target = target;
        }
        
        @Override
        public TaskAdaptable<Object> newTask(Entity entity, Effector<Object> effector, ConfigBag parameters) {
            return DynamicTasks.queueIfPossible( 
                    Effectors.invocation(target, Effectors.effector(effector).buildAbstract(), parameters) ).
                orSubmitAsync(entity).asTask();
        }
    }

}
