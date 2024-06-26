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
package org.apache.brooklyn.rest.resources;

import static com.google.common.collect.Iterables.filter;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.EntityAndItem;
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;
import org.apache.brooklyn.rest.api.SensorApi;
import org.apache.brooklyn.rest.domain.SensorSummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.SensorTransformer;
import org.apache.brooklyn.rest.util.EntityAttributesUtils;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@HaHotStateRequired
public class SensorResource extends AbstractBrooklynRestResource implements SensorApi {

    private static final Logger log = LoggerFactory.getLogger(SensorResource.class);

    @Override
    public List<SensorSummary> list(final String application, final String entityToken) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }

        List<SensorSummary> result = Lists.newArrayList();
        
        for (AttributeSensor<?> sensor : filter(entity.getEntityType().getSensors(), AttributeSensor.class)) {
            // Exclude config that user is not allowed to see
            if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_SENSOR, new EntityAndItem<String>(entity, sensor.getName()))) {
                log.trace("User {} not authorized to see sensor {} of entity {}; excluding from AttributeSensor list results", 
                        new Object[] {Entitlements.getEntitlementContext().user(), sensor.getName(), entity});
                continue;
            }
            result.add(SensorTransformer.sensorSummary(entity, sensor, ui.getBaseUriBuilder()));
        }
        
        return result;
    }

    @Override
    public Map<String, Object> batchSensorRead(final String application, final String entityToken, final Boolean useDisplayHints, final Boolean raw, final Boolean suppressSecrets) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }

        Map<String, Object> sensorMap = Maps.newHashMap();
        @SuppressWarnings("rawtypes")
        Iterable<AttributeSensor> sensors = filter(entity.getEntityType().getSensors(), AttributeSensor.class);

        for (AttributeSensor<?> sensor : sensors) {
            try {
                // Exclude sensors that user is not allowed to see
                if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_SENSOR, new EntityAndItem<String>(entity, sensor.getName()))) {
                    log.trace("User {} not authorized to see sensor {} of entity {}; excluding from current-state results",
                            new Object[]{Entitlements.getEntitlementContext().user(), sensor.getName(), entity});
                    continue;
                }

                Object value = EntityAttributesUtils.tryGetAttribute(entity, findSensor(entity, sensor.getName()));
                sensorMap.put(sensor.getName(),
                        resolving(value).preferJson(true).asJerseyOutermostReturnValue(false).useDisplayHints(useDisplayHints).raw(raw).context(entity).timeout(Duration.ZERO).renderAs(sensor)
                                .suppressIfSecret(sensor.getName(), suppressSecrets).filterOutputFields(sensor.getName().startsWith("internal")).resolve());
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                log.error(""+sensor+" on "+entity+" cannot be serialized for REST output; ignoring: "+e, e);
                sensorMap.put(sensor.getName(), MutableMap.of("type", "error", "message", "Value not available. See logs."));
            }
        }
        return sensorMap;
    }

    protected Object get(boolean preferJson, String application, String entityToken, String sensorName, Boolean useDisplayHints, Boolean raw, final Boolean suppressSecrets) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        AttributeSensor<?> sensor = findSensor(entity, sensorName);
        
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_SENSOR, new EntityAndItem<String>(entity, sensor.getName()))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see entity '%s' sensor '%s'",
                    Entitlements.getEntitlementContext().user(), entity, sensor.getName());
        }

        Maybe<?> vm = EntityAttributesUtils.getAttributeMaybe(entity, sensor);
        if (vm.isAbsent()) {
             Sensor<?> sensorDefinition = ((EntityInternal) entity).getMutableEntityType().getSensor(sensorName);

//            boolean sensorInMap = ((EntityInternal) entity).sensors().getAll().keySet().stream().anyMatch(k -> k.getName().equals(sensorName));
            // if sensor is defined, but value unavailable, return 424

            // if sensor defined, but not set, retun 204
            if (vm!=EntityAttributesUtils.SENSOR_NOT_SET) {
//                // could support 424, but that would probably be done below; this is raw, so any error is low level
//                return WebResourceUtils.dependencyFailed("Value specified but not resolvable.");
                vm.get();
            }

            if (sensorDefinition!=null && vm==EntityAttributesUtils.SENSOR_NOT_SET) {
                 return WebResourceUtils.noContent("No value for sensor");
            }

            // if sensor is not defined, return 404
            throw WebResourceUtils.notFound("Sensor '%s' not known", sensorName);

        }
        return resolving(vm.get()).preferJson(preferJson).asJerseyOutermostReturnValue(true).useDisplayHints(useDisplayHints).raw(raw).context(entity).immediately(true).renderAs(sensor)
                .suppressIfSecret(sensorName, suppressSecrets).resolve();
    }

    @Override
    public String getPlain(String application, String entityToken, String sensorName,
                           Boolean useDisplayHints, Boolean raw, final Boolean suppressSecrets) {
        return (String) get(false, application, entityToken, sensorName, useDisplayHints, raw, suppressSecrets);
    }

    @Override
    public Object get(final String application, final String entityToken, String sensorName, Boolean useDisplayHints, Boolean raw, final Boolean suppressSecrets) {
        return get(true, application, entityToken, sensorName, useDisplayHints, raw, suppressSecrets);
    }

    private AttributeSensor<?> findSensor(Entity entity, String name) {
        Sensor<?> s = entity.getEntityType().getSensor(name);
        if (s instanceof AttributeSensor) return (AttributeSensor<?>) s;
        return new BasicAttributeSensor<Object>(Object.class, name);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void setFromMap(String application, String entityToken, Map newValues) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                Entitlements.getEntitlementContext().user(), entity);
        }

        if (log.isDebugEnabled())
            log.debug("REST user "+Entitlements.getEntitlementContext()+" setting sensors "+newValues);
        for (Object entry: newValues.entrySet()) {
            String sensorName = Strings.toString(((Map.Entry)entry).getKey());
            Object newValue = ((Map.Entry)entry).getValue();
            
            AttributeSensor sensor = findSensor(entity, sensorName);
            entity.sensors().set(sensor, newValue);
        }
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void set(String application, String entityToken, String sensorName, Object newValue) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                Entitlements.getEntitlementContext().user(), entity);
        }
        
        AttributeSensor sensor = findSensor(entity, sensorName);
        if (log.isDebugEnabled())
            log.debug("REST user "+Entitlements.getEntitlementContext()+" setting sensor "+sensorName+" to "+newValue);
        entity.sensors().set(sensor, newValue);
    }
    
    @Override
    public void delete(String application, String entityToken, String sensorName) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                Entitlements.getEntitlementContext().user(), entity);
        }
        
        AttributeSensor<?> sensor = findSensor(entity, sensorName);
        if (log.isDebugEnabled())
            log.debug("REST user "+Entitlements.getEntitlementContext()+" deleting sensor "+sensorName);
        ((EntityInternal)entity).sensors().remove(sensor);
        ((EntityInternal)entity).getMutableEntityType().removeSensor(sensor);
    }
    
}
