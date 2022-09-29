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
package org.apache.brooklyn.core.sensor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.Beta;
import com.google.common.base.Predicates;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.feed.*;
import org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Super-class for entity initializers that add feeds.
 */
@Beta
public abstract class AbstractAddTriggerableSensor<T> extends AbstractAddSensorFeed<T> {

    public static final ConfigKey<Duration> SENSOR_PERIOD = ConfigKeys.newConfigKey(Duration.class, "period", "Period, including units e.g. 1m or 5s or 200ms", null);
    public static final ConfigKey<Object> SENSOR_TRIGGERS = ConfigKeys.newConfigKey(new TypeToken<Object>() {}, "triggers",
            "Sensors which should trigger this feed, supplied with list of maps containing sensor (name or sensor instance) and entity (ID or entity instance), or just sensor names or just one sensor");
    public static final ConfigKey<DslPredicates.DslPredicate> CONDITION = ConfigKeys.newConfigKey(DslPredicates.DslPredicate.class, "condition", "Optional condition required for this sensor feed to run");

    public static final ConfigKey<Boolean> ONLY_IF_SERVICE_UP = ConfigKeys.newBooleanConfigKey("onlyIfServiceUp", "Whether to run only if service is up.", null);

    protected AbstractAddTriggerableSensor() {}
    public AbstractAddTriggerableSensor(ConfigBag parameters) {
        super(parameters);
    }

    @JsonIgnore
    protected Duration getPeriod(Entity context, ConfigBag config) {
        if (config.containsKey(SENSOR_PERIOD) || !hasTriggers(config)) {
            if (context!=null) return Tasks.resolving(config, SENSOR_PERIOD).context(context).immediately(true).get();
            else return config.get(SENSOR_PERIOD);
        }
        return Duration.PRACTICALLY_FOREVER;
    }

    @JsonIgnore
    protected Maybe<Object> getTriggersMaybe(Entity context, ConfigBag config) {
        return Tasks.resolving(config, SENSOR_TRIGGERS).context(context).deep().immediately(true).getMaybe();
    }

    @Beta
    public static List<Pair<Entity,Sensor>> resolveTriggers(Entity context, Object otherTriggers) {
        Object triggers = Tasks.resolving(otherTriggers, Object.class).context(context).deep().immediately(true).get();

        if (triggers==null || (triggers instanceof Collection && ((Collection)triggers).isEmpty())) return Collections.emptyList();
        if (triggers instanceof String) {
            SensorFeedTrigger t = new SensorFeedTrigger();
            t.sensorName = (String)triggers;
            triggers = MutableList.of(t);
        }
        if (!(triggers instanceof Collection)) {
            throw new IllegalStateException("Triggers should be a list containing sensors or sensor names");
        }

        return ((Collection<?>)triggers).stream().map(ti -> {
            SensorFeedTrigger t;

            if (ti instanceof SensorFeedTrigger) {
                t = (SensorFeedTrigger) ti;
            } else {
                if (ti instanceof Map) {
                    t = Tasks.resolving(ti, SensorFeedTrigger.class).context(context).deep().get();
                } else if (ti instanceof String) {
                    t = new SensorFeedTrigger();
                    t.sensorName = (String) ti;
                } else {
                    throw new IllegalStateException("Trigger should be a map specifyin entity and sensor");
                }
            }

            Entity entity = t.entity;
            if (entity==null && t.entityId!=null) {
                String desiredComponentId = t.entityId;
                List<Entity> firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context,
                        Predicates.and(EntityPredicates.configEqualTo(BrooklynConfigKeys.PLAN_ID, desiredComponentId), x->true)::apply);
                if (firstGroupOfMatches.isEmpty()) {
                    firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context,
                            Predicates.and(EntityPredicates.idEqualTo(desiredComponentId), x->true)::apply);
                }
                if (!firstGroupOfMatches.isEmpty()) {
                    entity = firstGroupOfMatches.get(0);
                } else {
                    throw new IllegalStateException("Cannot find entity with ID '"+desiredComponentId+"'");
                }
            } else {
                entity = context;
            }

            Sensor sensor = t.sensor;
            if (sensor==null) {
                if (t.sensorName!=null) {
                    sensor = entity.getEntityType().getSensor(t.sensorName);
                    if (sensor==null) sensor = Sensors.newSensor(Object.class, t.sensorName);
                } else {
                    throw new IllegalStateException("Sensor is required for a trigger");
                }
            }
            return Pair.of(entity, sensor);
        }).collect(Collectors.toList());
    }

    protected boolean hasTriggers(ConfigBag config) {
        Maybe<Object> triggers = getTriggersMaybe(null, config);
        if (triggers==null || triggers.isAbsent()) return false;
        if (triggers.get() instanceof Collection && ((Collection)triggers.get()).isEmpty()) return false;
        return true;
    }

    public static class SensorFeedTrigger {
        Entity entity;
        @JsonIgnore
        String entityId;
        Sensor<?> sensor;
        @JsonIgnore
        String sensorName;

        // could support predicates on the value; but we do it on the entity which is enough

        public void setEntity(Entity entity) {
            this.entity = entity;
        }
        public void setEntity(String entityId) {
            this.entityId = entityId;
        }
        public Object getEntity() {
            return entity!=null ? entity : entityId;
        }

        public void setSensor(Sensor<?> sensor) {
            this.sensor = sensor;
        }
        public void setSensor(String sensorName) {
            this.sensorName = sensorName;
        }
        public Object getSensor() {
            return sensor!=null ? sensor : sensorName;
        }
    }


    protected void standardPollConfig(Entity entity, ConfigBag configBag, PollConfig<?,?,?> poll) {
        final Boolean suppressDuplicates = EntityInitializers.resolve(configBag, SUPPRESS_DUPLICATES);
        final Duration logWarningGraceTimeOnStartup = EntityInitializers.resolve(configBag, LOG_WARNING_GRACE_TIME_ON_STARTUP);
        final Duration logWarningGraceTime = EntityInitializers.resolve(configBag, LOG_WARNING_GRACE_TIME);

        poll.suppressDuplicates(Boolean.TRUE.equals(suppressDuplicates))
                .logWarningGraceTimeOnStartup(logWarningGraceTimeOnStartup)
                .logWarningGraceTime(logWarningGraceTime)
                .period(getPeriod(entity, initParams()))
                .otherTriggers(getTriggersMaybe(entity, configBag).orNull())
                .condition(new ConditionSupplierFromConfigBag(configBag, entity));
    }

    static class ConditionSupplierFromConfigBag implements Supplier<DslPredicates.DslPredicate> {
        final ConfigBag configBag;
        final Entity entity;

        ConditionSupplierFromConfigBag(ConfigBag configBag, Entity entity) {
            this.configBag = configBag;
            this.entity = entity;
        }

        @Override
        public DslPredicates.DslPredicate get() {
            return Tasks.resolving(configBag, CONDITION).context(entity).deep().immediately(true).get();
        }
    }

}
