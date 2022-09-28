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
package org.apache.brooklyn.core.workflow;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddSensorInitializer;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.sensor.AbstractAddTriggerableSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/** 
 * Configurable {@link EntityInitializer} which adds a sensor feed running a given workflow.
 */
@Beta
public final class WorkflowSensor<T> extends AbstractAddTriggerableSensor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowSensor.class);

    // override to be object
    public static final ConfigKey<String> SENSOR_TYPE = ConfigKeys.newConfigKeyWithDefault(AddSensorInitializer.SENSOR_TYPE, Object.class.getName());

    // do we need to have an option not to run when initialization is done?

    public WorkflowSensor() {}
    public WorkflowSensor(ConfigBag params) {
        super(params);
    }

    @Override
    public void apply(final EntityLocal entity) {
        ConfigBag params = initParams();

        // previously if a commandUrl was used we would listen for the install dir to be set; but that doesn't survive rebind;
        // now we install on first run as part of the SshFeed
        apply(entity, params);
    }

    private void apply(final EntityLocal entity, final ConfigBag params) {

        AttributeSensor<T> sensor = addSensor(entity);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding workflow sensor {} to {}", sensor.getName(), entity);
        }

        FunctionPollConfig<Object,Object> pollConfig = new FunctionPollConfig<Object,T>(sensor)
                .callable(new WorkflowSensorCallable(sensor.getName(), entity, params))
                .onSuccess(TypeCoercions.<T>function((Class)sensor.getTypeToken().getRawType()));

        standardPollConfig(entity, initParams(), pollConfig);

        FunctionFeed.Builder feedBuilder = FunctionFeed.builder()
                .name("Sensor Workflow Feed: "+sensor.getName())
                .entity(entity)
                .onlyIfServiceUp(Maybe.ofDisallowingNull(EntityInitializers.resolve(params, ONLY_IF_SERVICE_UP)).or(false))
                .poll(pollConfig);

        FunctionFeed feed = feedBuilder.build();
        entity.addFeed(feed);

    }

    @Override
    protected AttributeSensor<T> sensor(Entity entity) {
        // overridden because sensor type defaults to object here
        TypeToken<T> clazz = getType(entity, initParam(SENSOR_TYPE));
        return Sensors.newSensor(clazz, Preconditions.checkNotNull(initParam(SENSOR_NAME)));
    }

    static class WorkflowSensorCallable implements Callable<Object> {
        private final String sensorName;
        private final EntityLocal entity;
        private final ConfigBag params;

        public WorkflowSensorCallable(String sensorName, EntityLocal entity, ConfigBag params) {
            this.sensorName = sensorName;
            this.entity = entity;
            this.params = params;
        }

        @Override
        public Object call() throws Exception {
            WorkflowExecutionContext wc = WorkflowExecutionContext.of(entity, "Workflow for sensor " + sensorName, params, null, null);
            Maybe<Task<Object>> wt = wc.getOrCreateTask(false /* condition checked by poll config framework */);
            return DynamicTasks.queue(wt.get()).getUnchecked();
        }
    }
}
