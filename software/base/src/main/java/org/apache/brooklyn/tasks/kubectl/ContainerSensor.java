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
package org.apache.brooklyn.tasks.kubectl;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.AbstractAddTriggerableSensor;
import org.apache.brooklyn.core.sensor.ssh.SshCommandSensor;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

import static org.apache.brooklyn.core.mgmt.BrooklynTaskTags.SENSOR_TAG;

@SuppressWarnings({"UnstableApiUsage", "deprecation", "unchecked"})
public class ContainerSensor<T> extends AbstractAddTriggerableSensor<T> implements ContainerCommons {

    public static final ConfigKey<String> FORMAT = SshCommandSensor.FORMAT;
    public static final ConfigKey<Boolean> LAST_YAML_DOCUMENT = SshCommandSensor.LAST_YAML_DOCUMENT;

    private static final Logger LOG = LoggerFactory.getLogger(ContainerSensor.class);

    public ContainerSensor() {
    }

    public ContainerSensor(final ConfigBag parameters) {
        super(parameters);
    }

    @Override
    public void apply(final EntityLocal entity) {
        AttributeSensor<String> sensor = (AttributeSensor<String>) addSensor(entity);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding container sensor {} to {}", initParam(SENSOR_NAME), entity);
        }

        ConfigBag configBag = ConfigBag.newInstanceCopying(initParams());

        FunctionPollConfig<Object, String> poll = new FunctionPollConfig<>(sensor)
                .callable(new ContainerSensorCallable(entity, configBag, sensor));
        standardPollConfig(entity, configBag, poll);

        FunctionFeed.builder()
                .name("Container Sensor Feed: "+initParam(SENSOR_NAME))
                .entity(entity)
                .onlyIfServiceUp(Maybe.ofDisallowingNull(EntityInitializers.resolve(initParams(), ONLY_IF_SERVICE_UP)).or(false))
                .poll(poll)
                .build(true);
    }

    public static class ContainerSensorCallable implements Callable<Object> {
        private final Entity entity;
        private final ConfigBag configBag;
        private final Sensor<?> sensor;

        public ContainerSensorCallable(Entity entity, ConfigBag configBag, Sensor<?> sensor) {
            this.entity = entity;
            this.configBag = configBag;
            this.sensor = sensor;
        }
        public Object call() throws Exception {
            Task<ContainerTaskResult> containerTask = ContainerTaskFactory.newInstance()
                    .summary("Running " + EntityInitializers.resolve(configBag, SENSOR_NAME))
                    .jobIdentifier(entity.getApplication() + "-" + entity.getId() + "-" + SENSOR_TAG)
                    .configure(configBag.getAllConfig())
                    .newTask();
            DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);
            String mainStdout = containerTask.getUnchecked(configBag.get(TIMEOUT)).getMainStdout();
            return (new SshCommandSensor.CoerceOutputFunction<>(sensor.getTypeToken(), configBag.get(FORMAT), configBag.get(LAST_YAML_DOCUMENT))).apply(mainStdout);
        }

        @Override
        public String toString() {
            return "container-sensor[" + configBag.get(CONTAINER_IMAGE) + "]";
        }
    }

}
