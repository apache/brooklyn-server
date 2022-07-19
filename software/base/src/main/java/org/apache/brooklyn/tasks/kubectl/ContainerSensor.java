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

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.AbstractAddSensorFeed;
import org.apache.brooklyn.core.sensor.ssh.SshCommandSensor;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.brooklyn.core.mgmt.BrooklynTaskTags.SENSOR_TAG;

@SuppressWarnings({"UnstableApiUsage", "deprecation", "unchecked"})
public class ContainerSensor<T> extends AbstractAddSensorFeed<T> implements ContainerCommons {

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

        final Boolean suppressDuplicates = EntityInitializers.resolve(configBag, SUPPRESS_DUPLICATES);
        final Duration logWarningGraceTimeOnStartup = EntityInitializers.resolve(configBag, LOG_WARNING_GRACE_TIME_ON_STARTUP);
        final Duration logWarningGraceTime = EntityInitializers.resolve(configBag, LOG_WARNING_GRACE_TIME);

        ((EntityInternal)entity).feeds().add(FunctionFeed.builder()
                .entity(entity)
                .period(initParam(SENSOR_PERIOD))
                .onlyIfServiceUp()
                .poll(new FunctionPollConfig<>(sensor)
                        .callable(new Callable<Object>() {
                            @Override
                            public Object call() throws Exception {
                                Task<ContainerTaskFactory.ContainerTaskResult> containerTask = new ContainerTaskFactory.ConcreteContainerTaskFactory()
                                        .summary("Running " + EntityInitializers.resolve(configBag, SENSOR_NAME))
                                        .jobIdentifier(entity.getId() + "-" + SENSOR_TAG)
                                        .configure(configBag.getAllConfig())
                                        .newTask();
                                DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);
                                String mainStdout = containerTask.getUnchecked(Duration.of(5, TimeUnit.MINUTES)).getMainStdout();
                                return (new SshCommandSensor.CoerceOutputFunction<>(sensor.getTypeToken(), initParam(FORMAT), initParam(LAST_YAML_DOCUMENT))).apply(mainStdout);
                            }
                        })
                        .suppressDuplicates(Boolean.TRUE.equals(suppressDuplicates))
                        .logWarningGraceTimeOnStartup(logWarningGraceTimeOnStartup)
                        .logWarningGraceTime(logWarningGraceTime))
                .build());
    }


}

