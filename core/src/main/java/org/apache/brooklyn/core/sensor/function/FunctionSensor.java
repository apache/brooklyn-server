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
package org.apache.brooklyn.core.sensor.function;

import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.sensor.AbstractAddSensorFeed;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Functions;
import com.google.common.reflect.TypeToken;

/**
 * Configurable {@link org.apache.brooklyn.api.entity.EntityInitializer} which adds a function sensor feed.
 * This calls the function periodically, to compute the sensor's value.
 *
 * @see FunctionFeed
 */
@Beta
public final class FunctionSensor<T> extends AbstractAddSensorFeed<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionSensor.class);

    @SuppressWarnings("serial")
    public static final ConfigKey<Callable<?>> FUNCTION = ConfigKeys.newConfigKey(
            new TypeToken<Callable<?>>() {},
            "function",
            "The callable to be executed periodically",
            null);

    public FunctionSensor(final ConfigBag params) {
        super(params);
    }

    @Override
    public void apply(final EntityLocal entity) {
        super.apply(entity);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding HTTP JSON sensor {} to {}", name, entity);
        }

        final ConfigBag allConfig = ConfigBag.newInstanceCopying(this.params).putAll(params);
        
        final Callable<?> function = EntityInitializers.resolve(allConfig, FUNCTION);
        final Boolean suppressDuplicates = EntityInitializers.resolve(allConfig, SUPPRESS_DUPLICATES);
        final Duration logWarningGraceTimeOnStartup = EntityInitializers.resolve(allConfig, LOG_WARNING_GRACE_TIME_ON_STARTUP);
        final Duration logWarningGraceTime = EntityInitializers.resolve(allConfig, LOG_WARNING_GRACE_TIME);

        FunctionPollConfig<?, T> pollConfig = new FunctionPollConfig<Object, T>(sensor)
                .callable(function)
                .onFailureOrException(Functions.constant((T) null))
                .suppressDuplicates(Boolean.TRUE.equals(suppressDuplicates))
                .logWarningGraceTimeOnStartup(logWarningGraceTimeOnStartup)
                .logWarningGraceTime(logWarningGraceTime)
                .period(period);

        FunctionFeed feed = FunctionFeed.builder().entity(entity)
                .poll(pollConfig)
                .build();

        entity.addFeed(feed);
    }
}
