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

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddSensor;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.annotations.Beta;

/**
 * Super-class for entity initializers that add feeds.
 */
@Beta
public abstract class AbstractAddSensorFeed<T> extends AddSensor<T> {

    public static final ConfigKey<Boolean> SUPPRESS_DUPLICATES = ConfigKeys.newBooleanConfigKey(
            "suppressDuplicates", 
            "Whether to publish the sensor value again, if it is the same as the previous value",
            Boolean.FALSE);
    
    public static final ConfigKey<Duration> LOG_WARNING_GRACE_TIME_ON_STARTUP = ConfigKeys.newDurationConfigKey(
            "logWarningGraceTimeOnStartup",
            "On startup, the length of time before which a failure can be logged at WARN. "
                    + "This grace period is useful to avoid flooding the logs if the feed is expected " 
                    + "to sometimes be unavailable for a few seconds while the process-under-management" 
                    + "initialises.",
            Duration.millis(0));

    public static final ConfigKey<Duration> LOG_WARNING_GRACE_TIME = ConfigKeys.newDurationConfigKey(
            "logWarningGraceTime",
            "Length of time, after a successful poll, before a subsequent failure can be logged at WARN.",
            Duration.millis(0));

    public AbstractAddSensorFeed(final ConfigBag params) {
        super(params);
    }
}
