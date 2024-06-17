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
package org.apache.brooklyn.rest.util;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityAttributesUtils {

    private static final Logger LOG = LoggerFactory.getLogger(EntityAttributesUtils.class);

    /**
     * Attempts to read {@link AttributeSensor} from {@link Entity}.
     *
     * @param entity The entity to read sensor from.
     * @param sensor The sensor to read value of.
     * @param <T>    The type of the {@link AttributeSensor}.
     * @return value of {@link AttributeSensor} if read is successful, or {@code null} otherwise.
     */
    public static <T> T tryGetAttribute(Entity entity, AttributeSensor<T> sensor) {
        T attribute = null;
        try {
            attribute = entity.getAttribute(sensor);
        } catch (Exception exception) {
            Exceptions.propagateIfFatal(exception);
            LOG.warn("Error retrieving sensor " + sensor + " for " + entity + " (ignoring): " + exception);
        }
        return attribute;
    }

    public static final Maybe SENSOR_NOT_SET = Maybe.absent("Sensor is unset");

    /** returns {@link #SENSOR_NOT_SET} if not defined; equals check can be done to compare that with a different error */
    public static <T> Maybe<T> getAttributeMaybe(Entity entity, AttributeSensor<T> sensor) {
        T attribute = null;
        try {
            attribute = entity.getAttribute(sensor);
            if (attribute==null) {
                if (!entity.sensors().getAll().keySet().stream().anyMatch(sn -> sn.getName().equals(sensor.getName()))) {
                    return SENSOR_NOT_SET;
                }
            }
        } catch (Exception exception) {
            Exceptions.propagateIfFatal(exception);
            LOG.warn("Error retrieving sensor " + sensor + " for " + entity + " (ignoring): " + exception);
            return Maybe.absent(exception);
        }
        return Maybe.ofAllowingNull(attribute);
    }
}
