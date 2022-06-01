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
package org.apache.brooklyn.core.effector;

import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution.BrooklynTypeNameResolver;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.time.Duration;

/**
 * Entity initializer which adds a sensor to an entity.
 *
 * @since 0.7.0 */
@Beta
public interface AddSensorInitializerAbstractProto<T> extends EntityInitializer {

    public static final ConfigKey<String> SENSOR_NAME = ConfigKeys.newStringConfigKey("name", "The name of the sensor to create");
    public static final ConfigKey<Duration> SENSOR_PERIOD = ConfigKeys.newConfigKey(Duration.class, "period", "Period, including units e.g. 1m or 5s or 200ms; default 5 minutes", Duration.FIVE_MINUTES);
    public static final ConfigKey<String> SENSOR_TYPE = ConfigKeys.newStringConfigKey("targetType", "Target type for the value; default String", "java.lang.String");

    @SuppressWarnings("unchecked")
    @Beta
    public static <T> TypeToken<T> getType(Entity entity, String className, String name) {
        return (TypeToken<T>)(TypeToken) new BrooklynTypeNameResolver("sensor "+name+" on "+entity,
                RegisteredTypes.getClassLoadingContext(entity), true, true).getTypeToken(className);
    }
}
