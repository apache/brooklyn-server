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
package org.apache.brooklyn.util.core.xstream;

import com.thoughtworks.xstream.mapper.Mapper;
import com.thoughtworks.xstream.mapper.MapperWrapper;
import java.io.Serializable;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Not to be confused with {@link com.thoughtworks.xstream.mapper.LambdaMapper} which actually makes an effort,
 * this simply fails or warns once, depending on configuration.
 */
public class LambdaPreventionMapper extends MapperWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(LambdaPreventionMapper.class);

    private static final Pattern lambdaPattern = Pattern.compile(".*\\$\\$Lambda\\$[0-9]+/.*");
    
    enum LambdaPersistenceMode { ACCEPT, WARN, FAIL };

    public static final ConfigKey<LambdaPersistenceMode> LAMBDA_PERSISTENCE = ConfigKeys.newConfigKey(LambdaPersistenceMode.class, "brooklyn.persistence.advanced.lambdas",
            "How to handle attempts to persist data (entities, esp effectors; feeds; etc) containing lambdas; default since Apache Brooklyn 1.1 is to 'FAIL' on lambdas that don't implement Serializable and 'WARN' on those which do, " +
                    "because the former is guaranteed not to work and the latter is JVM-specific, and in all cases a concrete class is preferable. " +
                    "This setting can be used to restore previous behavior where required, where Brooklyn will silently 'ACCEPT' them even though it often won't restore on de-serialization, " +
                    "or a halfway house of 'WARN' (once only per type). Note the lambdas which implement Serializable may work in some JVMs but then break if deserialized to a different VM," +
                    "and lambdas that do not implement Serializable will always serialize then restore as null. The default FAIL will flag these errors on creation. " +
                    "The two cases 'if_serializale' and 'if_non_serializable' can be customized using sub-keys.", LambdaPersistenceMode.FAIL);
    public static final ConfigKey<LambdaPersistenceMode> LAMBDA_PERSISTENCE_SERIALIZABLE = ConfigKeys.newConfigKey(LambdaPersistenceMode.class, "brooklyn.persistence.advanced.lambdas.if_serializable", null, LambdaPersistenceMode.WARN);
    public static final ConfigKey<LambdaPersistenceMode> LAMBDA_PERSISTENCE_NON_SERIALIZABLE = ConfigKeys.newConfigKey(LambdaPersistenceMode.class, "brooklyn.persistence.advanced.lambdas.if_non_serializable", null, LambdaPersistenceMode.FAIL);


    static Set<String> warnedTypes = MutableSet.of();

    private final LambdaPersistenceMode persistenceMode;
    private final LambdaPersistenceMode persistenceModeIfSerializable;
    private final LambdaPersistenceMode persistenceModeIfNonSerializable;

    public static Function<MapperWrapper,MapperWrapper> factory(ConfigBag properties) {
        return factory(
                properties.getFirst(LAMBDA_PERSISTENCE),
                properties.getFirst(LAMBDA_PERSISTENCE_SERIALIZABLE, LAMBDA_PERSISTENCE),
                properties.getFirst(LAMBDA_PERSISTENCE_NON_SERIALIZABLE, LAMBDA_PERSISTENCE) );
    }

    public static Function<MapperWrapper,MapperWrapper> factory(@Nonnull LambdaPersistenceMode persistenceMode, @Nullable LambdaPersistenceMode persistenceModeIfSerializable, @Nullable LambdaPersistenceMode persistenceModeIfNonSerializable) {
        return wrapper -> new LambdaPreventionMapper(wrapper, persistenceMode, persistenceModeIfSerializable, persistenceModeIfNonSerializable);
    }

    public LambdaPreventionMapper(Mapper wrapped, @Nonnull LambdaPersistenceMode persistenceMode, @Nullable LambdaPersistenceMode persistenceModeIfSerializable, @Nullable LambdaPersistenceMode persistenceModeIfNonSerializable) {
        super(wrapped);
        this.persistenceMode = persistenceMode;
        this.persistenceModeIfSerializable = persistenceModeIfSerializable;
        this.persistenceModeIfNonSerializable = persistenceModeIfNonSerializable;
    }

    public String serializedClass(Class type) {
        if (isLambdaType(type)) {
            LambdaPersistenceMode mode = null;
            if (Serializable.class.isAssignableFrom(type)) {
                mode = persistenceModeIfSerializable;
            } else {
                mode = persistenceModeIfNonSerializable;
            }
            if (mode==null) {
                mode = persistenceMode;
            }
            if (mode==null) {
                mode = LAMBDA_PERSISTENCE.getDefaultValue();
            }
            switch (mode) {
                case ACCEPT:
                    break;
                case FAIL:
                    throw new IllegalStateException("Attempt to XML serialize lambda: " + type + "; forbidden by configuration");
                case WARN:
                    if (warnedTypes.add(type.toString())) {
                        LOG.warn("Attempt to XML serialize lambda: " + type + "; allowed, but may cause problems on de-serialization");
                    }
                    break;
            }
        }

        return super.serializedClass(type);
    }

    // from com.thoughtworks.xstream.core.util.Types but that package isn't exported
    public static final boolean isLambdaType(Class<?> type) {
        return type != null && type.isSynthetic() && lambdaPattern.matcher(type.getSimpleName()).matches();
    }
}
