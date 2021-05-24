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
package org.apache.brooklyn.core.objs;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.entity.internal.ConfigUtilsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * This is the actual type of a policy instance at runtime.
 */
public class AdjunctType implements Serializable {
    private static final long serialVersionUID = -662979234559595903L;

    private static final Logger LOG = LoggerFactory.getLogger(AdjunctType.class);

    private final String name;
    private final Map<String, ConfigKey<?>> configKeys;
    private final Set<ConfigKey<?>> configKeysSet;

    public AdjunctType(AbstractEntityAdjunct adjunct) {
        this(adjunct.getClass(), adjunct);
    }
    
    protected AdjunctType(Class<? extends EntityAdjunct> clazz) {
        this(clazz, null);
    }
    
    private AdjunctType(Class<? extends EntityAdjunct> clazz, AbstractEntityAdjunct adjunct) {
        name = clazz.getCanonicalName();
        configKeys = Collections.unmodifiableMap(findConfigKeys(clazz, null));
        configKeysSet = ImmutableSet.copyOf(this.configKeys.values());
        if (LOG.isTraceEnabled())
            LOG.trace("Policy {} config keys: {}", name, Joiner.on(", ").join(configKeys.keySet()));
    }
    
    AdjunctType(String name, Map<String, ConfigKey<?>> configKeys) {
        this.name = name;
        this.configKeys = ImmutableMap.copyOf(configKeys);
        this.configKeysSet = ImmutableSet.copyOf(this.configKeys.values());
    }

    public String getName() {
        return name;
    }
    
    public Set<ConfigKey<?>> getConfigKeys() {
        return configKeysSet;
    }
    
    public ConfigKey<?> getConfigKey(String name) {
        return configKeys.get(name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(name, configKeys);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (getClass() != obj.getClass()) return false;
        AdjunctType o = (AdjunctType) obj;
        if (!Objects.equal(name, o.getName())) return false;
        if (!Objects.equal(getConfigKeys(), o.getConfigKeys())) return false;
        return true;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(name)
                .add("configKeys", configKeys)
                .toString();
    }
    
    /**
     * Finds the config keys defined on the entity's class, statics and optionally any non-static (discouraged).
     */
    // TODO Remove duplication from EntityDynamicType
    protected static Map<String,ConfigKey<?>> findConfigKeys(Class<? extends EntityAdjunct> clazz, EntityAdjunct optionalInstance) {
        return ConfigUtilsInternal.findConfigKeys(clazz, optionalInstance);
    }

}
