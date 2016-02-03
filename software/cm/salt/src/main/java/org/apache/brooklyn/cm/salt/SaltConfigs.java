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
package org.apache.brooklyn.cm.salt;

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.config.MapConfigKey.MapModifications;
import org.apache.brooklyn.core.config.SetConfigKey.SetModifications;
import org.apache.brooklyn.core.entity.EntityInternal;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

/**
 * Conveniences for configuring Brooklyn Salt entities 
 *
 * @since 0.9.0
 */
@Beta
public class SaltConfigs {

    public static void addToRunList(EntitySpec<?> entity, String...states) {
        for (String state : states) {
            entity.configure(SaltConfig.START_STATES, SetModifications.addItem(state));
        }
    }

    public static void addToRunList(EntityInternal entity, String...states) {
        for (String state : states) {
            entity.config().set(SaltConfig.START_STATES, SetModifications.addItem(state));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void addLaunchAttributes(EntitySpec<?> entity, Map<?,?> attributesMap) {
        entity.configure(SaltConfig.SALT_SSH_LAUNCH_ATTRIBUTES, MapModifications.add((Map)attributesMap));
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void addLaunchAttributes(EntityInternal entity, Map<?,?> attributesMap) {
        entity.config().set(SaltConfig.SALT_SSH_LAUNCH_ATTRIBUTES, MapModifications.add((Map)attributesMap));
    }

    public static void addToFormulas(EntitySpec<?> entity, String formulaUrl) {
        entity.configure(SaltConfig.SALT_FORMULAS, SetModifications.addItem(formulaUrl));
    }

    public static void addToFormulas(EntityInternal entity, String formulaUrl) {
        entity.config().set(SaltConfig.SALT_FORMULAS, SetModifications.addItem(formulaUrl));
    }

    public static <T> T getRequiredConfig(Entity entity, ConfigKey<T> key) {
        return Preconditions.checkNotNull(
                Preconditions.checkNotNull(entity, "Entity must be supplied").getConfig(key), 
                "Key " + key + " is required on " + entity);
    }

}
