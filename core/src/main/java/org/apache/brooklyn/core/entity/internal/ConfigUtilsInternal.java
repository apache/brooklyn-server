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
package org.apache.brooklyn.core.entity.internal;

import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Internal utility methods for getting/setting config, such that it handles any deprecated names
 * defined in {@link ConfigKey#getDeprecatedNames()}.
 */
public class ConfigUtilsInternal {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtilsInternal.class);

    public static Map<?,?> setAllConfigKeys(Map<?,?> flags, Iterable<? extends ConfigKey<?>> configKeys, Configurable obj) {
        Map<?,?> unusedFlags = MutableMap.copyOf(flags);
        for (ConfigKey<?> key : configKeys) {
            ConfigValue values = getValue(unusedFlags, key);
            Maybe<Object> valueToUse = values.preferredValue();
            if (valueToUse.isPresent()) {
                setValue(obj, key, valueToUse.get());
                values.logIfDeprecatedValue(obj, key);
            }
        }
        return unusedFlags;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void setValue(Configurable obj, ConfigKey<?> key, Object val) {
        obj.config().set((ConfigKey)key, val);
    }
    
    private static ConfigValue getValue(Map<?,?> flags, ConfigKey<?> key) {
        Maybe<Object> val;
        String keyName = key.getName();
        if (flags.containsKey(keyName)) {
            val = Maybe.of(flags.get(keyName));
            flags.remove(keyName);
        } else {
            val = Maybe.absent();
        }
        Map<String, Object> deprecatedValues = new LinkedHashMap<>(key.getDeprecatedNames().size());
        for (String deprecatedName : key.getDeprecatedNames()) {
            if (flags.containsKey(deprecatedName)) {
                deprecatedValues.put(deprecatedName, flags.get(deprecatedName));
                flags.remove(deprecatedName);
            }
        }
        return new ConfigValue(val, deprecatedValues);
    }
    
    private static class ConfigValue {
        final Maybe<Object> val;
        final Map<String, Object> deprecatedValues;
        
        ConfigValue(Maybe<Object> val, Map<String, Object> deprecatedValues) {
            this.val = val;
            this.deprecatedValues = deprecatedValues;
        }
        
        Maybe<Object> preferredValue() {
            if (val.isPresent()) return val;
            return (deprecatedValues.isEmpty()) ? Maybe.absent() : Maybe.of(Iterables.get(deprecatedValues.values(), 0));
        }
        
        void logIfDeprecatedValue(Configurable obj, ConfigKey<?> key) {
            if (deprecatedValues.isEmpty()) return;
            
            if (val.isPresent()) {
                LOG.warn("Ignoring deprecated config value(s) on "+obj+" because contains value for "
                        +"'"+key.getName()+"', other deprecated name(s) present were: "+deprecatedValues.keySet());
            } else if (deprecatedValues.size() == 1) {
                LOG.warn("Using deprecated config value on "+obj+", should use '"+key.getName()+"', but used "
                        +"'"+Iterables.getOnlyElement(deprecatedValues.keySet())+"'");
            } else {
                LOG.warn("Using deprecated config value on "+obj+", should use '"+key.getName()+"', but used "
                        +"'"+Iterables.get(deprecatedValues.keySet(), 1)+"' and ignored values present for other "
                        +"deprecated name(s) "+Iterables.skip(deprecatedValues.keySet(), 1));
            }
        }
    }

    public static <T> Map<String,ConfigKey<?>> findConfigKeys(Class<? extends T> clazz, T optionalInstance) {
        try {
            Map<String,ConfigKey<?>> result = Maps.newLinkedHashMap();
            Map<String, Field> configFields = Maps.newLinkedHashMap();
            for (Field f : clazz.getFields()) {
                boolean isConfigKey = ConfigKey.class.isAssignableFrom(f.getType());
                if (!isConfigKey) {
                    if (!HasConfigKey.class.isAssignableFrom(f.getType())) {
                        // neither ConfigKey nor HasConfigKey
                        continue;
                    }
                }
                if (!Modifier.isStatic(f.getModifiers())) {
                    // require it to be static or we have an instance
                    LOG.warn("Discouraged use of non-static config key "+f+" defined in " + (optionalInstance!=null ? optionalInstance : clazz));
                    if (optionalInstance==null) continue;
                }
                ConfigKey<?> k = isConfigKey ? (ConfigKey<?>) f.get(optionalInstance) :
                        ((HasConfigKey<?>)f.get(optionalInstance)).getConfigKey();

                Field alternativeField = configFields.get(k.getName());
                // Allow overriding config keys (e.g. to set default values) when there is an assignable-from relationship between classes
                Field definitiveField = alternativeField != null ? Reflections.inferSubbestField(alternativeField, f) : f;
                boolean skip = false;
                if (definitiveField != f) {
                    // If they refer to the _same_ instance, just keep the one we already have
                    if (alternativeField.get(optionalInstance) == f.get(optionalInstance)) skip = true;
                }
                if (skip) {
                    //nothing
                } else if (definitiveField == f) {
                    result.put(k.getName(), k);
                    configFields.put(k.getName(), f);
                } else if (definitiveField != null) {
                    if (LOG.isDebugEnabled()) LOG.debug("multiple definitions for config key {} on {}; preferring that in sub-class: {} to {}", new Object[] {
                            k.getName(), optionalInstance!=null ? optionalInstance : clazz, alternativeField, f});
                } else if (definitiveField == null) {
                    LOG.warn("multiple definitions for config key {} on {}; preferring {} to {}", new Object[] {
                            k.getName(), optionalInstance!=null ? optionalInstance : clazz, alternativeField, f});
                }
            }

            return result;
        } catch (IllegalAccessException e) {
            throw Exceptions.propagate(e);
        }
    }

}
