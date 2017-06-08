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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.objs.BrooklynType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class BrooklynTypeSnapshot implements BrooklynType {
    private static final long serialVersionUID = 4670930188951106009L;
    
    private static final Logger LOG = LoggerFactory.getLogger(BrooklynTypeSnapshot.class);

    private final String name;
    private transient volatile String simpleName;
    private final Map<String, ConfigKey<?>> configKeys;
    private final Map<String, ConfigKey<?>> configKeysByDeprecatedName;
    
    private final Set<ConfigKey<?>> configKeysSet;

    protected BrooklynTypeSnapshot(String name, Map<String, ConfigKey<?>> configKeys) {
        this.name = name;
        this.configKeys = ImmutableMap.copyOf(configKeys);
        this.configKeysSet = ImmutableSet.copyOf(this.configKeys.values());
        
        this.configKeysByDeprecatedName = new LinkedHashMap<>();
        for (ConfigKey<?> key : configKeysSet) {
            for (String deprecatedName : key.getDeprecatedNames()) {
                if (configKeys.containsKey(deprecatedName)) {
                    LOG.warn("Conflicting config key name '"+deprecatedName+"' used in "+configKeys.get(deprecatedName)+" and as deprecated name of "+key+"; will prefer "+key+" but may cause problems");
                } else if (configKeysByDeprecatedName.containsKey(deprecatedName)) {
                    LOG.warn("Conflicting config key name '"+deprecatedName+"' used as deprecated name in both "+configKeysByDeprecatedName.get(deprecatedName)+" and "+key+"; may cause problems");
                } else {
                    configKeysByDeprecatedName.put(deprecatedName, key);
                }
            }
        }
    }

    @Override
    public String getName() {
        return name;
    }
    
    private String toSimpleName(String name) {
        String simpleName = name.substring(name.lastIndexOf(".")+1);
        if (Strings.isBlank(simpleName)) simpleName = name.trim();
        return Strings.makeValidFilename(simpleName);
    }

    @Override
    public String getSimpleName() {
        String sn = simpleName;
        if (sn==null) {
            sn = toSimpleName(getName());
            simpleName = sn;
        }
        return sn;
    }
    
    @Override
    public Set<ConfigKey<?>> getConfigKeys() {
        return configKeysSet;
    }
    
    @Override
    public ConfigKey<?> getConfigKey(String name) {
        ConfigKey<?> result = configKeys.get(name);
        if (result == null) result = configKeysByDeprecatedName.get(name);
        return result;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(name, configKeys);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BrooklynTypeSnapshot)) return false;
        BrooklynTypeSnapshot o = (BrooklynTypeSnapshot) obj;
        
        return Objects.equal(name, o.name) && Objects.equal(configKeys, o.configKeys);
    }
    
    @Override
    public String toString() {
        return toStringHelper().toString();
    }
    
    protected ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(name)
                .add("configKeys", configKeys);
    }
}
