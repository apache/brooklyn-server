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

import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.api.objs.Identifiable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.config.ConfigMap;
import org.apache.brooklyn.core.mgmt.HasBrooklynManagementContext;
import org.apache.brooklyn.core.mgmt.ManagementContextInjectable;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Identifiers;

/**
 * A parent class for ancilliary objects that do not require the full heavy lifting of {@link AbstractBrooklynObject}
 * or similar, but wish to use {@link ConfigKey} and {@link Configurable} in their construction, via the
 * {@code $brooklyn:object} method of the CAMP DSL.
 * <p>
 * Type coercion of values will occur when the {@link ConfigMap} is accessed, but resolving of {@link Task tasks} and other
 * deferred operations are assumed to have occurred prior to calling {@link org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport#set(ConfigKey, Object)} i.e. at
 * object construction.
 */
public class BasicConfigurableObject implements Configurable, Identifiable, ManagementContextInjectable, HasBrooklynManagementContext {

    @SetFromFlag("id")
    private String id = Identifiers.makeRandomId(8);

    private volatile ManagementContext managementContext;
    private final BasicConfigurationSupport config;

    public BasicConfigurableObject() {
        config = new BasicConfigurationSupport();
    }

    @Override
    public void setManagementContext(ManagementContext managementContext) {
        this.managementContext = managementContext;
    }

    @Override
    public ManagementContext getBrooklynManagementContext() {
        return managementContext;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ConfigurationSupport config() {
        return config;
    }

    @Override
    public <T> T getConfig(ConfigKey<T> key) {
        return config().get(key);
    }

    public static class BasicConfigurationSupport implements ConfigurationSupport {
        private final ConfigBag config = ConfigBag.newInstance();

        @Override
        public <T> T get(ConfigKey<T> key) {
            return config.get(key);
        }

        @Override
        public <T> Maybe<T> getMaybe(ConfigKey<T> ck) { return config.getMaybe(ck); }

        @Override
        public <T> T get(HasConfigKey<T> key) {
            return get(key.getConfigKey());
        }

        @Override
        public <T> T set(ConfigKey<T> key, T val) {
            T old = config.get(key);
            config.configure(key, val);
            return old;
        }

        @Override
        public <T> T set(HasConfigKey<T> key, T val) {
            return set(key.getConfigKey(), val);
        }

        @Override
        public <T> T set(ConfigKey<T> key, Task<T> val) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T set(HasConfigKey<T> key, Task<T> val) {
            return set(key.getConfigKey(), val);
        }

        @Override
        public Set<ConfigKey<?>> findKeysDeclared(Predicate<? super ConfigKey<?>> filter) {
            return MutableSet.copyOf(Iterables.filter(config.getAllConfigAsConfigKeyMap().keySet(), filter));
        }

        @Override
        public Set<ConfigKey<?>> findKeysPresent(Predicate<? super ConfigKey<?>> filter) {
            return findKeysDeclared(filter);
        }

    }
}
