/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.core.objs;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.config.ConfigMap.ConfigMapWithInheritance;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.config.StructuredConfigKey;
import org.apache.brooklyn.core.config.SubElementConfigKey;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.ImmediateSupplier.ImmediateUnsupportedException;
import org.apache.brooklyn.util.core.task.ImmediateSupplier.ImmediateValueNotAvailableException;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;

public abstract class AbstractConfigurationSupportInternal implements BrooklynObjectInternal.ConfigurationSupportInternal {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(AbstractConfigurationSupportInternal.class);

    @Override
    public <T> T get(HasConfigKey<T> key) {
        return get(key.getConfigKey());
    }

    @Override
    public Maybe<Object> getLocalRaw(HasConfigKey<?> key) {
        return getLocalRaw(key.getConfigKey());
    }

    @Override
    public Maybe<Object> getRaw(HasConfigKey<?> key) {
        return getRaw(key.getConfigKey());
    }

    @Override
    public <T> Maybe<T> getNonBlocking(HasConfigKey<T> key) {
        return getNonBlocking(key.getConfigKey());
    }

    @Override
    public <T> Maybe<T> getNonBlocking(final ConfigKey<T> key) {
        try {
            if (key instanceof StructuredConfigKey || key instanceof SubElementConfigKey) {
                return getNonBlockingResolvingStructuredKey(key);
            } else {
                return getNonBlockingResolvingSimple(key);
            }
        } catch (ImmediateValueNotAvailableException e) {
            return Maybe.absent(e);
        } catch (ImmediateUnsupportedException e) {
            return Maybe.absent(e);
        }
    }

    /**
     * For resolving a {@link StructuredConfigKey}, such as a {@link MapConfigKey}. Here we need to 
     * execute the custom logic, as is done by {@link #get(ConfigKey)}, but non-blocking!
     */
    protected <T> Maybe<T> getNonBlockingResolvingStructuredKey(final ConfigKey<T> key) {
        Callable<T> job = new Callable<T>() {
            @Override
            public T call() {
                try {
                    return get(key);
                } catch (RuntimeInterruptedException e) {
                    throw Exceptions.propagate(e); // expected; return gracefully
                }
            }
        };

        // TODO can we remove the DST ?  this is structured so maybe not
        Task<T> t = Tasks.<T>builder().body(job)
                .displayName("Resolving config "+key.getName())
                .description("Internal non-blocking structured key resolution")
                .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                .build();
        try {
            return getContext().getImmediately(t);
        } catch (ImmediateUnsupportedException e) {
            return Maybe.absent();
        }
    }

    /**
     * For resolving a "simple" config key - i.e. where there's not custom logic inside a 
     * {@link StructuredConfigKey} such as a {@link MapConfigKey}. For those, we'd need to do the
     * same as is in {@link #get(ConfigKey)}, but non-blocking! 
     * See {@link #getNonBlockingResolvingStructuredKey(ConfigKey)}.
     */
    protected <T> Maybe<T> getNonBlockingResolvingSimple(ConfigKey<T> key) {
        // TODO See AbstractConfigMapImpl.getConfigImpl, for how it looks up the "container" of the
        // key, so that it gets the right context entity etc.

        // getRaw returns Maybe(val) if the key was explicitly set (where val can be null)
        // or Absent if the config key was unset.
        Object unresolved = getRaw(key).or(key.getDefaultValue());
        // TODO add description that we are evaluating this config key to be used if the code below submits futher tasks
        // and look at other uses of "description" method
        // and make sure it is marked transient
        Maybe<Object> resolved = Tasks.resolving(unresolved)
                .as(Object.class)
                .immediately(true)
                .deep(true)
                .context(getContext())
                .getMaybe();
        if (resolved.isAbsent()) return Maybe.Absent.<T>castAbsent(resolved);
        
        // likely we don't need this coercion if we set  as(key.getType())  above, 
        // but that needs confirmation and quite a lot of testing 
        return TypeCoercions.tryCoerce(resolved.get(), key.getTypeToken());
    }

    @Override
    public <T> T set(HasConfigKey<T> key, Task<T> val) {
        return set(key.getConfigKey(), val);
    }

    @Override
    public <T> T set(HasConfigKey<T> key, T val) {
        return set(key.getConfigKey(), val);
    }

    protected abstract AbstractConfigMapImpl<? extends BrooklynObject> getConfigsInternal();
    protected abstract <T> void assertValid(ConfigKey<T> key, T val);
    protected abstract BrooklynObject getContainer();
    protected abstract <T> void onConfigChanging(ConfigKey<T> key, Object val);
    protected abstract <T> void onConfigChanged(ConfigKey<T> key, Object val);

    @Override
    public <T> T get(ConfigKey<T> key) {
        return getConfigsInternal().getConfig(key);
    }

    @SuppressWarnings("unchecked")
    protected <T> T setConfigInternal(ConfigKey<T> key, Object val) {
        onConfigChanging(key, val);
        T result = (T) getConfigsInternal().setConfig(key, val);
        onConfigChanged(key, val);
        return result;
    }

    @Override
    public <T> T set(ConfigKey<T> key, T val) {
        assertValid(key, val);
        return setConfigInternal(key, val);
    }

    @Override
    public <T> T set(ConfigKey<T> key, Task<T> val) {
        return setConfigInternal(key, val);
    }

    @Override
    public ConfigBag getLocalBag() {
        return ConfigBag.newInstance(getConfigsInternal().getAllConfigLocalRaw());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Maybe<Object> getRaw(ConfigKey<?> key) {
        return (Maybe<Object>) getConfigsInternal().getConfigInheritedRaw(key).getWithoutError().asMaybe();
    }

    @Override
    public Maybe<Object> getLocalRaw(ConfigKey<?> key) {
        return getConfigsInternal().getConfigLocalRaw(key);
    }

    @Override
    public void putAll(Map<?, ?> vals) {
        getConfigsInternal().putAll(vals);
    }

    @Override @Deprecated
    public void set(Map<?, ?> vals) {
        putAll(vals);
    }

    @Override
    public void removeKey(String key) {
        getConfigsInternal().removeKey(key);
    }

    @Override
    public void removeKey(ConfigKey<?> key) {
        getConfigsInternal().removeKey(key);
    }

    @Override
    public void removeAllLocalConfig() {
        getConfigsInternal().setLocalConfig(MutableMap.<ConfigKey<?>,Object>of());
    }

    @Override @Deprecated
    public Set<ConfigKey<?>> findKeys(Predicate<? super ConfigKey<?>> filter) {
        return getConfigsInternal().findKeys(filter);
    }

    @Override
    public Set<ConfigKey<?>> findKeysDeclared(Predicate<? super ConfigKey<?>> filter) {
        return getConfigsInternal().findKeysDeclared(filter);
    }

    @Override
    public Set<ConfigKey<?>> findKeysPresent(Predicate<? super ConfigKey<?>> filter) {
        return getConfigsInternal().findKeysPresent(filter);
    }

    @Override
    public ConfigMapWithInheritance<? extends BrooklynObject> getInternalConfigMap() {
        return getConfigsInternal();
    }

    @Override
    public Map<ConfigKey<?>,Object> getAllLocalRaw() {
        return getConfigsInternal().getAllConfigLocalRaw();
    }

    @SuppressWarnings("deprecation")
    @Override
    // see super; we aspire to depreate this due to poor treatment of inheritance
    public ConfigBag getBag() {
        return getConfigsInternal().getAllConfigBag();
    }

    /**
     * @return An execution context for use by {@link #getNonBlocking(ConfigKey)}
     */
    @Nullable
    protected abstract ExecutionContext getContext();
}
