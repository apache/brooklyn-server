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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;

public abstract class AbstractConfigurationSupportInternal implements BrooklynObjectInternal.ConfigurationSupportInternal {

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
        if (key instanceof StructuredConfigKey || key instanceof SubElementConfigKey) {
            return getNonBlockingResolvingStructuredKey(key);
        } else {
            return getNonBlockingResolvingSimple(key);
        }
    }

    /**
     * For resolving a {@link StructuredConfigKey}, such as a {@link MapConfigKey}. Here we need to 
     * execute the custom logic, as is done by {@link #get(ConfigKey)}, but non-blocking!
     */
    protected <T> Maybe<T> getNonBlockingResolvingStructuredKey(final ConfigKey<T> key) {
        // TODO This is a poor implementation. We risk timing out when it's just doing its
        // normal work (e.g. because job's thread was starved), rather than when it's truly 
        // blocked. Really we'd need to dig into the implementation of get(key), so that the 
        // underlying work can be configured with a timeout, for when it finally calls 
        // ValueResolver.

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

        Task<T> t = getContext().submit(Tasks.<T>builder().body(job)
                .displayName("Resolving dependent value")
                .description("Resolving "+key.getName())
                .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                .build());
        try {
            T result = t.get(ValueResolver.NON_BLOCKING_WAIT);
            return Maybe.of(result);
        } catch (TimeoutException e) {
            t.cancel(true);
            return Maybe.<T>absent();
        } catch (ExecutionException e) {
            LOG.debug("Problem resolving "+key.getName()+", returning <absent>", e);
            return Maybe.<T>absent();
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
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
        final Object marker = new Object();
        // Give tasks a short grace period to resolve.
        Object resolved = Tasks.resolving(unresolved)
                .as(Object.class)
                .defaultValue(marker)
                .immediately(true)
                .deep(true)
                .context(getContext())
                .get();
        return (resolved != marker)
                ? TypeCoercions.tryCoerce(resolved, key.getTypeToken())
                        : Maybe.<T>absent();
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
