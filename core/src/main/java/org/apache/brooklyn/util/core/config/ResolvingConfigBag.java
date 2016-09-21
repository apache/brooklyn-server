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
package org.apache.brooklyn.util.core.config;

import java.util.Map;

import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * As for {@link ConfigBag}, but resolves values that are of type {@link DeferredSupplier}.
 */
@Beta
public class ResolvingConfigBag extends ConfigBag {

    // Relies on various getters all delegating to a few common methods.

    private final ManagementContext mgmt;
    protected final ConfigBag parentBag;
    
    protected transient volatile Function<Object, Object> transformer;


    @Beta
    public static ConfigBag newInstanceExtending(ManagementContext mgmt, ConfigBag parentBag) {
        return new ResolvingConfigBag(mgmt, parentBag);
    }
    
    public ResolvingConfigBag(ManagementContext mgmt, ConfigBag parentBag) {
        this.mgmt = mgmt;
        this.parentBag = parentBag;
        copy(parentBag);
    }

    protected Function<Object, Object> getTransformer() {
        if (transformer == null) {
            transformer = new Function<Object, Object>() {
                @Override public Object apply(Object input) {
                    if (input instanceof DeferredSupplier<?>) {
                        try {
                            ExecutionContext exec = mgmt.getServerExecutionContext();
                            return Tasks.resolveValue(input, Object.class, exec);
                        } catch (Exception e) {
                            throw Exceptions.propagate(e);
                        }
                    }
                    return input;
                }
            };
        }
        return transformer;
    }
    
    @Override
    public void markUsed(String key) {
        super.markUsed(key);
        if (parentBag!=null)
            parentBag.markUsed(key);
    }

    // If copying from another {@link ResolvingConfigBag}, avoid resolving the config while doing 
    // that copy.
    @Override
    protected ConfigBag copyWhileSynched(ConfigBag otherRaw) {
        if (otherRaw instanceof ResolvingConfigBag) {
            ResolvingConfigBag other = (ResolvingConfigBag) otherRaw;
            if (isSealed()) 
                throw new IllegalStateException("Cannot copy "+other+" to "+this+": this config bag has been sealed and is now immutable.");
            putAll(other.getAllConfigUntransformed());
            markAll(Sets.difference(other.getAllConfigUntransformed().keySet(), other.getUnusedConfigUntransformed().keySet()));
            setDescription(other.getDescription());
            return this;
        } else {
            return super.copyWhileSynched(otherRaw);
        }
    }

    protected Map<String,Object> getAllConfigUntransformed() {
        return super.getAllConfig();
    }

    protected Map<String,Object> getUnusedConfigUntransformed() {
        return super.getUnusedConfig();
    }

    @SuppressWarnings("unchecked")
    protected <T> T get(ConfigKey<T> key, boolean markUsed) {
        return (T) getTransformer().apply(super.get(key, markUsed));
    }

    @Override
    public Map<String,Object> getAllConfig() {
        // Lazily transform copy of map
        return Maps.transformValues(super.getAllConfig(), getTransformer());
    }

    @Override
    public Map<ConfigKey<?>, ?> getAllConfigAsConfigKeyMap() {
        // Lazily transform copy of map
        return Maps.transformValues(super.getAllConfigAsConfigKeyMap(), getTransformer());
    }

    @Override
    public Map<String,Object> getUnusedConfig() {
        // Lazily transform copy of map
        return Maps.transformValues(super.getUnusedConfig(), getTransformer());
    }

    @Override
    public Map<String,Object> getUnusedConfigMutable() {
        throw new UnsupportedOperationException();
    }

    protected synchronized Maybe<Object> getStringKeyMaybe(String key, boolean markUsed) {
        Maybe<Object> result = super.getStringKeyMaybe(key, markUsed);
        return (result.isPresent()) ? Maybe.of(getTransformer().apply(result.get())) : result;
    }

    @Override
    public Map<String,Object> getAllConfigMutable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getAllConfigRaw() {
        return getAllConfigMutable();
    }
}
