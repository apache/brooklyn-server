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
package org.apache.brooklyn.core.config.internal;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigValueAtContainer;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Function;

public class LazyContainerAndKeyValue<TContainer,TValue> implements ConfigValueAtContainer<TContainer,TValue> {
    
    private final TContainer container;
    private final ConfigKey<TValue> key;
    private final Function<TContainer,Maybe<TValue>> evaluationFunction;
    private Maybe<TValue> resolved;
    
    public LazyContainerAndKeyValue(ConfigKey<TValue> key, TContainer container, Function<TContainer, Maybe<TValue>> evaluationFunction) {
        this.key = key;
        this.container = container;
        this.evaluationFunction = evaluationFunction;
    }

    protected synchronized Maybe<TValue> resolve() {
        if (resolved==null) { 
            resolved = evaluationFunction.apply(getContainer());
        }
        return resolved;
    }

    @Override
    public TContainer getContainer() {
        return container;
    }

    @Override
    public TValue get() {
        if (resolve().isPresent()) return resolve().get();
        return getDefaultValue();
    }
    
    @Override
    public Maybe<TValue> asMaybe() {
        if (resolve().isPresent()) return resolve();
        return getDefaultValueMaybe();
    }

    @Override
    public boolean isValueExplicitlySet() {
        return resolve().isPresent();
    }

    @Override
    public ConfigKey<TValue> getKey() {
        return key;
    }

    @Override
    public TValue getDefaultValue() {
        return getDefaultValueMaybe().orNull();
    }
    
    public Maybe<TValue> getDefaultValueMaybe() {
        if (key==null || !key.hasDefaultValue()) return Maybe.absent();
        return Maybe.of(key.getDefaultValue());
    }
}