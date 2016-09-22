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

import javax.annotation.Nullable;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigValueAtContainer;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class LazyContainerAndKeyValue<TContainer,TValue> implements ConfigValueAtContainer<TContainer,TValue> {
    
    @Nullable private final TContainer container;
    @Nullable private final ConfigKey<TValue> key;
    private final Function<TContainer,Maybe<Object>> lookupResolutionFunction;
    private final Function<Maybe<Object>,Maybe<TValue>> conversionFunction;
    private Maybe<TValue> resolved;
    
    public LazyContainerAndKeyValue(@Nullable ConfigKey<TValue> key, @Nullable TContainer container, 
            Function<TContainer, Maybe<Object>> lookupResolutionFunction,
            Function<Maybe<Object>, Maybe<TValue>> conversionFunction) {
        this.key = key;
        this.container = container;
        this.lookupResolutionFunction = Preconditions.checkNotNull(lookupResolutionFunction);
        this.conversionFunction = Preconditions.checkNotNull(conversionFunction);
    }

    protected synchronized Maybe<TValue> resolve() {
        if (resolved==null) { 
            resolved = conversionFunction.apply(
                lookupResolutionFunction.apply(getContainer()));
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
        return getDefaultValue().orNull();
    }
    
    @Override
    public Maybe<TValue> asMaybe() {
        if (resolve().isPresent()) return resolve();
        return getDefaultValue();
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
    public Maybe<TValue> getDefaultValue() {
        if (key==null || !key.hasDefaultValue()) return Maybe.absent();
        return conversionFunction.apply(Maybe.of((Object)key.getDefaultValue()));
    }
    
    @Override
    public String toString() {
        return super.toString()+"[key="+key+"; container="+container+"]";
    }

}
