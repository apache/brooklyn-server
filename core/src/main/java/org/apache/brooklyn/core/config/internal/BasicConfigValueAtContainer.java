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

public class BasicConfigValueAtContainer<TContainer,TValue> implements ConfigValueAtContainer<TContainer,TValue> {
    
    TContainer container = null;
    Maybe<TValue> value = Maybe.absent();
    boolean valueWasExplicitlySet;
    ConfigKey<TValue> key = null;
    
    @Override public TContainer getContainer() { return container; }
    @Override public TValue get() { return value.orNull(); }
    @Override public Maybe<TValue> asMaybe() { return value; }
    @Override public boolean isValueExplicitlySet() { return valueWasExplicitlySet; }
    @Override public ConfigKey<TValue> getKey() { return key; }
    @Override public TValue getDefaultValue() { return getDefaultValueMaybe().orNull(); }
    
    public void setContainer(TContainer container) {
        this.container = container;
    }
    public void setValue(Maybe<TValue> value) {
        this.value = value;
    }
    public void setValueWasExplicitlySet(boolean valueWasExplicitlySet) {
        this.valueWasExplicitlySet = valueWasExplicitlySet;
    }
    public void setKey(ConfigKey<TValue> key) {
        this.key = key;
    }
    
    public Maybe<TValue> getDefaultValueMaybe() { return key!=null && key.hasDefaultValue() ? Maybe.ofAllowingNull(key.getDefaultValue()) : Maybe.<TValue>absent(); }
    
}
