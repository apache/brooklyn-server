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
package org.apache.brooklyn.config;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Supplier;

public interface ConfigValueAtContainer<TContainer,TValue> extends Supplier<TValue> {
    
    /** Returns the value for this key, or null.  Technically this returns {@link #asMaybe()} or null. */
    @Override
    @Nullable TValue get();
    /** Absent if no value can be found, typically meaning no default value, but in raw value lookups it may ignore default values. */ 
    @Nonnull Maybe<? extends TValue> asMaybe();
    /** If false, any contents of {@link #get()} will have come from {@link #getDefaultValue()}. */
    boolean isValueExplicitlySet();
    
    /** The container where the value was found (possibly an ancestor of the queried object).
     * This may be null in internal uses to indicate an anonymous query. */
    @Nullable TContainer getContainer();
    
    /** The key whose value is being held here.
     * <p>
     * This may be null when working in a context where the query key is widely known
     * to indicate that no key was defined at this container. */
    @Nullable ConfigKey<? extends TValue> getKey();
    /** The default value on the key, if available and permitted, 
     * possibly coerced or resolved in the scope of {@link #getContainer()},
     * and possibly absent e.g. in raw value lookups */
    @Nonnull Maybe<TValue> getDefaultValue();

}
