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
package org.apache.brooklyn.core.mgmt.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * Replacement for use of Groovy's ObservableList; for internal purposes only.
 */
class ObservableSet<T> {

    private final Set<T> delegate;
    
    private final Set<CollectionChangeListener<? super T>> listeners = Sets.newCopyOnWriteArraySet();
    
    public ObservableSet() {
        this(new LinkedHashSet<T>());
    }
    
    public ObservableSet(Set<T> delegate) {
        this.delegate = checkNotNull(delegate, "delegate");
    }
    
    public void addListener(CollectionChangeListener<? super T> listener) {
        listeners.add(checkNotNull(listener, "listener"));
    }
    
    public void removeListener(CollectionChangeListener<? super T> listener) {
        listeners.remove(checkNotNull(listener, "listener"));
    }
    
    public boolean add(T val) {
        boolean changed = delegate.add(val);
        if (changed) {
            for (CollectionChangeListener<? super T> listener : listeners) {
                listener.onItemAdded(val);
            }
        }
        return changed;
    }
    
    public boolean remove(T val) {
        boolean changed = delegate.remove(val);
        if (changed) {
            for (CollectionChangeListener<? super T> listener : listeners) {
                listener.onItemRemoved(val);
            }
        }
        return changed;
    }
    
    public boolean contains(T val) {
        return delegate.contains(val);
    }
}
