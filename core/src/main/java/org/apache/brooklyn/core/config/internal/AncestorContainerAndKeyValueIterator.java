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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigValueAtContainer;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Function;

public class AncestorContainerAndKeyValueIterator<TContainer,TValue> implements Iterator<ConfigValueAtContainer<TContainer,TValue>> {
    private TContainer lastContainer;
    private final Function<TContainer, ConfigKey<TValue>> keyFindingFunction; 
    private final Function<TContainer, Maybe<TValue>> localEvaluationFunction; 
    private final Function<TContainer, TContainer> parentFunction;
    
    public AncestorContainerAndKeyValueIterator(TContainer childContainer, 
            Function<TContainer, ConfigKey<TValue>> keyFindingFunction, 
            Function<TContainer, Maybe<TValue>> localEvaluationFunction, 
            Function<TContainer, TContainer> parentFunction) {
        this.lastContainer = childContainer;
        this.keyFindingFunction = keyFindingFunction;
        this.localEvaluationFunction = localEvaluationFunction;
        this.parentFunction = parentFunction;
    }

    @Override
    public boolean hasNext() {
        return parentFunction.apply(lastContainer)!=null;
    }
    
    @Override
    public ConfigValueAtContainer<TContainer,TValue> next() {
        TContainer nextContainer = parentFunction.apply(lastContainer);
        if (nextContainer==null) throw new NoSuchElementException("Cannot search ancestors further than "+lastContainer);
        lastContainer = nextContainer;
        return new LazyContainerAndKeyValue<TContainer,TValue>(keyFindingFunction.apply(lastContainer), lastContainer, localEvaluationFunction);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This iterator does not support removal");
    }
}
