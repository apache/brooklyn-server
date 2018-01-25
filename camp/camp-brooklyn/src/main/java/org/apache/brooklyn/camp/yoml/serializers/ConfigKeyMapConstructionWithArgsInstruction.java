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
package org.apache.brooklyn.camp.yoml.serializers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigInheritances;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigValueAtContainer;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.internal.AncestorContainerAndKeyValueIterator;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions.AbstractConstructionWithArgsInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions.WrappingConstructionInstruction;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;

/** Instruction for using a constructor which takes a single argument being a map of string,object
 * config keys. This will look at the actual keys defined on the types and traverse the construction
 * instruction hierarchy to ensure the key values are inherited correctly from supertype definitions.  */
public class ConfigKeyMapConstructionWithArgsInstruction extends AbstractConstructionWithArgsInstruction {
    protected final Map<String,ConfigKey<?>> keysByNameOrAlias;

    public ConfigKeyMapConstructionWithArgsInstruction(Class<?> type, 
            Map<String, Object> values, 
            WrappingConstructionInstruction optionalOuter,
            Map<String,ConfigKey<?>> keysByNameOrAlias) {
        super(type, MutableList.of(values), optionalOuter);
        this.keysByNameOrAlias = keysByNameOrAlias;
    }

    @Override
    protected List<?> combineArguments(final List<ConstructionInstruction> constructorsSoFarOutermostFirst) {
        Set<String> innerNames = MutableSet.of();
        for (ConstructionInstruction i: constructorsSoFarOutermostFirst) {
            innerNames.addAll(getKeyValuesAt(i).keySet());
        }

        // collect all keys, real local ones, and anonymous ones for anonymous config from parents
        
        // TODO if keys are defined at yaml-based type or type parent we don't currently have a way to get them
        // (the TypeRegistry API needs to return more than just java class for that)
        final Map<String,ConfigKey<?>> keysByName = MutableMap.of();
        
        for (ConfigKey<?> keyDeclaredHere: keysByNameOrAlias.values()) {
            keysByName.put(keyDeclaredHere.getName(), keyDeclaredHere);
        }
        
        MutableMap<ConfigKey<?>,Object> localValues = MutableMap.of();
        for (Map.Entry<String, ?> aliasAndValueHere: getKeyValuesAt(this).entrySet()) {
            ConfigKey<?> k = keysByNameOrAlias.get(aliasAndValueHere.getKey());
            if (k==null) {
                // don't think it should come here; all keys will be known
                k = anonymousKey(aliasAndValueHere.getKey());
            }
            if (!keysByName.containsKey(k.getName())) {
                keysByName.put(k.getName(), k);
            }
            if (!localValues.containsKey(k.getName())) {
                localValues.put(k, aliasAndValueHere.getValue());
            }
        }
        
        for (String innerKeyName: innerNames) {
            if (!keysByName.containsKey(innerKeyName)) {
                // parent defined a value under a key which doesn't match config keys we know
                keysByName.put(innerKeyName, anonymousKey(innerKeyName));
            }
        }
        
        Map<String,Object> result = MutableMap.of();
        for (final ConfigKey<?> k: keysByName.values()) {
            // go through all keys defined here recognising aliases, 
            // and anonymous keys for other keys at parents (ignoring aliases)
            Maybe<Object> value = localValues.containsKey(k) ? Maybe.ofAllowingNull(localValues.get(k)) : Maybe.absent();
            // don't set default values
//                Maybe<Object> defaultValue = k.hasDefaultValue() ? Maybe.ofAllowingNull((Object)k.getDefaultValue()) : Maybe.absent();
            
            Function<ConstructionInstruction, ConfigKey<Object>> keyFn = new Function<ConstructionInstruction, ConfigKey<Object>>() {
                @SuppressWarnings("unchecked")
                @Override
                public ConfigKey<Object> apply(ConstructionInstruction input) {
                    // type inheritance so pretty safe to assume outermost key declaration 
                    return (ConfigKey<Object>) keysByName.get(input);
                } 
            };
            Function<ConstructionInstruction, Maybe<Object>> lookupFn = new Function<ConstructionInstruction, Maybe<Object>>() {
                @Override
                public Maybe<Object> apply(ConstructionInstruction input) {
                    Map<String, ?> values = getKeyValuesAt(input);  // TODO allow aliases?
                    if (values.containsKey(k.getName())) return Maybe.of((Object)values.get(k.getName()));
                    return Maybe.absent();
                }
            };
            Function<Maybe<Object>, Maybe<Object>> coerceFn = Functions.identity();
            Function<ConstructionInstruction, ConstructionInstruction> parentFn = new Function<ConstructionInstruction, ConstructionInstruction>() {
                @Override
                public ConstructionInstruction apply(ConstructionInstruction input) {
                    // parent is the one *after* us in the list, *not* input.getOuterInstruction()
                    Iterator<ConstructionInstruction> ci = constructorsSoFarOutermostFirst.iterator();
                    ConstructionInstruction cc = ConfigKeyMapConstructionWithArgsInstruction.this; 
                    while (ci.hasNext()) {
                        if (input.equals(cc)) {
                            return ci.next();
                        }
                        cc = ci.next();
                    }
                    return null;
                }
            };
            Iterator<ConfigValueAtContainer<ConstructionInstruction,Object>> ancestors = new AncestorContainerAndKeyValueIterator<ConstructionInstruction,Object>(
                this, keyFn, lookupFn, coerceFn, parentFn); 
            
            ConfigInheritance inheritance = ConfigInheritances.findInheritance(k, InheritanceContext.TYPE_DEFINITION, BasicConfigInheritance.OVERWRITE);
            
            @SuppressWarnings("unchecked")
            ReferenceWithError<ConfigValueAtContainer<ConstructionInstruction, Object>> newValue = 
                ConfigInheritances.<ConstructionInstruction,Object>resolveInheriting(this, (ConfigKey<Object>)k, value, Maybe.absent(), 
                ancestors, InheritanceContext.TYPE_DEFINITION, inheritance);
            
            if (newValue.getWithError().isValueExplicitlySet()) 
                result.put(k.getName(), newValue.getWithError().get());
        }
        
        return MutableList.of(result);
    }

    protected ConfigKey<?> anonymousKey(String key) {
        return ConfigKeys.newConfigKey(Object.class, key);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, ?> getKeyValuesAt(ConstructionInstruction i) {
        if (i.getArgs()==null) return MutableMap.of();
        if (i.getArgs().size()!=1) throw new IllegalArgumentException("Wrong length of constructor params, expected one: "+i.getArgs());
        Object arg = Iterables.getOnlyElement(i.getArgs());
        if (arg==null) return MutableMap.of();
        if (!(arg instanceof Map)) throw new IllegalArgumentException("Wrong type of constructor param, expected map: "+arg);
        return (Map<String,?>)arg;
    }
}
