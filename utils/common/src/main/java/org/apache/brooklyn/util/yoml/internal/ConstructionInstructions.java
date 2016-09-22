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
package org.apache.brooklyn.util.yoml.internal;

import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/** Utilities for working with {@link ConstructionInstruction} */
public class ConstructionInstructions {

    private static final Logger log = LoggerFactory.getLogger(ConstructionInstructions.class);
    
    public static class Factory {
        
        /** Returns a new construction instruction which creates an instance of the given type,
         * preferring the optional instruction but enforcing the given type constraint,
         * instructing to create using a no-arg constructor if this is the outermost instruction,
         * and if an inner instruction not putting any constraints on the arguments.
         * see {@link #newUsingConstructorWithArgs(Class, List, ConstructionInstruction)}. */
        public static ConstructionInstruction newDefault(Class<?> type, ConstructionInstruction optionalOuter) {
            if (optionalOuter!=null) {
                if (optionalOuter instanceof WrappingConstructionInstruction) {
                    return newUsingConstructorWithArgs(type, null, optionalOuter);
                } else {
                    log.warn("Ignoring nested construction instruction which is not a wrapping instruction: "+optionalOuter+" for "+type);
                }
            }
            return newUsingConstructorWithArgs(type, null, null);
        }

        /** Returns a new construction instruction which creates an instance of the given type with the given args,
         * preferring the optional instruction but enforcing the given type constraint and exposing the given args for it to inherit,
         * and if there is no outer instruction it creates from a constructor taking the given args
         * and merging with any inner instruction's args using the given strategy 
         * (if args are null here, the merge strategies should prefer the inner args,
         * and if all args everywhere are null, using the no-arg constuctor, as per {@link #newDefault(Class, ConstructionInstruction)}) */
        public static ConstructionInstruction newUsingConstructorWithArgsAndArgsListMergeStrategy(
                Class<?> type, @Nullable List<?> args, ConstructionInstruction optionalOuter, ObjectMergeStrategy argsListMergeStrategy) {
            return new BasicConstructionWithArgsInstruction(type, args, (BasicConstructionWithArgsInstruction)optionalOuter,
                argsListMergeStrategy);
        }

        /** As {@link #newUsingConstructorWithArgsAndArgsListMergeStrategy(Class, List, ConstructionInstruction, ObjectMergeStrategy)}
         * but using the default argument list strategy {@link ArgsListMergeStrategy}
         * (which asserts arguments lists are the same size and fails if they are different, ignoring any null lists)
         * configured to merge individual arguments according to the given provided strategy */
        public static ConstructionInstruction newUsingConstructorWithArgsAndArgumentMergeStrategy(
                Class<?> type, @Nullable List<?> args, ConstructionInstruction optionalOuter, ObjectMergeStrategy argumentMergeStrategy) {
            return new BasicConstructionWithArgsInstruction(type, args, (WrappingConstructionInstruction)optionalOuter,
                new ArgsListMergeStrategy(argumentMergeStrategy));
        }

        /** As {@link #newUsingConstructorWithArgsAndArgumentMergeStrategy(Class, List, ConstructionInstruction, ObjectMergeStrategy)}
         * but using {@link MapMergeStrategy} to merge arguments, ie:
         * argument lists are required to be the same length or at least one null;
         * where an argument in the list is a map they are deeply merged, and otherwise the othermost is preferred */
        public static ConstructionInstruction newUsingConstructorWithArgs(
                Class<?> type, @Nullable List<?> args, ConstructionInstruction optionalOuter) {
            return newUsingConstructorWithArgsAndArgumentMergeStrategy(type, args, optionalOuter,
                MapMergeStrategy.DEEP_MERGE);
        }

    }
    
    public interface ObjectMergeStrategy {
        public Object merge(Object obj1, Object obj2);
    }
    
    /** Merge arg lists, as follows:
     * if either list is null, take the other;
     * otherwise require them to be the same length.
     * <p>
     * If any arg is not a list, this throws. The return type is guaranteed to be null,
     * or a list the same size as the (at least one) non-null list argument.
     * <p>
     * For each entry, it applies the given deep merge strategy.
     */
    public static class ArgsListMergeStrategy implements ObjectMergeStrategy {
        final ObjectMergeStrategy strategyForEachArgument;
     
        public ArgsListMergeStrategy(ObjectMergeStrategy strategyForEachArgument) {
            this.strategyForEachArgument = Preconditions.checkNotNull(strategyForEachArgument);
        }

        public Object merge(Object olderArgs, Object args) {
            if (!(olderArgs==null || olderArgs instanceof List) || !(args==null || args instanceof List))
                throw new IllegalArgumentException("Only merges lists, not "+olderArgs+" and "+args);
            return mergeLists((List<?>)olderArgs, (List<?>)args);
        }
        
        public List<?> mergeLists(List<?> olderArgs, List<?> args) {
            if (olderArgs==null) return args;
            if (args==null) return olderArgs;
            
            List<Object> newArgs;
            if (olderArgs.size() != args.size()) 
                throw new IllegalStateException("Incompatible arguments, sizes "+olderArgs.size()+" and "+args.size()+": "+olderArgs+" and "+args);
            // merge
            newArgs = MutableList.of();
            Iterator<?> i1 = olderArgs.iterator();
            Iterator<?> i2 = args.iterator();
            while (i2.hasNext()) {
                Object o1 = i1.next();
                Object o2 = i2.next();
                newArgs.add(strategyForEachArgument.merge(o1,  o2));
            }
            return newArgs;
        }
    }
    
    public static class AlwaysPreferLatter implements ObjectMergeStrategy {
        @Override
        public Object merge(Object obj1, Object obj2) {
            return obj2;
        }
    }
    
    public static class MapMergeStrategy implements ObjectMergeStrategy {

        public static ObjectMergeStrategy DEEP_MERGE = new MapMergeStrategy();
        static { ((MapMergeStrategy)DEEP_MERGE).setStrategyForMapEntries(DEEP_MERGE); }
        
        public MapMergeStrategy() {}

        ObjectMergeStrategy strategyForMapEntries = new AlwaysPreferLatter();

        public void setStrategyForMapEntries(ObjectMergeStrategy strategyForMapEntries) {
            this.strategyForMapEntries = strategyForMapEntries;
        }
        
        @Override
        public Object merge(Object o1, Object o2) {
            if (applies(o1, o2)) {
                return mergeMapsDeep((Map<?,?>)o1, (Map<?,?>)o2);
            }
            // prefer o2 even if null, as a way of overwriting a map in its entirety
            return o2;
        }

        public boolean applies(Object o1, Object o2) {
            return (o1 instanceof Map) && (o2 instanceof Map);
        }
        
        public Map<Object,Object> mergeMapsDeep(Map<?, ?> o1, Map<?, ?> o2) {
            MutableMap<Object,Object> result = MutableMap.<Object,Object>copyOf(o1);
            if (o2!=null) {
                for (Map.Entry<?,?> e2: o2.entrySet()) {
                    Object v2 = e2.getValue();
                    if (result.containsKey(e2.getKey())) {
                        v2 = strategyForMapEntries.merge(result.get(e2.getKey()), v2);
                    }
                    result.put(e2.getKey(), v2);
                }
            }
            return result;
        }
    }
    
    /** interface for a construction instruction which may be invoked by one it wraps */
    public interface WrappingConstructionInstruction extends ConstructionInstruction {
        /** constructors list has next-outermost first */
        public Maybe<Object> create(Class<?> typeConstraintSoFar, @Nullable List<ConstructionInstruction> innerConstructors);
    }

    /** see {@link Factory#newUsingConstructorWithArgsAndArgsListMergeStrategy(Class, List, ConstructionInstruction, ObjectMergeStrategy)} 
     * and {@link Factory#newUsingConstructorWithArgsAndArgumentMergeStrategy(Class, List, ConstructionInstruction, ObjectMergeStrategy)} */
    public static abstract class AbstractConstructionWithArgsInstruction implements WrappingConstructionInstruction {
        private final Class<?> type;
        private final List<?> args;
        private final WrappingConstructionInstruction outerInstruction;
        
        protected AbstractConstructionWithArgsInstruction(Class<?> type, List<?> args, @Nullable WrappingConstructionInstruction outerInstruction) {
            this.type = type;
            this.args = args;
            this.outerInstruction = outerInstruction;
        }
        
        @Override public Class<?> getType() { return type; }
        @Override public List<?> getArgs() { return args; }
        @Override public ConstructionInstruction getOuterInstruction() { return outerInstruction; }
        
        @Override
        public Maybe<Object> create() {
            return create(null, null);
        }
        
        @Override
        public Maybe<Object> create(Class<?> typeConstraintSoFar, List<ConstructionInstruction> constructorsSoFarOutermostFirst) {
            if (typeConstraintSoFar==null) typeConstraintSoFar = type;
            if (type!=null) {
                if (typeConstraintSoFar==null || typeConstraintSoFar.isAssignableFrom(type)) typeConstraintSoFar = type;
                else if (type.isAssignableFrom(typeConstraintSoFar)) { /* fine */ }
                else {
                    throw new IllegalStateException("Incompatible expected types "+typeConstraintSoFar+" and "+type);
                }
            }

            if (outerInstruction!=null) {
                if (constructorsSoFarOutermostFirst==null) constructorsSoFarOutermostFirst = MutableList.of();
                else constructorsSoFarOutermostFirst = MutableList.copyOf(constructorsSoFarOutermostFirst);
                
                constructorsSoFarOutermostFirst.add(0, this);
                return outerInstruction.create(typeConstraintSoFar, constructorsSoFarOutermostFirst);
            }
            
            return createWhenNoOuter(typeConstraintSoFar, constructorsSoFarOutermostFirst);
        }

        protected Maybe<Object> createWhenNoOuter(Class<?> typeConstraintSoFar, List<ConstructionInstruction> constructorsSoFar) {
            List<?> combinedArgs = combineArguments(constructorsSoFar);
            
            if (typeConstraintSoFar==null) throw new IllegalStateException("No type information available");
            if ((typeConstraintSoFar.getModifiers() & (Modifier.ABSTRACT | Modifier.INTERFACE)) != 0) 
                throw new IllegalStateException("Insufficient type information: expected "+type+" is not directly instantiable");
            return Reflections.invokeConstructorFromArgsIncludingPrivate(typeConstraintSoFar, combinedArgs==null ? new Object[0] : combinedArgs.toArray());
        }

        protected abstract List<?> combineArguments(@Nullable List<ConstructionInstruction> constructorsSoFar);
    }

    /** see {@link Factory#newDefault(Class, ConstructionInstruction)} and other methods in that factory class */
    protected static class BasicConstructionWithArgsInstruction extends AbstractConstructionWithArgsInstruction {
        private ObjectMergeStrategy argsListMergeStrategy;

        protected BasicConstructionWithArgsInstruction(Class<?> type, List<?> args, @Nullable WrappingConstructionInstruction outerInstruction,
                ObjectMergeStrategy argsListMergeStrategy) {
            super(type, args, outerInstruction);
            this.argsListMergeStrategy = argsListMergeStrategy;
        }

        protected List<?> combineArguments(@Nullable List<ConstructionInstruction> constructorsSoFarOutermostFirst) {
            List<?> combinedArgs = null;
            List<ConstructionInstruction> constructorsInnermostFirst = MutableList.copyOf(constructorsSoFarOutermostFirst);
            Collections.reverse(constructorsInnermostFirst);
            
            for (ConstructionInstruction i: constructorsInnermostFirst) {
                combinedArgs = (List<?>) argsListMergeStrategy.merge(combinedArgs, i.getArgs());
            }
            combinedArgs = (List<?>) argsListMergeStrategy.merge(combinedArgs, getArgs());
            return combinedArgs;
        }
    }
    
    
}
