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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;

public class ConstructionInstructions {

    public static class Factory {
        
        /** Returns a new construction instruction which creates an instance of the given type,
         * preferring the optional instruction but enforcing the given type constraint,
         * and if there is no outer instruction it creates from a no-arg constructor */
        public static ConstructionInstruction newDefault(Class<?> type, ConstructionInstruction optionalOuter) {
            if (optionalOuter!=null) {
                if (optionalOuter instanceof ConstructorWithArgsInstruction)
                    return newUsingConstructorWithArgs(type, null, optionalOuter);
            }
            return newUsingConstructorWithArgs(type, null, null);
        }

        /** Returns a new construction instruction which creates an instance of the given type with the given args,
         * preferring the optional instruction but enforcing the given type constraint and passing the given args for it to inherit,
         * and if there is no outer instruction it creates from a constructor taking the given args 
         * (or no-arg if null, making it the same as {@link #newDefault(Class, ConstructionInstruction)}) */
        public static ConstructionInstruction newUsingConstructorWithArgs(Class<?> type, @Nullable List<?> args, ConstructionInstruction optionalOuter) {
            return new ConstructorWithArgsInstruction(type, args, (ConstructorWithArgsInstruction)optionalOuter);
        }

        /** Merge arg lists, as follows:
         * if either list is null, take the other;
         * otherwise require them to be the same length.
         * <p>
         * For each entry, if they are maps, merge deep preferring the latter's values when they are not maps.
         * If they are anything else, we prefer the latter.
         */
        public static List<Object> mergeArgumentLists(List<?> olderArgs, List<?> args) {
            List<Object> newArgs;
            if (olderArgs==null || olderArgs.isEmpty()) newArgs = MutableList.copyOf(args);
            else if (args==null) newArgs = MutableList.copyOf(olderArgs);
            else {
                if (olderArgs.size() != args.size()) 
                    throw new IllegalStateException("Incompatible arguments, sizes "+olderArgs.size()+" and "+args.size()+": "+olderArgs+" and "+args);
                // merge
                newArgs = MutableList.of();
                Iterator<?> i1 = olderArgs.iterator();
                Iterator<?> i2 = args.iterator();
                while (i2.hasNext()) {
                    Object o1 = i1.next();
                    Object o2 = i2.next();
                    if ((o2 instanceof Map) && (o1 instanceof Map)) {
                        o2 = mergeMapsDeep((Map<?,?>)o1, (Map<?,?>)o2);
                    }
                    newArgs.add(o2);
                }
            }

            return newArgs;
        }

        public static Map<Object,Object> mergeMapsDeep(Map<?, ?> o1, Map<?, ?> o2) {
            MutableMap<Object,Object> result = MutableMap.<Object,Object>copyOf(o1);
            if (o2!=null) {
                for (Map.Entry<?,?> e2: o2.entrySet()) {
                    Object v2 = e2.getValue();
                    if (v2 instanceof Map) {
                        Object old = result.get(e2.getKey());
                        if (old instanceof Map) {
                           v2 = mergeMapsDeep((Map<?,?>)old, (Map<?,?>)v2); 
                        }
                    }
                    result.put(e2.getKey(), v2);
                }
            }
            return result;
        }
    }
    
    public static class ConstructorWithArgsInstruction implements ConstructionInstruction {
        private final Class<?> type;
        private final List<?> args;
        private final ConstructorWithArgsInstruction outerInstruction;
        
        protected ConstructorWithArgsInstruction(Class<?> type, List<?> args, @Nullable ConstructorWithArgsInstruction outerInstruction) {
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
        
        protected Maybe<Object> create(Class<?> typeConstraintSoFar, List<?> argsSoFar) {
            if (typeConstraintSoFar==null) typeConstraintSoFar = type;
            if (type!=null) {
                if (typeConstraintSoFar==null || typeConstraintSoFar.isAssignableFrom(type)) typeConstraintSoFar = type;
                else if (type.isAssignableFrom(typeConstraintSoFar)) { /* fine */ }
                else {
                    throw new IllegalStateException("Incompatible expected types "+typeConstraintSoFar+" and "+type);
                }
            }

            List<Object> newArgs = Factory.mergeArgumentLists(argsSoFar, args);
            
            if (outerInstruction!=null) {
                return outerInstruction.create(typeConstraintSoFar, newArgs);
            }
            
            if (typeConstraintSoFar==null) throw new IllegalStateException("No type information available");
            if ((typeConstraintSoFar.getModifiers() & (Modifier.ABSTRACT | Modifier.INTERFACE)) != 0) 
                throw new IllegalStateException("Insufficient type information: expected "+type+" is not directly instantiable");
            return Reflections.invokeConstructorFromArgsIncludingPrivate(typeConstraintSoFar, newArgs.toArray());
        }
    }
    
}
