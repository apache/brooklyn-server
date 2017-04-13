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
package org.apache.brooklyn.core.objs.proxy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class InternalFactory {

    private static final Logger LOG = LoggerFactory.getLogger(InternalFactory.class);
    
    protected final ManagementContextInternal managementContext;

    /**
     * For tracking if constructor has been called by framework, or in legacy way (i.e. directly).
     * 
     * To be deleted once we delete support for constructing directly (and expecting configure() to be
     * called inside the constructor, etc).
     * 
     * @author aled
     */
    public static class FactoryConstructionTracker {
        private static ThreadLocal<Boolean> constructing = new ThreadLocal<Boolean>();
        
        public static boolean isConstructing() {
            return (constructing.get() == Boolean.TRUE);
        }
        
        static void reset() {
            constructing.set(Boolean.FALSE);
        }
        
        static void setConstructing() {
            constructing.set(Boolean.TRUE);
        }
    }

    /**
     * Returns true if this is a "new-style" policy (i.e. where not expected callers to use the constructor directly to instantiate it).
     * 
     * @param clazz
     */
    public static boolean isNewStyle(Class<?> clazz) {
        try {
            clazz.getDeclaredConstructor(new Class[0]);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }
    
    public InternalFactory(ManagementContextInternal managementContext) {
        this.managementContext = checkNotNull(managementContext, "managementContext");
    }

    /**
     * Constructs an instance (e.g. of entity, location, enricher or policy.
     * Checks if special instructions on the spec, if supplied;
     * else if new-style class, calls no-arg constructor (ignoring spec); 
     * else if old-style, uses flags if avail as args or as spec and passes to constructor.
     */
    protected <T> T construct(Class<? extends T> clazz, AbstractBrooklynObjectSpec<?,?> optionalSpec, Map<String,?> optionalOldStyleConstructorFlags) {
        try {
            if (optionalSpec!=null) {
                ConfigBag bag = ConfigBag.newInstance(optionalSpec.getConfig());
                Class<? extends SpecialBrooklynObjectConstructor> special = bag.get(SpecialBrooklynObjectConstructor.Config.SPECIAL_CONSTRUCTOR);
                if (special!=null) {
                    // special construction requested; beta and very limited scope; see SpecialBrooklynObjectConstructor 
                    return constructWithSpecial(special, clazz, optionalSpec);
                }
            }
            
            if (isNewStyle(clazz)) {
                return constructNewStyle(clazz);
            } else {
                MutableMap<String,Object> constructorFlags = MutableMap.of();
                if (optionalOldStyleConstructorFlags!=null) constructorFlags.add(optionalOldStyleConstructorFlags);
                else constructorFlags.add(optionalSpec.getFlags());
                
                return constructOldStyle(clazz, MutableMap.copyOf(constructorFlags));
            }
        } catch (Exception e) {
            throw Exceptions.propagate(e);
         }
     }

    private <T> T constructWithSpecial(Class<? extends SpecialBrooklynObjectConstructor> special, Class<? extends T> clazz, AbstractBrooklynObjectSpec<?, ?> spec) {
        try {
            return special.newInstance().create(managementContext, clazz, spec);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw Exceptions.propagate("Unable to create "+clazz+" "+spec+" using special "+special, e);
        }
    }

    /**
     * Constructs a new instance (fails if no no-arg constructor).
     */
    protected <T> T constructNewStyle(Class<T> clazz) {
        if (!isNewStyle(clazz)) {
            throw new IllegalStateException("Class "+clazz+" must have a no-arg constructor");
        }
        
        try {
            FactoryConstructionTracker.setConstructing();
            try {
                Constructor<T> constructor = clazz.getDeclaredConstructor(new Class[0]);
                if (!(Modifier.isPublic(clazz.getModifiers()) && Modifier.isPublic(constructor.getModifiers()))
                        && !constructor.isAccessible()) {
                    try {
                        constructor.setAccessible(true);
                        LOG.warn("Set no-arg constructor accessible for "+clazz+" (deprecated; should have public no-arg constructor)");
                    } catch (SecurityException e) {
                        LOG.warn("Unable to set no-arg constructor accessible for "+clazz+" (continuing, but may subsequently fail): " + e);
                    }
                }
                return constructor.newInstance(new Object[0]);
            } finally {
                FactoryConstructionTracker.reset();
            }
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    protected <T> T constructOldStyle(Class<T> clazz, Map<String,?> flags) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        FactoryConstructionTracker.setConstructing();
        Maybe<T> v;
        try {
            v = Reflections.invokeConstructorFromArgs(clazz, new Object[] {MutableMap.copyOf(flags)}, true);
        } finally {
            FactoryConstructionTracker.reset();
        }
        if (v.isPresent()) {
            return v.get();
        } else {
            throw new IllegalStateException("No valid constructor defined for "+clazz+" (expected no-arg or single java.util.Map argument)");
        }
    }
}
