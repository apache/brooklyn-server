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
package org.apache.brooklyn.util.javalang;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;

import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * Use {@link Reflections} methods to access this. The methods are declared here (to this 
 * package-private class) so we can avoid having an ever-growing single Reflections class!
 */
class MethodAccessibleReflections {

    private static final Logger LOG = LoggerFactory.getLogger(MethodAccessibleReflections.class);
    
    /**
     * Contains method.toString() representations for methods we have logged about failing to
     * set accessible (or to find an alternative accessible version). Use this to ensure we 
     * log.warn just once per method, rather than risk flooding our log.
     */
    private static final Set<String> SET_ACCESSIBLE_FAILED_LOGGED_METHODS = Sets.newConcurrentHashSet();
    
    /**
     * Contains method.toString() representations for methods we have logged about having set
     * accessible. Having to setAccessible is discouraged, so want a single log.warn once per
     * method.
     */
    private static final Set<String> SET_ACCESSIBLE_SUCCEEDED_LOGGED_METHODS = Sets.newConcurrentHashSet();
    
    static boolean trySetAccessible(Method method) {
        try {
            method.setAccessible(true);
            if (SET_ACCESSIBLE_SUCCEEDED_LOGGED_METHODS.add(method.toString())) {
                LOG.warn("Discouraged use of setAccessible, called for method " + method);
            } else {
                if (LOG.isTraceEnabled()) LOG.trace("Discouraged use of setAccessible, called for method " + method);
            }
            return true;
            
        } catch (SecurityException e) {
            boolean added = SET_ACCESSIBLE_FAILED_LOGGED_METHODS.add(method.toString());
            if (added) {
                LOG.warn("Problem setting accessible for method " + method, e);
            } else {
                if (LOG.isTraceEnabled()) LOG.trace("Problem setting accessible for method " + method, e);
            }
            return false;
        }
    }

    /**
     * @see {@link Reflections#findAccessibleMethod(Method)}
     */
    static Method findAccessibleMethod(Method method) {
        if (!Modifier.isPublic(method.getModifiers())) {
            trySetAccessible(method);
            return method;
        }
        boolean declaringClassPublic = Modifier.isPublic(method.getDeclaringClass().getModifiers());
        if (!declaringClassPublic) {
            // reflectively calling a public method on a private class can fail, unless we first set it
            // call setAccessible. But first see if there is a public method on a public super-type
            // that we can call instead!
            Maybe<Method> publicMethod = tryFindPublicEquivalent(method);
            if (publicMethod.isPresent()) {
                LOG.debug("Switched method for publicly accessible equivalent: method={}; origMethod={}", publicMethod.get(), method);
                return publicMethod.get();
            } else {
                trySetAccessible(method);
                return method;
            }
        }
        
        return method;
    }
    
    private static Maybe<Method> tryFindPublicEquivalent(Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            return Maybe.absent();
        }
        
        Class<?> clazz = method.getDeclaringClass();
        
        for (Class<?> interf : clazz.getInterfaces()) {
            Maybe<Method> altMethod = tryFindPublicMethod(interf, method.getName(), method.getParameterTypes());
            if (altMethod.isPresent()) {
                return altMethod;
            }
        }
        
        Class<?> superClazz = clazz.getSuperclass();
        while (superClazz != null) {
            Maybe<Method> altMethod = tryFindPublicMethod(superClazz, method.getName(), method.getParameterTypes());
            if (altMethod.isPresent()) {
                return altMethod;
            }
            superClazz = superClazz.getSuperclass();
        }
        
        return Maybe.absent();
    }

    private static Maybe<Method> tryFindPublicMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
        if (!Modifier.isPublic(clazz.getModifiers())) {
            return Maybe.absent();
        }
        
        try {
            Method altMethod = clazz.getMethod(methodName, parameterTypes);
            if (Modifier.isPublic(altMethod.getModifiers()) && !Modifier.isStatic(altMethod.getModifiers())) {
                return Maybe.of(altMethod);
            }
        } catch (NoSuchMethodException | SecurityException e) {
            // Not found; return absent
        }
        
        return Maybe.absent();
    }
}
