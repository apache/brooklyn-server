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

import java.lang.reflect.Member;
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
    
    /**
     * Contains method.toString() representations for methods for which {@link #findAccessibleMethod(Method)} 
     * failed to find a suitable publicly accessible method.
     */
    private static final Set<String> FIND_ACCESSIBLE_FAILED_LOGGED_METHODS = Sets.newConcurrentHashSet();

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
    
    // Copy of org.apache.commons.lang3.reflect.MemberUtils, except not checking !m.isSynthetic()
    static boolean isAccessible(Member m) {
        return m != null && Modifier.isPublic(m.getModifiers());
    }

    static boolean isAccessible(Class<?> c) {
        return c != null && Modifier.isPublic(c.getModifiers());
    }
    
    /**
     * @see {@link Reflections#findAccessibleMethod(Method)}
     */
    // Similar to org.apache.commons.lang3.reflect.MethodUtils.getAccessibleMethod
    // We could delegate to that, but currently brooklyn-utils-common doesn't depend on commons-lang3; maybe it should?
    static Maybe<Method> findAccessibleMethod(Method method) {
        if (!isAccessible(method)) {
            String err = "Method is not public, so not normally accessible for "+method;
            if (FIND_ACCESSIBLE_FAILED_LOGGED_METHODS.add(method.toString())) {
                LOG.warn(err+"; usage may subsequently fail");
            }
            return Maybe.absent(err);
        }
        
        boolean declaringClassAccessible = isAccessible(method.getDeclaringClass());
        if (declaringClassAccessible) {
            return Maybe.of(method);
        }
        
        // reflectively calling a public method on a private class can fail (unless one first 
        // calls setAccessible). Check if this overrides a public method on a public super-type
        // that we can call instead!
        
        if (Modifier.isStatic(method.getModifiers())) {
            String err = "Static method not declared on a public class, so not normally accessible for "+method;
            if (FIND_ACCESSIBLE_FAILED_LOGGED_METHODS.add(method.toString())) {
                LOG.warn(err+"; usage may subsequently fail");
            }
            return Maybe.absent(err);
        }

        Maybe<Method> altMethod = tryFindAccessibleEquivalent(method);
        
        if (altMethod.isPresent()) {
            LOG.debug("Switched method for publicly accessible equivalent: method={}; origMethod={}", altMethod.get(), method);
            return altMethod;
        } else {
            String err = "No accessible (overridden) method found in public super-types for method " + method;
            if (FIND_ACCESSIBLE_FAILED_LOGGED_METHODS.add(method.toString())) {
                LOG.warn(err);
            }
            return Maybe.absent(err);
        }
    }
    
    private static Maybe<Method> tryFindAccessibleEquivalent(Method method) {
        Class<?> clazz = method.getDeclaringClass();
        
        for (Class<?> interf : Reflections.getAllInterfaces(clazz)) {
            Maybe<Method> altMethod = tryFindAccessibleMethod(interf, method.getName(), method.getParameterTypes());
            if (altMethod.isPresent()) {
                return altMethod;
            }
        }
        
        Class<?> superClazz = clazz.getSuperclass();
        while (superClazz != null) {
            Maybe<Method> altMethod = tryFindAccessibleMethod(superClazz, method.getName(), method.getParameterTypes());
            if (altMethod.isPresent()) {
                return altMethod;
            }
            superClazz = superClazz.getSuperclass();
        }
        
        return Maybe.absent();
    }

    private static Maybe<Method> tryFindAccessibleMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
        if (!isAccessible(clazz)) {
            return Maybe.absent();
        }
        
        try {
            Method altMethod = clazz.getMethod(methodName, parameterTypes);
            if (isAccessible(altMethod) && !Modifier.isStatic(altMethod.getModifiers())) {
                return Maybe.of(altMethod);
            }
        } catch (NoSuchMethodException | SecurityException e) {
            // Not found; swallow, and return absent
        }
        
        return Maybe.absent();
    }
}
