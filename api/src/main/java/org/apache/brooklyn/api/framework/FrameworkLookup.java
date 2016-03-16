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
package org.apache.brooklyn.api.framework;

import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.OsgiUtil;
import org.osgi.framework.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

/**
 * A utility to fetch an instance of a class from either OSGI if available, or by a Service Loader otherwise.
 *
 * The intention of this class is to provide a means to lookup beans in the OSGI service registry that were
 * previously looked up via a ServiceLoader, but still maintain the backward compatible behaviour when not running
 * in an OSGI container.
 *
 * NOTE, when OSGI bundle registry is used, this code performs a BundleContext.getService(). When calling
 * BundleContext.getService() the reference count for the service in question is increased, and without any calls to
 * BundleContext.ungetService() the only moment it will drop to zero is when the client bundle is unloaded,
 * which this code does not do.  Therefore this class is not suitable for use in a situation where client code needs
 * to take account of services coming and going, and explicitly avoid using the service when its reference count has
 * gone to zero.
 */
public class FrameworkLookup {

    private static final Logger LOG = LoggerFactory.getLogger(FrameworkLookup.class);

    /**
     * Find an instance of the given class in the framework.
     * This first performs an OSGI lookup if the OSGI framework is available. If it is not then it falls back to
     * attempting a lookup from the current context classpath via a ServiceLoader.
     *
     * @param clazz The class (typically the class of an interface) to search in the framework.
     * @param <T>  The type for the class.
     * @return  A maybe of the instance found in the framework.
     */
    public static <T> Maybe<T> lookup (Class<T> clazz) {
        return lookup(clazz, null);
    }

    /**
     * Find an instance of the given class in the framework.
     * This first performs an OSGI lookup if the OSGI framework is available. If it is not then it falls back to
     * attempting a lookup from the current context classpath via a ServiceLoader.
     *
     * @param clazz The class (typically the class of an interface) to search in the framework.
     * @param loader If falling back to ServiceLoader, this class loader is used to load the class. If <tt>null</tt>,
     *         the system class loader (or, failing that, the bootstrap class loader) is used.
     * @param <T>  The type for the class.
     * @return  A maybe of the instance found in the framework.
     */
    public static <T> Maybe<T> lookup (Class<T> clazz, ClassLoader loader) {

        Maybe<T> result;
        if (OsgiUtil.isBrooklynInsideFramework()) {
            result = lookupInOsgi(clazz);
        } else {
            result = lookupViaServiceLoader(clazz, loader);
        }

        return result;
    }

    /**
     * Find all instances of the given class in the framework.
     * This first performs an OSGI lookup if the OSGI framework is available. If it is not then it falls back to
     * attempting a lookup from the current context classpath via a ServiceLoader.
     *
     * @param clazz The class (typically the class of an interface) to search in the framework.
     * @param <T>  The type for the class.
     * @return  An iterable over the instances found in the framework.
     */
    public static <T> Iterable<T> lookupAll(Class<T> clazz) {
        return lookupAll(clazz, null);
    }

    /**
     * Find all instances of the given class in the framework.
     * This first performs an OSGI lookup if the OSGI framework is available. If it is not then it falls back to
     * attempting a lookup from the current context classpath via a ServiceLoader.
     *
     * @param clazz The class (typically the class of an interface) to search in the framework.
     * @param loader If falling back to ServiceLoader, this class loader is used to load the class. If <tt>null</tt>,
     *         the system class loader (or, failing that, the bootstrap class loader) is used.
     * @param <T>  The type for the class.
     * @return  An iterable over the instances found in the framework.
     */
    public static <T> Iterable<T> lookupAll(Class<T> clazz, ClassLoader loader) {

        Iterable<T> result;
        if (OsgiUtil.isBrooklynInsideFramework()) {
            result = lookupAllInOsgi(clazz);
        } else {
            result = lookupAllViaServiceLoader(clazz, loader);
        }
        return result;
    }

    private static <T> ServiceLoader<T> lookupAllViaServiceLoader(Class<T> clazz, ClassLoader loader) {
        LOG.debug("Looking up all " + clazz.getSimpleName() + "  via ServiceLoader");

        if (null == loader) {
            return ServiceLoader.load(clazz);
        }
        return ServiceLoader.load(clazz, loader);
    }

    private static <T> Iterable<T> lookupAllInOsgi(Class<T> clazz) {
        final List<T> result = new ArrayList<>();
        final Bundle bundle = FrameworkUtil.getBundle(clazz);
        if (bundle != null) {
            LOG.debug("Looking up all " + clazz.getSimpleName() + " in OSGI");
            BundleContext ctx = bundle.getBundleContext();
            for (ServiceReference<T> reference: getServiceReferences(clazz, ctx)) {
                result.add(ctx.getService(reference));
            }
        }
        return result;
    }

    private static <T> Maybe<T> lookupViaServiceLoader(Class<T> clazz, ClassLoader loader) {
        LOG.debug("Looking up " + clazz.getSimpleName() + "  via ServiceLoader");

        Maybe<T> result = Maybe.absent("No class " + clazz.getSimpleName() + " found with ServiceLoader");
        ServiceLoader<T> LOADER = lookupAllViaServiceLoader(clazz, loader);
        for (T item : LOADER) {
            return Maybe.of(item);
        }
        return result;
    }

    private static <T> Maybe<T> lookupInOsgi(Class<T> clazz) {
        Maybe<T> result = Maybe.absent("No class " + clazz.getSimpleName() + " found with OSGI");

        final Bundle bundle = FrameworkUtil.getBundle(clazz);
        if (bundle != null) {
            LOG.debug("Looking up " + clazz.getSimpleName() + " in OSGI");
            BundleContext ctx = bundle.getBundleContext();
            final ServiceReference<T> reference = ctx.getServiceReference(clazz);
            final T service = ctx.getService(reference);
            result = Maybe.of(service);
        }
        return result;
    }

    private static <T> Collection<ServiceReference<T>> getServiceReferences(Class<T> clazz, BundleContext ctx) {
        try {
            return ctx.getServiceReferences(clazz, null);
        } catch (InvalidSyntaxException e) {
            // handle the checked(!) exception declared on the method, we pass null as filter above
            throw new RuntimeException(e);
        }
    }



}
