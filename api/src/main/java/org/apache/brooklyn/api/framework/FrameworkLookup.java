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
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

/**
 * A utility to fetch an instance of a class from either OSGI if available, or by a Service Loader otherwise.
 *
 * The intention of this class is to provide a means to lookup beans in the OSGI service registry that were
 * previously looked up via a ServiceLoader, but still maintain the backward compatible behaviour when not running
 * in an OSGI container.
 */
public class FrameworkLookup {

    private static final Logger LOG = LoggerFactory.getLogger(FrameworkLookup.class);

    /**
     * Find an instance of the given class in the framework.
     * This first performs an OSGI lookup if the OSGI framework is available. If it is not, or no implementation is
     * found in OSGI, then it falls back to attempting a lookup from the current context classpath via a ServiceLoader.
     *
     * @param clazz The class (typically the class of an interface) to search in the framework.
     * @param <T>  The type for the class.
     * @return  A maybe of the instance found in the framework.
     */
    public static <T> Maybe<T> lookup (Class<T> clazz) {
        Maybe<T> result = lookupInOsgi(clazz);

        if (result.isAbsent()) {
            result = lookupViaServiceLoader(clazz);
        }

        return result;
    }

    private static <T> Maybe<T> lookupViaServiceLoader(Class<T> clazz) {
        LOG.debug("Looking up  via ServiceLoader");

        Maybe<T> result = Maybe.absent("No result found with ServiceLoader");
        ServiceLoader<T> LOADER = ServiceLoader.load(clazz);
        for (T item : LOADER) {
            return Maybe.of(item);
        }
        return result;
    }

    private static <T> Maybe<T> lookupInOsgi(Class<T> clazz) {
        Maybe<T> result = Maybe.absent("No result found with OSGI");

        final Bundle bundle = FrameworkUtil.getBundle(clazz);
        if (bundle != null) {
            LOG.debug("Looking up T in OSGI");
            BundleContext ctx = bundle.getBundleContext();
            final ServiceReference<T> reference = ctx.getServiceReference(clazz);
            final T service = ctx.getService(reference);
            result = Maybe.of(service);
        }
        return result;
    }
}
