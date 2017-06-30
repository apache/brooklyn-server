/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.core;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.util.core.osgi.SystemFrameworkLoader;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LoaderDispatcher<T> {
    Maybe<T> tryLoadFrom(Bundle bundle, String name);
    Maybe<T> tryLoadFrom(BrooklynClassLoadingContext loader, String name);
    Maybe<T> tryLoadFrom(ClassLoader loader, String name);

    public class ClassLoaderDispatcher implements LoaderDispatcher<Class<?>> {
        private static final Logger log = LoggerFactory.getLogger(LoaderDispatcher.class);
        public static final ClassLoaderDispatcher INSTANCE = new ClassLoaderDispatcher();

        @Override
        public Maybe<Class<?>> tryLoadFrom(Bundle bundle, String className) {
            try {
                return Maybe.<Class<?>>of(SystemFrameworkLoader.get().loadClassFromBundle(className, bundle));
            } catch (ClassNotFoundException e) {
                return Maybe.absent("Failed to load class " + className + " from bundle " + bundle, e);
            } catch (NoClassDefFoundError e) {
                // Can happen if a bundle misbehaves (e.g. it doesn't include Import-Package for 
                // all the packages it really needs.
                return Maybe.absent("Failed to load class " + className + " from bundle " + bundle, e);
            }
        }

        @Override
        public Maybe<Class<?>> tryLoadFrom(BrooklynClassLoadingContext loader, String className) {
            try {
                // return Maybe.<Class<?>>of(loader.loadClass(className));
                return loader.tryLoadClass(className);
            } catch (IllegalStateException e) {
                propagateIfCauseNotClassNotFound(e);
                return Maybe.absent("Failed to load class " + className + " from loader " + loader, e);
            } catch (NoClassDefFoundError e) {
                // Can happen if a bundle misbehaves (e.g. it doesn't include Import-Package for 
                // all the packages it really needs.
                return Maybe.absent("Failed to load class " + className + " from loader " + loader, e);
            }
        }

        @Override
        public Maybe<Class<?>> tryLoadFrom(ClassLoader classLoader, String className) {
            try {
                // Note that Class.forName(name, false, classLoader) doesn't seem to like us returning a 
                // class with a different name from that intended (e.g. stripping off an OSGi prefix).
                return Maybe.<Class<?>>of(classLoader.loadClass(className));
            } catch (IllegalStateException e) {
                propagateIfCauseNotClassNotFound(e);
                return Maybe.absent("Failed to load class " + className + " from class loader " + classLoader, e);
            } catch (ClassNotFoundException e) {
                return Maybe.absent("Failed to load class " + className + " from class loader " + classLoader, e);
            } catch (NoClassDefFoundError e) {
                // Can happen if a bundle misbehaves (e.g. it doesn't include Import-Package for 
                // all the packages it really needs.
                return Maybe.absent("Failed to load class " + className + " from class loader " + classLoader, e);
            }
        }

        private void propagateIfCauseNotClassNotFound(IllegalStateException e) {
            // TODO loadClass() should not throw IllegalStateException; should throw ClassNotFoundException without wrapping.
            ClassNotFoundException cnfe = Exceptions.getFirstThrowableOfType(e, ClassNotFoundException.class);
            NoClassDefFoundError ncdfe = Exceptions.getFirstThrowableOfType(e, NoClassDefFoundError.class);
            if (cnfe == null && ncdfe == null) {
                throw e;
            } else {
                if (ncdfe != null) {
                    log.debug("Class loading failure", ncdfe);
                } else if (cnfe != null) {
                    BundleException bundleException = Exceptions.getFirstThrowableOfType(cnfe, BundleException.class);
                    // wiring problem
                    if (bundleException != null) {
                        log.debug("Class loading failure", cnfe);
                    }
                }
                // ignore, try next way of loading
            }
        }

    }

    public class ResourceLoaderDispatcher implements LoaderDispatcher<URL> {
        public static final ResourceLoaderDispatcher INSTANCE = new ResourceLoaderDispatcher();

        @Override
        public Maybe<URL> tryLoadFrom(Bundle bundle, String name) {
            return Maybe.ofDisallowingNull(SystemFrameworkLoader.get().getResourceFromBundle(name, bundle));
        }

        @Override
        public Maybe<URL> tryLoadFrom(BrooklynClassLoadingContext loader, String name) {
            return Maybe.ofDisallowingNull(loader.getResource(name));
        }

        @Override
        public Maybe<URL> tryLoadFrom(ClassLoader classLoader, String name) {
            return Maybe.ofDisallowingNull(classLoader.getResource(name));
        }

    }

    public class MultipleResourceLoaderDispatcher implements LoaderDispatcher<Iterable<URL>> {
        public static final MultipleResourceLoaderDispatcher INSTANCE = new MultipleResourceLoaderDispatcher();

        @Override
        public Maybe<Iterable<URL>> tryLoadFrom(Bundle bundle, String name) {
            try {
                return emptyToMaybeNull(SystemFrameworkLoader.get().getResourcesFromBundle(name, bundle));
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }

        @Override
        public Maybe<Iterable<URL>> tryLoadFrom(BrooklynClassLoadingContext loader, String name) {
            return emptyToMaybeNull(loader.getResources(name));
        }

        @Override
        public Maybe<Iterable<URL>> tryLoadFrom(ClassLoader classLoader, String name) {
            try {
                return emptyToMaybeNull(classLoader.getResources(name));
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }
        
        private Maybe<Iterable<URL>> emptyToMaybeNull(Iterable<URL> iter) {
            if (iter.iterator().hasNext()) {
                return Maybe.of(iter);
            } else {
                return Maybe.absentNull();
            }
        }
        
        private Maybe<Iterable<URL>> emptyToMaybeNull(Enumeration<URL> res) {
            if (res == null) {
                return Maybe.absentNull();
            } else {
                return emptyToMaybeNull(Collections.list(res));
            }
        }

    }

}

