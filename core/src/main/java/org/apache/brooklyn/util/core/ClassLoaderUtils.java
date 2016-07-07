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

import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.core.osgi.SystemFrameworkLoader;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.SystemFramework;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;

public class ClassLoaderUtils {
    /**
     * White list format should be 
     * <bundle symbolic name regex>[:<bundle version regex in dotted format (X.X.X.SNAPSHOT, instead of X.X.X-SNAPSHOT)>]
     */
    private static final String WHITE_LIST_DEFAULT = "org\\.apache\\.brooklyn\\..*:" + BrooklynVersion.get().replaceFirst("-", ".");
    private static final String WHITE_LIST_KEY = "org.apache.brooklyn.classloader.fallback.bundles";
    private static final String CLASS_NAME_DELIMITER = ":";
    private static final Logger log = LoggerFactory.getLogger(ClassLoaderUtils.class);

    // Class.forName gets the class loader from the calling class.
    // We don't have access to the same reflection API so need to pass it explicitly.
    private final Class<?> callingClass;
    private final Entity entity;
    private final ManagementContext mgmt;

    public ClassLoaderUtils(Object callingObj, Entity entity) {
        this(callingObj.getClass(), entity);
    }
    public ClassLoaderUtils(Object callingObj, @Nullable ManagementContext mgmt) {
        this(callingObj.getClass(), mgmt);
    }

    public ClassLoaderUtils(Class<?> callingClass) {
        this.callingClass = callingClass;
        this.entity = null;
        this.mgmt = null;
    }

    public ClassLoaderUtils(Class<?> callingClass, Entity entity) {
        this.callingClass = callingClass;
        this.entity = entity;
        this.mgmt = ((EntityInternal)entity).getManagementContext();
    }

    public ClassLoaderUtils(Class<?> callingClass, @Nullable ManagementContext mgmt) {
        this.callingClass = callingClass;
        this.entity = null;
        this.mgmt = mgmt;
    }

    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (looksLikeBundledClassName(name)) {
            String[] arr = name.split(CLASS_NAME_DELIMITER);
            String symbolicName;
            String version;
            String className;
            if (arr.length > 3) {
                throw new IllegalStateException("'" + name + "' doesn't look like a class name and contains too many colons to be parsed as bundle:version:class triple.");
            } else if (arr.length == 3) {
                symbolicName = arr[0];
                version = arr[1];
                className = arr[2];
            } else if (arr.length == 2) {
                symbolicName = arr[0];
                version = null;
                className = arr[1];
            } else {
                throw new IllegalStateException("'" + name + "' contains a bundle:version:class delimiter, but only one of those specified");
            }
            return loadClass(symbolicName, version, className);
        }

        if (entity != null && mgmt != null) {
            String catalogItemId = entity.getCatalogItemId();
            if (catalogItemId != null) {
                CatalogItem<?, ?> item = CatalogUtils.getCatalogItemOptionalVersion(mgmt, catalogItemId);
                if (item != null) {
                    BrooklynClassLoadingContext loader = CatalogUtils.newClassLoadingContext(mgmt, item);
                    try {
                        return loader.loadClass(name);
                    } catch (IllegalStateException e) {
                        ClassNotFoundException cnfe = Exceptions.getFirstThrowableOfType(e, ClassNotFoundException.class);
                        NoClassDefFoundError ncdfe = Exceptions.getFirstThrowableOfType(e, NoClassDefFoundError.class);
                        if (cnfe == null && ncdfe == null) {
                            throw e;
                        } else {
                            // ignore, fall back to Class.forName(...)
                        }
                    }
                } else {
                    log.warn("Entity " + entity + " refers to non-existent catalog item " + catalogItemId + ". Trying to load class " + name);
                }
            }
        }

        try {
            // Used instead of callingClass.getClassLoader() as it could be null (only for bootstrap classes)
            return Class.forName(name, true, callingClass.getClassLoader());
        } catch (ClassNotFoundException e) {
        }

        if (mgmt != null) {
            try {
                return mgmt.getCatalogClassLoader().loadClass(name);
            } catch (ClassNotFoundException e) {
            }
        }

        Class<?> cls = tryLoadFromBundleWhiteList(name);
        if (cls != null) {
            return cls;
        } else {
            throw new ClassNotFoundException("Class " + name + " not found on the application class path, nor in the bundle white list.");
        }
    }

    public Class<?> loadClass(String symbolicName, @Nullable String version, String className) throws ClassNotFoundException {
        Framework framework = getFramework();
        if (framework != null) {
            Maybe<Bundle> bundle = Osgis.bundleFinder(framework)
                .symbolicName(symbolicName)
                .version(version)
                .find();
            if (bundle.isAbsent() && version != null) {
                bundle = Osgis.bundleFinder(framework)
                    .symbolicName(symbolicName)
                    // Convert X.X.X-SNAPSHOT to X.X.X.SNAPSHOT. Any better way to do it?
                    .version(version.replace("-", "."))
                    .find();
            }
            if (bundle.isAbsent()) {
                throw new IllegalStateException("Bundle " + symbolicName + ":" + (version != null ? version : "any") + " not found to load class " + className);
            }
            return SystemFrameworkLoader.get().loadClassFromBundle(className, bundle.get());
        } else {
            return Class.forName(className);
        }
    }

    protected Framework getFramework() {
        if (mgmt != null) {
            Maybe<OsgiManager> osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager();
            if (osgiManager.isPresent()) {
                OsgiManager osgi = osgiManager.get();
                return osgi.getFramework();
            }
        }

        // Requires that caller code is executed AFTER loading bundle brooklyn-core
        Bundle bundle = FrameworkUtil.getBundle(ClassLoaderUtils.class);
        if (bundle != null) {
            BundleContext bundleContext = bundle.getBundleContext();
            return (Framework) bundleContext.getBundle(0);
        } else {
            return null;
        }
    }

    private boolean looksLikeBundledClassName(String name) {
        return name.indexOf(CLASS_NAME_DELIMITER) != -1;
    }


    private static class WhiteListBundlePredicate implements Predicate<Bundle> {
        private Pattern symbolicName;
        private Pattern version;

        private WhiteListBundlePredicate(String symbolicName, String version) {
            this.symbolicName = Pattern.compile(symbolicName);
            this.version = version != null ? Pattern.compile(version) : null;
        }

        @Override
        public boolean apply(Bundle input) {
            return symbolicName.matcher(input.getSymbolicName()).matches() &&
                    (version == null || version.matcher(input.getVersion().toString()).matches());
        }
    
    }

    private Class<?> tryLoadFromBundleWhiteList(String name) {
        Framework framework = getFramework();
        if (framework == null) {
            return null;
        }
        List<Bundle> bundles = Osgis.bundleFinder(framework)
            .satisfying(createBundleMatchingPredicate())
            .findAll();
        SystemFramework bundleLoader = SystemFrameworkLoader.get();
        for (Bundle b : bundles) {
            try {
                return bundleLoader.loadClassFromBundle(name, b);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
            }
        }
        return null;
    }

    protected WhiteListBundlePredicate createBundleMatchingPredicate() {
        String whiteList = System.getProperty(WHITE_LIST_KEY, WHITE_LIST_DEFAULT);
        String[] arr = whiteList.split(":");
        String symbolicName = arr[0];
        String version = null;
        if (arr.length > 2) {
            throw new IllegalStateException("Class loading fallback bundle white list '" + whiteList + "' not in the expected format <symbolic name regex>[:<version regex>].");
        } else if (arr.length == 2) {
            version = arr[1];
        }
        return new WhiteListBundlePredicate(symbolicName, version);
    }

}
