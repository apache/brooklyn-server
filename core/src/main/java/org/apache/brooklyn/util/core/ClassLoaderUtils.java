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

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URL;
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
import org.apache.brooklyn.util.core.LoaderDispatcher.ClassLoaderDispatcher;
import org.apache.brooklyn.util.core.LoaderDispatcher.MultipleResourceLoaderDispatcher;
import org.apache.brooklyn.util.core.LoaderDispatcher.ResourceLoaderDispatcher;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.OsgiUtils;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

public class ClassLoaderUtils {
    
    private static final Logger log = LoggerFactory.getLogger(ClassLoaderUtils.class);

    /**
     * White list format should be 
     * <bundle symbolic name regex>[:<bundle version regex in dotted format (X.X.X.SNAPSHOT, instead of X.X.X-SNAPSHOT)>]
     */
    static final String WHITE_LIST_KEY = "org.apache.brooklyn.classloader.fallback.bundles";
    static final String CLASS_NAME_DELIMITER = ":";
    private static final String WHITE_LIST_DEFAULT = "org\\.apache\\.brooklyn\\..*:" + OsgiUtils.toOsgiVersion(BrooklynVersion.get());

    // Class.forName gets the class loader from the calling class.
    // We don't have access to the same reflection API so need to pass it explicitly.
    private final ClassLoader classLoader;
    private final Entity entity;
    private final ManagementContext mgmt;

    public ClassLoaderUtils(Object callingObj, Entity entity) {
        this(callingObj.getClass(), entity);
    }
    public ClassLoaderUtils(Object callingObj, @Nullable ManagementContext mgmt) {
        this(callingObj.getClass(), mgmt);
    }

    public ClassLoaderUtils(Class<?> callingClass) {
        checkNotNull(callingClass, "callingClass");
        this.classLoader = getValidClassLoader(callingClass.getClassLoader());
        this.entity = null;
        this.mgmt = null;
    }

    public ClassLoaderUtils(@Nullable ClassLoader cl) {
        this.classLoader = getValidClassLoader(cl);
        this.entity = null;
        this.mgmt = null;
    }

    public ClassLoaderUtils(@Nullable ClassLoader cl, @Nullable ManagementContext mgmt) {
        this.classLoader = getValidClassLoader(cl);
        this.entity = null;
        this.mgmt = checkNotNull(mgmt, "mgmt");
    }

    public ClassLoaderUtils(Class<?> callingClass, Entity entity) {
        checkNotNull(callingClass, "callingClass");
        this.classLoader = getValidClassLoader(callingClass.getClassLoader());
        this.entity = checkNotNull(entity, "entity");
        this.mgmt = ((EntityInternal)entity).getManagementContext();
    }

    public ClassLoaderUtils(Class<?> callingClass, @Nullable ManagementContext mgmt) {
        checkNotNull(callingClass, "callingClass");
        this.classLoader = getValidClassLoader(callingClass.getClassLoader());
        this.entity = null;
        this.mgmt = mgmt;
    }

    // class.getClassLoader() could return null for classes on the boot class path,
    // provide an alternative in this case.
    protected ClassLoader getValidClassLoader(ClassLoader cl) {
        return (cl != null) ? cl : getClass().getClassLoader();
    }

    /**
     * Loads the given class, handle OSGi bundles. The class could be in one of the following formats:
     * <ul>
     *   <li>{@code <classname>}, such as {@code com.google.common.net.HostAndPort}
     *   <li>{@code <bunde-symbolicName>:<classname>}, such as {@code com.google.guava:com.google.common.net.HostAndPort}
     *   <li>{@code <bunde-symbolicName>:<bundle-version>:<classname>}, such as {@code com.google.guava:16.0.1:com.google.common.net.HostAndPort}
     * </ul>
     * 
     * The classloading order is as follows:
     * <ol>
     *   <li>If the class explicitly states the bundle name and version, then load from that.
     *   <li>Otherwise try to load from the catalog's classloader. This is so we respect any 
     *       {@code libraries} supplied in the catalog metadata, and can thus handle updating 
     *       catalog versions. It also means we can try our best to handle a catalog that
     *       uses a different bundle version from something that ships with Brooklyn.
     *   <li>The white-listed bundles (i.e. those that ship with Brooklyn). We prefer the 
     *       version of the bundle that Brooklyn depends on, rather than taking the highest
     *       version installed (e.g. Karaf has Guava 16.0.1 and 18.0; we want the former, which
     *       Brooklyn uses).
     *   <li>The classloader passed in. Normally this is a boring unhelpful classloader (e.g.
     *       obtained from {@code callingClass.getClassLoader()}), so won't work. But it's up
     *       to the caller if they pass in something more useful.
     *   <li>The {@link ManagementContext#getCatalogClassLoader()}. Again, this is normally not helpful. 
     *       We instead would prefer the specific catalog item's classloader (which we tried earlier).
     *   <li>If we were given a bundle name without a version, then finally try just using the
     *       most recent version of the bundle that is available in the OSGi container.
     * </ol>
     * 
     * The list of "white-listed bundles" are controlled using the system property named
     * {@link #WHITE_LIST_KEY}, defaulting to all {@code org.apache.brooklyn.*} bundles.
     */
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        Maybe<Class<?>> cls = load(name, ClassLoaderDispatcher.INSTANCE);
        if (cls.isPresent()) {
            return cls.get();
        } else {
            throw new ClassNotFoundException("Class " + name + " not found on the application class path, nor in the bundle white list.", getReturnException(cls));
        }
    }

    public Class<?> loadClass(String symbolicName, @Nullable String version, String className) throws ClassNotFoundException {
        try {
            return tryLoadFromBundle(ClassLoaderDispatcher.INSTANCE, symbolicName, version, className).get();
        } catch (IllegalStateException e) {
            throw new ClassNotFoundException("Class " + className + " could not be loaded from bundle " + toBundleString(symbolicName, version), e);
        }
    }

    /**
     * Finds the resource with the given name.
     * @see {@link #loadClass(String)} for loading order
     *
     * @return null when no resource is found
     */
    public URL getResource(String name) {
        return load(stripLeadingSlash(name), ResourceLoaderDispatcher.INSTANCE).orNull();
    }

    /**
     * Finds all the resources with the given name. Aborts going through subsequent fallbacks when it finds at least one resource.
     * @see {@link #loadClass(String)} for loading order
     * 
     * @return empty {@link Iterable} when no resources find
     */
    public Iterable<URL> getResources(String name) {
        Maybe<Iterable<URL>> ret = load(stripLeadingSlash(name), MultipleResourceLoaderDispatcher.INSTANCE);
        if (ret.isPresent()) {
            return ret.get();
        } else {
            return ImmutableList.of();
        }
    }

    protected <T> Maybe<T> load(String name, LoaderDispatcher<T> dispatcher) {
        String symbolicName;
        String version;
        String className;

        if (looksLikeBundledClassName(name)) {
            String[] arr = name.split(CLASS_NAME_DELIMITER);
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
        } else {
            symbolicName = null;
            version = null;
            className = name;
        }

        Maybe<T> cls;
        if (symbolicName != null && version != null) {
            // Very explicit; do as we're told!
            return tryLoadFromBundle(dispatcher, symbolicName, version, className);
        }
        
        if (symbolicName != null) {
            cls = tryLoadFromBundle(dispatcher, symbolicName, version, className);
            if (cls.isPresent()) {
                return cls;
            }
            Maybe<T> result = loadClass(name, dispatcher, className);
            
            String notFoundWithSymbolicNameMessage = "No class '"+className+"' in bundle '"+symbolicName+"' (using spec '"+name+"')";
            if (result.isPresent()) {
                log.warn(notFoundWithSymbolicNameMessage+"; found using deprecated no-bundle syntax, but this behaviour is deprecated and likely to be unsupported in future. Change so invalid bundle is not supplied.");
            } else {
                return Maybe.absent(notFoundWithSymbolicNameMessage);
            }
        }

        return loadClass(name, dispatcher, className);
    }
    
    private <T> Maybe<T> loadClass(String name, LoaderDispatcher<T> dispatcher, String className) {
        Maybe<T> cls;
        if (entity != null && mgmt != null) {
            String catalogItemId = entity.getCatalogItemId();
            if (catalogItemId != null) {
                CatalogItem<?, ?> item = CatalogUtils.getCatalogItemOptionalVersion(mgmt, catalogItemId);
                if (item != null) {
                    BrooklynClassLoadingContext loader = CatalogUtils.newClassLoadingContext(mgmt, item);
                    cls = dispatcher.tryLoadFrom(loader, className);
                    if (cls.isPresent()) {
                        return cls;
                    }
                } else {
                    log.warn("Entity " + entity + " refers to non-existent catalog item " + catalogItemId + ". Trying to load class " + name);
                }
            }
        }

        cls = tryLoadFromBundleWhiteList(dispatcher, className);
        if (cls.isPresent()) {
            return cls;
        }

        cls = dispatcher.tryLoadFrom(classLoader, className);
        if (cls.isPresent()) {
            return cls;
        }

        if (mgmt != null) {
            cls = dispatcher.tryLoadFrom(mgmt.getCatalogClassLoader(), className);
            if (cls.isPresent()) {
                return cls;
            }
        }

        return Maybe.absentNull();
    }

    protected <T> Maybe<T> tryLoadFromBundle(LoaderDispatcher<T> dispatcher, String symbolicName, String version, String name) {
        Framework framework = getFramework();
        if (framework != null) {
            Maybe<Bundle> bundle = Osgis.bundleFinder(framework)
                .symbolicName(symbolicName)
                .version(OsgiUtils.toOsgiVersion(version))
                .find();
            if (bundle.isAbsent()) {
                throw new IllegalStateException("Bundle " + toBundleString(symbolicName, version) + " not found to load " + name);
            }
            return dispatcher.tryLoadFrom(bundle.get(), name);
        } else {
            log.warn("Request for bundle '"+symbolicName+"' "+(Strings.isNonBlank(version) ? "("+version+") " : "")+"will be ignored, loading '"+name+"' as no framework available");
            return dispatcher.tryLoadFrom(classLoader, name);
        }
    }

    @Beta
    public boolean isBundleWhiteListed(Bundle bundle) {
        WhiteListBundlePredicate p = createBundleMatchingPredicate();
        return p.apply(bundle);
    }

    private Framework getFramework() {
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


    protected static class WhiteListBundlePredicate implements Predicate<Bundle> {
        private final Pattern symbolicName;
        private final Pattern version;

        private WhiteListBundlePredicate(String symbolicName, String version) {
            this.symbolicName = Pattern.compile(checkNotNull(symbolicName, "symbolicName"));
            this.version = version != null ? Pattern.compile(version) : null;
        }

        @Override
        public boolean apply(Bundle input) {
            return symbolicName.matcher(input.getSymbolicName()).matches() &&
                    (version == null || version.matcher(input.getVersion().toString()).matches());
        }
    }

    protected <T> Maybe<T> tryLoadFromBundleWhiteList(LoaderDispatcher<T> dispatcher, String className) {
        Framework framework = getFramework();
        if (framework == null) {
            return Maybe.absentNull();
        }
        List<Bundle> bundles = Osgis.bundleFinder(framework)
            .satisfying(createBundleMatchingPredicate())
            .findAll();
        for (Bundle b : bundles) {
            Maybe<T> item = dispatcher.tryLoadFrom(b, className);
            if (item.isPresent()) {
                return item;
            }
        }
        return Maybe.absentNull();
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

    private String toBundleString(String symbolicName, String version) {
        return symbolicName + ":" + (version != null ? version : "any");
    }

    /**
     * cls should always be isAbsent()
     * @return null for cls.isNull(). Otherwise return the exception that's contained in the Maybe.
     */
    protected Exception getReturnException(Maybe<Class<?>> cls) {
        if (cls.isNull()) {
            return null;
        }
        try {
            cls.get();
            return null;
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            return e;
        }
    }

    // ClassLoader.getResource returns null if there's a leading slash
    private String stripLeadingSlash(String name) {
        String[] arr = name.split(":");
        int last = arr.length - 1;
        if (arr[last].startsWith("/")) {
            arr[last] = arr[last].substring(1);
        }
        return Joiner.on(":").join(arr);
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + MoreObjects.toStringHelper(this)
            .add("claddLoader", classLoader)
            .add("entity", entity)
            .add("mgmt", mgmt) + "]";
    }
}
