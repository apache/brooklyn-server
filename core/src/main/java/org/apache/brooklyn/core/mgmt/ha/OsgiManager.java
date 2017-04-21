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
package org.apache.brooklyn.core.mgmt.ha;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.catalog.internal.CatalogBundleLoader;
import org.apache.brooklyn.core.mgmt.persist.OsgiClassPrefixer;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.core.osgi.Osgis.BundleFinder;
import org.apache.brooklyn.util.core.osgi.SystemFrameworkLoader;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.os.Os.DeletionResult;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class OsgiManager {

    private static final Logger log = LoggerFactory.getLogger(OsgiManager.class);
    
    public static final ConfigKey<Boolean> USE_OSGI = BrooklynServerConfig.USE_OSGI;
    
    /* see Osgis for info on starting framework etc */
    
    protected final ManagementContext mgmt;
    protected final OsgiClassPrefixer osgiClassPrefixer;
    protected Framework framework;
    protected File osgiCacheDir;
    protected Map<String, ManagedBundle> managedBundles = MutableMap.of();
    
    public OsgiManager(ManagementContext mgmt) {
        this.mgmt = mgmt;
        this.osgiClassPrefixer = new OsgiClassPrefixer();
    }

    public void start() {
        try {
            osgiCacheDir = BrooklynServerPaths.getOsgiCacheDirCleanedIfNeeded(mgmt);
            
            // any extra OSGi startup args could go here
            framework = Osgis.getFramework(osgiCacheDir.getAbsolutePath(), false);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    public void stop() {
        Osgis.ungetFramework(framework);
        if (BrooklynServerPaths.isOsgiCacheForCleaning(mgmt, osgiCacheDir)) {
            // See exception reported in https://issues.apache.org/jira/browse/BROOKLYN-72
            // We almost always fail to delete he OSGi temp directory due to a concurrent modification.
            // Therefore keep trying.
            final AtomicReference<DeletionResult> deletionResult = new AtomicReference<DeletionResult>();
            Repeater.create("Delete OSGi cache dir")
                    .until(new Callable<Boolean>() {
                        @Override
                        public Boolean call() {
                            deletionResult.set(Os.deleteRecursively(osgiCacheDir));
                            return deletionResult.get().wasSuccessful();
                        }})
                    .limitTimeTo(Duration.ONE_SECOND)
                    .backoffTo(Duration.millis(50))
                    .run();
            if (deletionResult.get().getThrowable()!=null) {
                log.debug("Unable to delete "+osgiCacheDir+" (possibly being modified concurrently?): "+deletionResult.get().getThrowable());
            }
        }
        osgiCacheDir = null;
        framework = null;
    }

    public Map<String, ManagedBundle> getManagedBundles() {
        return ImmutableMap.copyOf(managedBundles);
    }
    
    public Bundle installUploadedBundle(ManagedBundle bundleMetadata, InputStream zipIn, boolean loadCatalogBom) {
        try {
            Bundle alreadyBundle = checkBundleInstalledThrowIfInconsistent(bundleMetadata, false);
            if (alreadyBundle!=null) {
                return alreadyBundle;
            }

            File zipF = Os.newTempFile("brooklyn-bundle-transient-"+bundleMetadata, "zip");
            FileOutputStream fos = new FileOutputStream(zipF);
            Streams.copy(zipIn, fos);
            zipIn.close();
            fos.close();
            
            Bundle bundleInstalled = framework.getBundleContext().installBundle(bundleMetadata.getOsgiUniqueUrl(), 
                new FileInputStream(zipF));
            checkCorrectlyInstalled(bundleMetadata, bundleInstalled);
            if (!bundleMetadata.isNameResolved()) {
                ((BasicManagedBundle)bundleMetadata).setSymbolicName(bundleInstalled.getSymbolicName());
                ((BasicManagedBundle)bundleMetadata).setVersion(bundleInstalled.getVersion().toString());
            }
            ((BasicManagedBundle)bundleMetadata).setTempLocalFileWhenJustUploaded(zipF);
            
            synchronized (managedBundles) {
                managedBundles.put(bundleMetadata.getId(), bundleMetadata);
            }
            mgmt.getRebindManager().getChangeListener().onChanged(bundleMetadata);
            
            // starting here  flags wiring issues earlier
            // but may break some things running from the IDE
            bundleInstalled.start();

            if (loadCatalogBom) {
                loadCatalogBom(bundleInstalled);
            }
            
            return bundleInstalled;
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw new IllegalStateException("Bundle "+bundleMetadata+" failed to install: " + Exceptions.collapseText(e), e);
        }
    }
    
    // TODO uninstall bundle, and call change listener onRemoved ?
    // TODO on snapshot install, uninstall old equivalent snapshots (items in use might stay in use though?)
    
    public synchronized Bundle registerBundle(CatalogBundle bundleMetadata) {
        try {
            Bundle alreadyBundle = checkBundleInstalledThrowIfInconsistent(bundleMetadata, true);
            if (alreadyBundle!=null) {
                return alreadyBundle;
            }

            Bundle bundleInstalled = Osgis.install(framework, bundleMetadata.getUrl());

            checkCorrectlyInstalled(bundleMetadata, bundleInstalled);
            return bundleInstalled;
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw new IllegalStateException("Bundle from "+bundleMetadata.getUrl()+" failed to install: " + e.getMessage(), e);
        }
    }

    @Beta
    // TODO this is designed to work if the FEATURE_LOAD_BUNDLE_CATALOG_BOM is disabled, the default, but unintuitive here
    // it probably works even if that is true, but we should consider what to do;
    // possibly remove that other capability, so that bundles with BOMs _have_ to be installed via this method.
    // (load order gets confusing with auto-scanning...)
    public List<? extends CatalogItem<?,?>> loadCatalogBom(Bundle bundle) {
        List<? extends CatalogItem<?, ?>> catalogItems = MutableList.of();
        loadCatalogBom(mgmt, bundle, catalogItems);
        return catalogItems;
    }
    
    private static Iterable<? extends CatalogItem<?, ?>> loadCatalogBom(ManagementContext mgmt, Bundle bundle, Iterable<? extends CatalogItem<?, ?>> catalogItems) {
        if (!BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_LOAD_BUNDLE_CATALOG_BOM)) {
            // if the above feature is not enabled, let's do it manually (as a contract of this method)
            try {
                // TODO improve on this - it ignores the configuration of whitelists, see CatalogBomScanner.
                // One way would be to add the CatalogBomScanner to the new Scratchpad area, then retrieving the singleton
                // here to get back the predicate from it.
                final Predicate<Bundle> applicationsPermitted = Predicates.<Bundle>alwaysTrue();

                catalogItems = new CatalogBundleLoader(applicationsPermitted, mgmt).scanForCatalog(bundle);
            } catch (RuntimeException ex) {
                try {
                    bundle.uninstall();
                } catch (BundleException e) {
                    log.error("Cannot uninstall bundle " + bundle.getSymbolicName() + ":" + bundle.getVersion(), e);
                }
                throw new IllegalArgumentException("Error installing catalog items", ex);
            }
        }
        return catalogItems;
    }
    
    private void checkCorrectlyInstalled(OsgiBundleWithUrl bundle, Bundle b) {
        String nv = b.getSymbolicName()+":"+b.getVersion().toString();

        if (!isBundleNameEqualOrAbsent(bundle, b)) {
            throw new IllegalStateException("Bundle already installed as "+nv+" but user explicitly requested "+bundle);
        }

        List<Bundle> matches = Osgis.bundleFinder(framework)
                .symbolicName(b.getSymbolicName())
                .version(b.getVersion().toString())
                .findAll();
        if (matches.isEmpty()) {
            log.error("OSGi could not find bundle "+nv+" in search after installing it from "+bundle);
        } else if (matches.size()==1) {
            log.debug("Bundle from "+bundle.getUrl()+" successfully installed as " + nv + " ("+b+")");
        } else {
            log.warn("OSGi has multiple bundles matching "+nv+", when installing "+bundle+"; not guaranteed which versions will be consumed");
            // TODO for snapshot versions we should indicate which one is best to use
        }
    }

    /** return already-installed bundle or null */
    private Bundle checkBundleInstalledThrowIfInconsistent(OsgiBundleWithUrl bundleMetadata, boolean requireUrlIfNotAlreadyPresent) {
        String bundleUrl = bundleMetadata.getUrl();
        if (bundleUrl != null) {
            Maybe<Bundle> installedBundle = Osgis.bundleFinder(framework).requiringFromUrl(bundleUrl).find();
            if (installedBundle.isPresent()) {
                Bundle b = installedBundle.get();
                String nv = b.getSymbolicName()+":"+b.getVersion().toString();
                if (!isBundleNameEqualOrAbsent(bundleMetadata, b)) {
                    throw new IllegalStateException("User requested bundle " + bundleMetadata + " but already installed as "+nv);
                } else {
                    log.trace("Bundle from "+bundleUrl+" already installed as "+nv+"; not re-registering");
                }
                return b;
            }
        } else {
            Maybe<Bundle> installedBundle;
            if (bundleMetadata.isNameResolved()) {
                installedBundle = Osgis.bundleFinder(framework).symbolicName(bundleMetadata.getSymbolicName()).version(bundleMetadata.getVersion()).find();
            } else {
                installedBundle = Maybe.absent("Bundle metadata does not have URL nor does it have both name and version");
            }
            if (installedBundle.isPresent()) {
                log.trace("Bundle "+bundleMetadata+" installed from "+installedBundle.get().getLocation());
            } else {
                if (requireUrlIfNotAlreadyPresent) {
                    throw new IllegalStateException("Bundle "+bundleMetadata+" not previously registered, but URL is empty.",
                        Maybe.Absent.getException(installedBundle));
                }
            }
            return installedBundle.orNull();
        }
        return null;
    }

    public static boolean isBundleNameEqualOrAbsent(OsgiBundleWithUrl bundle, Bundle b) {
        return !bundle.isNameResolved() ||
                (bundle.getSymbolicName().equals(b.getSymbolicName()) &&
                bundle.getVersion().equals(b.getVersion().toString()));
    }

    public <T> Maybe<Class<T>> tryResolveClass(String type, OsgiBundleWithUrl... osgiBundles) {
        return tryResolveClass(type, Arrays.asList(osgiBundles));
    }
    public <T> Maybe<Class<T>> tryResolveClass(String type, Iterable<? extends OsgiBundleWithUrl> osgiBundles) {
        Map<OsgiBundleWithUrl,Throwable> bundleProblems = MutableMap.of();
        Set<String> extraMessages = MutableSet.of();
        for (OsgiBundleWithUrl osgiBundle: osgiBundles) {
            try {
                Maybe<Bundle> bundle = findBundle(osgiBundle);
                if (bundle.isPresent()) {
                    Bundle b = bundle.get();
                    Optional<String> strippedType = osgiClassPrefixer.stripMatchingPrefix(b, type);
                    String typeToLoad = strippedType.isPresent() ? strippedType.get() : type;
                    if (osgiClassPrefixer.hasPrefix(typeToLoad)) {
                        bundleProblems.put(osgiBundle, new UserFacingException("Bundle does not match prefix in type name '"+typeToLoad+"'"));
                        continue;
                    }
                    //Extension bundles don't support loadClass.
                    //Instead load from the app classpath.
                    Class<T> clazz = SystemFrameworkLoader.get().loadClassFromBundle(typeToLoad, b);
                    return Maybe.of(clazz);
                } else {
                    bundleProblems.put(osgiBundle, Maybe.getException(bundle));
                }
                
            } catch (Exception e) {
                // should come from classloading now; name formatting or missing bundle errors will be caught above 
                Exceptions.propagateIfFatal(e);
                bundleProblems.put(osgiBundle, e);

                Throwable cause = e.getCause();
                if (cause != null && cause.getMessage().contains("Unresolved constraint in bundle")) {
                    if (BrooklynVersion.INSTANCE.getVersionFromOsgiManifest()==null) {
                        extraMessages.add("No brooklyn-core OSGi manifest available. OSGi will not work.");
                    }
                    if (BrooklynVersion.isDevelopmentEnvironment()) {
                        extraMessages.add("Your development environment may not have created necessary files. Doing a maven build then retrying may fix the issue.");
                    }
                    if (!extraMessages.isEmpty()) log.warn(Strings.join(extraMessages, " "));
                    log.warn("Unresolved constraint resolving OSGi bundle "+osgiBundle+" to load "+type+": "+cause.getMessage());
                    if (log.isDebugEnabled()) log.debug("Trace for OSGi resolution failure", e);
                }
            }
        }
        if (bundleProblems.size()==1) {
            Throwable error = Iterables.getOnlyElement(bundleProblems.values());
            if (error instanceof ClassNotFoundException && error.getCause()!=null && error.getCause().getMessage()!=null) {
                error = Exceptions.collapseIncludingAllCausalMessages(error);
            }
            return Maybe.absent("Unable to resolve class "+type+" in "+Iterables.getOnlyElement(bundleProblems.keySet())
                + (extraMessages.isEmpty() ? "" : " ("+Strings.join(extraMessages, " ")+")"), error);
        } else {
            return Maybe.absent(Exceptions.create("Unable to resolve class "+type+": "+bundleProblems
                + (extraMessages.isEmpty() ? "" : " ("+Strings.join(extraMessages, " ")+")"), bundleProblems.values()));
        }
    }

    public Maybe<Bundle> findBundle(OsgiBundleWithUrl catalogBundle) {
        //Either fail at install time when the user supplied name:version is different
        //from the one reported from the bundle
        //or
        //Ignore user supplied name:version when URL is supplied to be able to find the
        //bundle even if it's with a different version.
        //
        //For now we just log a warning if there's a version discrepancy at install time,
        //so prefer URL if supplied.
        BundleFinder bundleFinder = Osgis.bundleFinder(framework);
        if (catalogBundle.getUrl() != null) {
            bundleFinder.requiringFromUrl(catalogBundle.getUrl());
        } else {
            bundleFinder.symbolicName(catalogBundle.getSymbolicName()).version(catalogBundle.getVersion());
        }
        return bundleFinder.find();
    }

    /**
     * Iterates through catalogBundles until one contains a resource with the given name.
     */
    public URL getResource(String name, Iterable<? extends OsgiBundleWithUrl> osgiBundles) {
        for (OsgiBundleWithUrl osgiBundle: osgiBundles) {
            try {
                Maybe<Bundle> bundle = findBundle(osgiBundle);
                if (bundle.isPresent()) {
                    URL result = bundle.get().getResource(name);
                    if (result!=null) return result;
                }
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
            }
        }
        return null;
    }

    /**
     * @return URL's to all resources matching the given name (using {@link Bundle#getResources(String)} in the referenced osgi bundles.
     */
    public Iterable<URL> getResources(String name, Iterable<? extends OsgiBundleWithUrl> osgiBundles) {
        Set<URL> resources = Sets.newLinkedHashSet();
        for (OsgiBundleWithUrl catalogBundle : osgiBundles) {
            try {
                Maybe<Bundle> bundle = findBundle(catalogBundle);
                if (bundle.isPresent()) {
                    Enumeration<URL> result = bundle.get().getResources(name);
                    resources.addAll(Collections.list(result));
                }
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
            }
        }
        return resources;
    }

    public Framework getFramework() {
        return framework;
    }

}
