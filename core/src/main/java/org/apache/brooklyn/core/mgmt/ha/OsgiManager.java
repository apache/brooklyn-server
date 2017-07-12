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
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.catalog.internal.CatalogBundleLoader;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.core.osgi.Osgis.BundleFinder;
import org.apache.brooklyn.util.core.osgi.SystemFrameworkLoader;
import org.apache.brooklyn.util.core.xstream.OsgiClassPrefixer;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.os.Os.DeletionResult;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class OsgiManager {

    private static final Logger log = LoggerFactory.getLogger(OsgiManager.class);
    
    public static final ConfigKey<Boolean> USE_OSGI = BrooklynServerConfig.USE_OSGI;
    public static final ConfigKey<Boolean> REUSE_OSGI = ConfigKeys.newBooleanConfigKey("brooklyn.osgi.reuse",
        "Whether the OSGi container can reuse a previous one and itself can be reused, defaulting to false, "
        + "often overridden in tests for efficiency (and will ignore the cache dir)", false);
    
    /** The {@link Framework#start()} event is the most expensive one; in fact a restart seems to be _more_ expensive than
     * a start from scratch; however if we leave it running, uninstalling any extra bundles, then tests are fast and don't leak.
     * See OsgiTestingLeaksAndSpeedTest. */
    protected static final boolean REUSED_FRAMEWORKS_ARE_KEPT_RUNNING = true;
    
    /* see `Osgis` class for info on starting framework etc */
    
    final ManagementContext mgmt;
    final OsgiClassPrefixer osgiClassPrefixer;
    Framework framework;
    
    private boolean reuseFramework;
    private Set<Bundle> bundlesAtStartup;
    private File osgiCacheDir;
    final ManagedBundlesRecord managedBundlesRecord = new ManagedBundlesRecord();
    final Map<VersionedName,ManagedBundle> wrapperBundles = MutableMap.of();
    
    static class ManagedBundlesRecord {
        private Map<String, ManagedBundle> managedBundlesByUid = MutableMap.of();
        private Map<VersionedName, String> managedBundlesUidByVersionedName = MutableMap.of();
        private Map<String, String> managedBundlesUidByUrl = MutableMap.of();
        
        synchronized Map<String, ManagedBundle> getManagedBundles() {
            return ImmutableMap.copyOf(managedBundlesByUid);
        }

        synchronized String getManagedBundleId(VersionedName vn) {
            return managedBundlesUidByVersionedName.get(VersionedName.toOsgiVersionedName(vn));
        }

        synchronized ManagedBundle getManagedBundle(VersionedName vn) {
            return managedBundlesByUid.get(managedBundlesUidByVersionedName.get(VersionedName.toOsgiVersionedName(vn)));
        }
        
        synchronized String getManagedBundleIdFromUrl(String url) {
            return managedBundlesUidByUrl.get(url);
        }
        
        synchronized ManagedBundle getManagedBundleFromUrl(String url) {
            String id = getManagedBundleIdFromUrl(url);
            if (id==null) return null;
            return managedBundlesByUid.get(id);
        }

        synchronized void setManagedBundleUrl(String url, String id) {
            managedBundlesUidByUrl.put(url, id);    
        }
        
        synchronized void addManagedBundle(OsgiBundleInstallationResult result) {
            managedBundlesByUid.put(result.getMetadata().getId(), result.getMetadata());
            managedBundlesUidByVersionedName.put(VersionedName.toOsgiVersionedName(result.getMetadata().getVersionedName()), 
                result.getMetadata().getId());
            if (Strings.isNonBlank(result.getMetadata().getUrl())) {
                managedBundlesUidByUrl.put(result.getMetadata().getUrl(), result.getMetadata().getId());
            }
        }
    }
    
    private static AtomicInteger numberOfReusableFrameworksCreated = new AtomicInteger();
    private static final List<Framework> OSGI_FRAMEWORK_CONTAINERS_FOR_REUSE = MutableList.of();
    
    public OsgiManager(ManagementContext mgmt) {
        this.mgmt = mgmt;
        this.osgiClassPrefixer = new OsgiClassPrefixer();
    }

    public void start() {
        if (framework!=null) {
            throw new IllegalStateException("OSGi framework already set in this management context");
        }
        
        try {
            if (mgmt.getConfig().getConfig(REUSE_OSGI)) {
                reuseFramework = true;
                
                synchronized (OSGI_FRAMEWORK_CONTAINERS_FOR_REUSE) {
                    if (!OSGI_FRAMEWORK_CONTAINERS_FOR_REUSE.isEmpty()) {
                        framework = OSGI_FRAMEWORK_CONTAINERS_FOR_REUSE.remove(0);
                    }
                }
                if (framework!=null) {
                    if (!REUSED_FRAMEWORKS_ARE_KEPT_RUNNING) {
                        // don't think we need to do 'init'
//                        framework.init();
                        framework.start();
                    }
                    
                    log.debug("Reusing OSGi framework container from "+framework.getBundleContext().getProperty(Constants.FRAMEWORK_STORAGE)+" for mgmt node "+mgmt.getManagementNodeId());
                    
                    return;
                }
                osgiCacheDir = Os.newTempDir("brooklyn-osgi-reusable-container");
                Os.deleteOnExitRecursively(osgiCacheDir);
                if (numberOfReusableFrameworksCreated.incrementAndGet()%10==0) {
                    log.warn("Possible leak of reusable OSGi containers ("+numberOfReusableFrameworksCreated+" total)");
                }
                
            } else {
                osgiCacheDir = BrooklynServerPaths.getOsgiCacheDirCleanedIfNeeded(mgmt);
            }
            
            // any extra OSGi startup args could go here
            framework = Osgis.getFramework(osgiCacheDir.getAbsolutePath(), false);
            log.debug("OSGi framework container created in "+osgiCacheDir+" mgmt node "+mgmt.getManagementNodeId()+
                (reuseFramework ? "(reusable, "+numberOfReusableFrameworksCreated.get()+" total)" : "") );
            
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } finally {
            if (reuseFramework) {
                bundlesAtStartup = MutableSet.copyOf(Arrays.asList(framework.getBundleContext().getBundles()));
            }
        }
    }

    public void stop() {
        if (reuseFramework) {
            for (Bundle b: framework.getBundleContext().getBundles()) {
                if (!bundlesAtStartup.contains(b)) {
                    try {
                        log.info("Uninstalling "+b+" from OSGi container in "+framework.getBundleContext().getProperty(Constants.FRAMEWORK_STORAGE));
                        b.uninstall();
                    } catch (BundleException e) {
                        Exceptions.propagateIfFatal(e);
                        log.warn("Unable to uninstall "+b+"; container in "+framework.getBundleContext().getProperty(Constants.FRAMEWORK_STORAGE)+" will not be reused: "+e, e);
                        reuseFramework = false;
                        break;
                    }
                }
            }
        }
        
        if (!reuseFramework || !REUSED_FRAMEWORKS_ARE_KEPT_RUNNING) {
            Osgis.ungetFramework(framework);
        }
        
        if (reuseFramework) {
            synchronized (OSGI_FRAMEWORK_CONTAINERS_FOR_REUSE) {
                OSGI_FRAMEWORK_CONTAINERS_FOR_REUSE.add(framework);
            }
            
        } else if (BrooklynServerPaths.isOsgiCacheForCleaning(mgmt, osgiCacheDir)) {
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

    /** Map of bundles by UID */
    public Map<String, ManagedBundle> getManagedBundles() {
        return managedBundlesRecord.getManagedBundles();
    }

    /** Gets UID given a name, or null */
    public String getManagedBundleId(VersionedName vn) {
        return managedBundlesRecord.getManagedBundleId(vn);
    }
    
    public ManagedBundle getManagedBundle(VersionedName vn) {
        return managedBundlesRecord.getManagedBundle(vn);
    }

    /** For bundles which are installed by a URL, see whether a bundle has been installed from that URL */
    public ManagedBundle getManagedBundleFromUrl(String url) {
        return managedBundlesRecord.getManagedBundleFromUrl(url);
    }
    
    /** See {@link OsgiArchiveInstaller#install()}, using default values */
    public ReferenceWithError<OsgiBundleInstallationResult> install(InputStream zipIn) {
        return new OsgiArchiveInstaller(this, null, zipIn).install();
    }

    /** See {@link OsgiArchiveInstaller#install()}, but deferring the start and catalog load */
    public ReferenceWithError<OsgiBundleInstallationResult> installDeferredStart(@Nullable ManagedBundle knownBundleMetadata, @Nullable InputStream zipIn, boolean validateTypes) {
        OsgiArchiveInstaller installer = new OsgiArchiveInstaller(this, knownBundleMetadata, zipIn);
        installer.setDeferredStart(true);
        installer.setValidateTypes(validateTypes);
        
        return installer.install();
    }
    
    /** See {@link OsgiArchiveInstaller#install()} - this exposes custom options */
    @Beta
    public ReferenceWithError<OsgiBundleInstallationResult> install(@Nullable ManagedBundle knownBundleMetadata, @Nullable InputStream zipIn,
            boolean start, boolean loadCatalogBom, boolean forceUpdateOfNonSnapshots) {
        
        log.debug("Installing bundle from stream - known details: "+knownBundleMetadata);
        
        OsgiArchiveInstaller installer = new OsgiArchiveInstaller(this, knownBundleMetadata, zipIn);
        installer.setStart(start);
        installer.setLoadCatalogBom(loadCatalogBom);
        installer.setForce(forceUpdateOfNonSnapshots);
        
        return installer.install();
    }
    
    /**
     * Removes this bundle from Brooklyn management, 
     * removes all catalog items it defined,
     * and then uninstalls the bundle from OSGi.
     * <p>
     * No checking is done whether anything is using the bundle;
     * behaviour of such things is not guaranteed. They will work for many things
     * but attempts to load new classes may fail.
     * <p>
     * Callers should typically fail if anything from this bundle is in use.
     */
    public void uninstallUploadedBundle(ManagedBundle bundleMetadata) {
        synchronized (managedBundlesRecord) {
            ManagedBundle metadata = managedBundlesRecord.managedBundlesByUid.remove(bundleMetadata.getId());
            if (metadata==null) {
                throw new IllegalStateException("No such bundle registered: "+bundleMetadata);
            }
            managedBundlesRecord.managedBundlesUidByVersionedName.remove(bundleMetadata.getVersionedName());
            managedBundlesRecord.managedBundlesUidByUrl.remove(bundleMetadata.getUrl());
            removeInstalledWrapperBundle(bundleMetadata);
        }
        mgmt.getRebindManager().getChangeListener().onUnmanaged(bundleMetadata);

        uninstallCatalogItemsFromBundle( bundleMetadata.getVersionedName() );
        
        Bundle bundle = framework.getBundleContext().getBundle(bundleMetadata.getOsgiUniqueUrl());
        if (bundle==null) {
            throw new IllegalStateException("No such bundle installed: "+bundleMetadata);
        }
        try {
            bundle.stop();
            bundle.uninstall();
        } catch (BundleException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Beta
    public void uninstallCatalogItemsFromBundle(VersionedName bundle) {
        List<RegisteredType> thingsFromHere = ImmutableList.copyOf(getTypesFromBundle( bundle ));
        log.debug("Uninstalling items from bundle "+bundle+": "+thingsFromHere);
        for (RegisteredType t: thingsFromHere) {
            mgmt.getCatalog().deleteCatalogItem(t.getSymbolicName(), t.getVersion());
        }
    }

    @Beta
    public Iterable<RegisteredType> getTypesFromBundle(final VersionedName vn) {
        return mgmt.getTypeRegistry().getMatching(RegisteredTypePredicates.containingBundle(vn));
    }
    
    /** @deprecated since 0.12.0 use {@link #install(ManagedBundle, InputStream, boolean, boolean)} */
    @Deprecated
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
    public List<? extends CatalogItem<?,?>> loadCatalogBomLegacy(Bundle bundle) {
        return loadCatalogBomLegacy(bundle, false);
    }
    
    @Beta  // as above
    public List<? extends CatalogItem<?,?>> loadCatalogBomLegacy(Bundle bundle, boolean force) {
        return MutableList.copyOf(loadCatalogBomInternal(mgmt, bundle, force, true, true));
    }
    
    // since 0.12.0 no longer returns items; it installs non-persisted RegisteredTypes to the type registry instead 
    @Beta
    public void loadCatalogBom(Bundle bundle, boolean force, boolean validate) {
        loadCatalogBomInternal(mgmt, bundle, force, validate, false);
    }
    
    private static Iterable<? extends CatalogItem<?, ?>> loadCatalogBomInternal(ManagementContext mgmt, Bundle bundle, boolean force, boolean validate, boolean legacy) {
        Iterable<? extends CatalogItem<?, ?>> catalogItems = MutableList.of();

        try {
            final Predicate<Bundle> applicationsPermitted = Predicates.<Bundle>alwaysTrue();

            CatalogBundleLoader cl = new CatalogBundleLoader(applicationsPermitted, mgmt);
            if (legacy) {
                catalogItems = cl.scanForCatalogLegacy(bundle, force);
            } else {
                cl.scanForCatalog(bundle, force, validate);
                catalogItems = null;
            }
        } catch (RuntimeException ex) {
            // TODO confirm -- as of May 2017 we no longer uninstall the bundle if install of catalog items fails;
            // caller needs to upgrade, or uninstall then reinstall
            // (this uninstall wouldn't have unmanaged it in brooklyn in any case)
//                try {
//                    bundle.uninstall();
//                } catch (BundleException e) {
//                    log.error("Cannot uninstall bundle " + bundle.getSymbolicName() + ":" + bundle.getVersion()+" (after error installing catalog items)", e);
//                }
            throw new IllegalArgumentException("Error installing catalog items", ex);
        }
            
        return catalogItems;
    }
    
    void checkCorrectlyInstalled(OsgiBundleWithUrl bundle, Bundle b) {
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
                installedBundle = Osgis.bundleFinder(framework).symbolicName(bundleMetadata.getSymbolicName()).version(bundleMetadata.getSuppliedVersionString()).find();
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
                bundle.getOsgiVersionString().equals(b.getVersion().toString()));
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
        // Prefer OSGi Location as URL or the managed bundle recorded URL,
        // not bothering to check name:version if supplied here (eg to forgive snapshot version discrepancies);
        // but fall back to name/version if URL is not known.
        // Version checking may be stricter at install time.
        Maybe<Bundle> result = null;
        if (catalogBundle.getUrl() != null) {
            BundleFinder bundleFinder = Osgis.bundleFinder(framework);
            bundleFinder.requiringFromUrl(catalogBundle.getUrl());
            result = bundleFinder.find();
            if (result.isPresent()) {
                return result;
            }
            
            ManagedBundle mb = getManagedBundleFromUrl(catalogBundle.getUrl());
            if (mb!=null) {
                bundleFinder.requiringFromUrl(null);
                bundleFinder.symbolicName(mb.getSymbolicName()).version(mb.getSuppliedVersionString());
                result = bundleFinder.find();
                if (result.isPresent()) {
                    return result;
                }
            }
        }

        if (catalogBundle.getSymbolicName()!=null) {
            BundleFinder bundleFinder = Osgis.bundleFinder(framework);
            bundleFinder.symbolicName(catalogBundle.getSymbolicName()).version(catalogBundle.getSuppliedVersionString());
            return bundleFinder.find();
        }
        if (result!=null) {
            return result;
        }
        return Maybe.absent("Insufficient information in "+catalogBundle+" to find bundle");
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

    // track wrapper bundles lifecvcle specially, to avoid removing it while it's installing
    public void addInstalledWrapperBundle(ManagedBundle mb) {
        synchronized (wrapperBundles) {
            wrapperBundles.put(mb.getVersionedName(), mb);
        }
    }
    public void removeInstalledWrapperBundle(ManagedBundle mb) {
        synchronized (wrapperBundles) {
            wrapperBundles.remove(mb.getVersionedName());
        }
    }
    public Collection<ManagedBundle> getInstalledWrapperBundles() {
        synchronized (wrapperBundles) {
            return MutableSet.copyOf(wrapperBundles.values());
        }
    }

}
