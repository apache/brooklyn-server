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
package org.apache.brooklyn.core.catalog.internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.mgmt.rebind.RebindExceptionHandler;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.ManagementContextInjectable;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.FatalRuntimeException;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

@Beta
public class CatalogInitialization implements ManagementContextInjectable {

    /*

    A1) if not persisting, go to B1
    A2) if there is a persisted catalog, read it and go to C1
    A3) go to B1

    B1) look for --catalog-initial, if so read it, then go to C1
    B2) look for BrooklynServerConfig.BROOKLYN_CATALOG_URL, if so, read it, supporting YAML, then go to C1
    B3) look for ~/.brooklyn/catalog.bom, if exists, read it then go to C1
    B4) read all classpath://brooklyn/default.catalog.bom items, if they exist (and for now they will)
    B5) go to C1

    C1) if persisting, read the rest of the persisted items (entities etc)

     */

    private static final Logger log = LoggerFactory.getLogger(CatalogInitialization.class);
    
    private String initialUri;

    private boolean disallowLocal = false;
    private List<Function<CatalogInitialization, Void>> callbacks = MutableList.of();
    /** has run an unofficial initialization (i.e. an early load, triggered by an early read of the catalog) */
    private boolean hasRunUnofficialInitialization = false; 
    /** has run an official initialization, but it is not a permanent one (e.g. during a hot standby mode, or a run failed) */
    private boolean hasRunTransientOfficialInitialization = false; 
    /** has run an official initialization which is permanent (node is master, and the new catalog is now set) */
    private boolean hasRunFinalInitialization = false;
    /** is running a populate method; used to prevent recursive loops */
    private boolean isPopulating = false;
    
    private ManagementContextInternal managementContext;
    private boolean isStartingUp = false;
    private boolean failOnStartupErrors = false;
    
    private Object populatingCatalogMutex = new Object();
    
    public CatalogInitialization() {
        this(null);
    }

    public CatalogInitialization(String initialUri) {
        this.initialUri = initialUri;
    }
    
    @Override
    public void setManagementContext(ManagementContext managementContext) {
        Preconditions.checkNotNull(managementContext, "management context");
        if (this.managementContext!=null && managementContext!=this.managementContext)
            throw new IllegalStateException("Cannot switch management context, from "+this.managementContext+" to "+managementContext);
        this.managementContext = (ManagementContextInternal) managementContext;
    }
    
    /** Called by the framework to set true while starting up, and false afterwards,
     * in order to assist in appropriate logging and error handling. */
    public void setStartingUp(boolean isStartingUp) {
        this.isStartingUp = isStartingUp;
    }

    public void setFailOnStartupErrors(boolean startupFailOnCatalogErrors) {
        this.failOnStartupErrors = startupFailOnCatalogErrors;
    }

    public CatalogInitialization addPopulationCallback(Function<CatalogInitialization, Void> callback) {
        callbacks.add(callback);
        return this;
    }
    
    public ManagementContextInternal getManagementContext() {
        return Preconditions.checkNotNull(managementContext, "management context has not been injected into "+this);
    }

    /** Returns true if the canonical initialization has completed, 
     * that is, an initialization which is done when a node is rebinded as master
     * (or an initialization done by the startup routines when not running persistence);
     * see also {@link #hasRunAnyInitialization()}. */
    public boolean hasRunFinalInitialization() { return hasRunFinalInitialization; }
    
    /** Returns true if an official initialization has run,
     * even if it was a transient run, e.g. so that the launch sequence can tell whether rebind has triggered initialization */
    public boolean hasRunOfficialInitialization() { return hasRunFinalInitialization || hasRunTransientOfficialInitialization; }
    
    /** Returns true if the initializer has run at all,
     * including transient initializations which might be needed before a canonical becoming-master rebind,
     * for instance because the catalog is being accessed before loading rebind information
     * (done by {@link #populateUnofficial(BasicBrooklynCatalog)}) */
    public boolean hasRunAnyInitialization() { return hasRunFinalInitialization || hasRunTransientOfficialInitialization || hasRunUnofficialInitialization; }

    /**
     * This method will almost certainly be changed. It is an interim step, to move the catalog-initialization
     * decisions and logic out of {@link org.apache.brooklyn.core.mgmt.rebind.RebindIteration} and into 
     * a single place. We can then make smarter decisions about what to do with the persisted state's catalog.
     * 
     * @param mode
     * @param persistedState
     * @param exceptionHandler
     * @param rebindLogger
     */
    @Beta
    public void populateCatalog(ManagementNodeState mode, PersistedCatalogState persistedState, RebindExceptionHandler exceptionHandler, RebindLogger rebindLogger) {
        // Always installing the bundles from persisted state
        installBundles(mode, persistedState, exceptionHandler, rebindLogger);
        
        // Decide whether to add the persisted catalog items, or to use the "initial items".
        // Logic copied (unchanged) from RebindIteration.installBundlesAndRebuildCatalog.
        boolean isEmpty = persistedState.isEmpty();
        Collection<CatalogItem<?,?>> itemsForResettingCatalog = null;
        boolean needsInitialItemsLoaded;
        boolean needsAdditionalItemsLoaded;
        if (!isEmpty) {
            rebindLogger.debug("RebindManager clearing local catalog and loading from persisted state");
            itemsForResettingCatalog = persistedState.getLegacyCatalogItems();
            needsInitialItemsLoaded = false;
            // only apply "add" if we haven't yet done so while in MASTER mode
            needsAdditionalItemsLoaded = !hasRunFinalInitialization();
        } else {
            if (hasRunFinalInitialization()) {
                rebindLogger.debug("RebindManager has already done the final official run, not doing anything (even though persisted state empty)");
                needsInitialItemsLoaded = false;
                needsAdditionalItemsLoaded = false;
            } else {
                rebindLogger.debug("RebindManager loading initial catalog locally because persisted state empty and the final official run has not yet been performed");
                needsInitialItemsLoaded = true;
                needsAdditionalItemsLoaded = true;
            }
        }

        // TODO in read-only mode, perhaps do this less frequently than entities etc, maybe only if things change?
        populateCatalog(mode, needsInitialItemsLoaded, needsAdditionalItemsLoaded, itemsForResettingCatalog);
    }
    
    /** makes or updates the mgmt catalog, based on the settings in this class 
     * @param nodeState the management node for which this is being read; if master, then we expect this run to be the last one,
     *   and so subsequent applications should ignore any initialization data (e.g. on a subsequent promotion to master, 
     *   after a master -> standby -> master cycle)
     * @param needsInitialItemsLoaded whether the catalog needs the initial items loaded
     * @param needsAdditionalItemsLoaded whether the catalog needs the additions loaded
     * @param optionalExplicitItemsForResettingCatalog
     *   if supplied, the catalog is reset to contain only these items, before calling any other initialization
     *   for use primarily when rebinding
     */
    public void populateCatalog(ManagementNodeState nodeState, boolean needsInitialItemsLoaded, boolean needsAdditionsLoaded, Collection<CatalogItem<?, ?>> optionalExplicitItemsForResettingCatalog) {
        if (log.isDebugEnabled()) {
            String message = "Populating catalog for "+nodeState+", needsInitial="+needsInitialItemsLoaded+", needsAdditional="+needsAdditionsLoaded+", explicitItems="+(optionalExplicitItemsForResettingCatalog==null ? "null" : optionalExplicitItemsForResettingCatalog.size())+"; from "+JavaClassNames.callerNiceClassAndMethod(1);
            if (!ManagementNodeState.isHotProxy(nodeState)) {
                log.debug(message);
            } else {
                // in hot modes, make this message trace so we don't get too much output then
                log.trace(message);
            }
        }
        synchronized (populatingCatalogMutex) {
            try {
                if (hasRunFinalInitialization() && (needsInitialItemsLoaded || needsAdditionsLoaded)) {
                    // if we have already run "final" then we should only ever be used to reset the catalog, 
                    // not to initialize or add; e.g. we are being given a fixed list on a subsequent master rebind after the initial master rebind 
                    log.warn("Catalog initialization called to populate initial, even though it has already run the final official initialization");
                }
                isPopulating = true;
                BasicBrooklynCatalog catalog = (BasicBrooklynCatalog) managementContext.getCatalog();
                if (!catalog.getCatalog().isLoaded()) {
                    catalog.load();
                } else {
                    if (needsInitialItemsLoaded && hasRunAnyInitialization()) {
                        // an indication that something caused it to load early; not severe, but unusual
                        if (hasRunTransientOfficialInitialization) {
                            log.debug("Catalog initialization now populating, but has noted a previous official run which was not final (probalby loaded while in a standby mode, or a previous run failed); overwriting any items installed earlier");
                        } else {
                            log.warn("Catalog initialization now populating, but has noted a previous unofficial run (it may have been an early web request); overwriting any items installed earlier");
                        }
                        catalog.reset(ImmutableList.<CatalogItem<?,?>>of());
                    }
                }

                populateCatalogImpl(catalog, needsInitialItemsLoaded, needsAdditionsLoaded, optionalExplicitItemsForResettingCatalog);
                if (nodeState == ManagementNodeState.MASTER) {
                    // TODO ideally this would remain false until it has *persisted* the changed catalog;
                    // if there is a subsequent startup failure the forced additions will not be persisted,
                    // but nor will they be loaded on a subsequent run.
                    // callers will have to restart a brooklyn, or reach into this class to change this field,
                    // or (recommended) manually adjust the catalog.
                    // TODO also, if a node comes up in standby, the addition might not take effector for a while
                    //
                    // however since these options are mainly for use on the very first brooklyn run, it's not such a big deal; 
                    // once up and running the typical way to add items is via the REST API
                    hasRunFinalInitialization = true;
                }
            } catch (Throwable e) {
                log.warn("Error populating catalog (rethrowing): "+e, e);
                throw Exceptions.propagate(e);
            } finally {
                if (!hasRunFinalInitialization) {
                    hasRunTransientOfficialInitialization = true;
                }
                isPopulating = false;
            }
        }
    }

    private void populateCatalogImpl(BasicBrooklynCatalog catalog, boolean needsInitialItemsLoaded, boolean needsAdditionsLoaded, Collection<CatalogItem<?, ?>> optionalItemsForResettingCatalog) {
        if (optionalItemsForResettingCatalog!=null) {
            catalog.reset(optionalItemsForResettingCatalog);
        }
        
        if (needsInitialItemsLoaded) {
            populateInitial(catalog);
        }

        if (needsAdditionsLoaded) {
            populateViaCallbacks(catalog);
        }
    }

    protected void populateInitial(BasicBrooklynCatalog catalog) {
        if (disallowLocal) {
            if (!hasRunFinalInitialization()) {
                log.debug("CLI initial catalog not being read when local catalog load mode is disallowed.");
            }
            return;
        }

//        B1) look for --catalog-initial, if so read it, then go to C1
//        B2) look for BrooklynServerConfig.BROOKLYN_CATALOG_URL, if so, read it, supporting YAML, then go to C1
//        B3) look for ~/.brooklyn/catalog.bom, if exists, read it then go to C1
//        B4) read all classpath://brooklyn/default.catalog.bom items, if they exist (and for now they will)
//        B5) go to C1

        if (initialUri!=null) {
            populateInitialFromUri(catalog, initialUri);
            return;
        }
        
        String catalogUrl = managementContext.getConfig().getConfig(BrooklynServerConfig.BROOKLYN_CATALOG_URL);
        if (Strings.isNonBlank(catalogUrl)) {
            populateInitialFromUri(catalog, catalogUrl);
            return;
        }
        
        catalogUrl = Os.mergePaths(BrooklynServerConfig.getMgmtBaseDir( managementContext.getConfig() ), "catalog.bom");
        if (new File(catalogUrl).exists()) {
            populateInitialFromUri(catalog, new File(catalogUrl).toURI().toString());
            return;
        }
        
        // otherwise look for for classpath:/brooklyn/default.catalog.bom --
        // there is one on the classpath which says to scan, and provides a few templates;
        // if one is supplied by user in the conf/ dir that will override the item from the classpath
        // (TBD - we might want to scan for all such bom's?)
        
        catalogUrl = "classpath:/brooklyn/default.catalog.bom";
        if (new ResourceUtils(this).doesUrlExist(catalogUrl)) {
            populateInitialFromUri(catalog, catalogUrl);
            return;
        }
        
        log.info("No catalog found on classpath or specified; catalog will not be initialized.");
        return;
    }
    
    private void populateInitialFromUri(BasicBrooklynCatalog catalog, String catalogUrl) {
        log.debug("Loading initial catalog from {}", catalogUrl);

        try {
            String contents = new ResourceUtils(this).getResourceAsString(catalogUrl);

            catalog.reset(MutableList.<CatalogItem<?,?>>of());
            Object result = catalog.addItems(contents);
            
            log.debug("Loaded initial catalog from {}: {}", catalogUrl, result);
            
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.warn("Error importing catalog from " + catalogUrl + ": " + e, e);
        }
    }

    protected void populateViaCallbacks(BasicBrooklynCatalog catalog) {
        for (Function<CatalogInitialization, Void> callback: callbacks)
            callback.apply(this);
    }

    /** Creates the catalog based on parameters set here, if not yet loaded,
     * but ignoring persisted state and warning if persistence is on and we are starting up
     * (because the official persistence is preferred and the catalog will be subsequently replaced);
     * for use when the catalog is accessed before persistence is completed. 
     * <p>
     * This method is primarily used during testing, which in many cases does not enforce the full startup order
     * and which wants a local catalog in any case. It may also be invoked if a client requests the catalog
     * while the server is starting up. */
    public void populateUnofficial(BasicBrooklynCatalog catalog) {
        synchronized (populatingCatalogMutex) {
            // check isPopulating in case this method gets called from inside another populate call
            if (hasRunAnyInitialization() || isPopulating) return;
            log.debug("Populating catalog unofficially ("+catalog+")");
            isPopulating = true;
            try {
                if (isStartingUp) {
                    log.warn("Catalog access requested when not yet initialized; populating best effort rather than through recommended pathway. Catalog data may be replaced subsequently.");
                }
                populateCatalogImpl(catalog, true, true, null);
            } finally {
                hasRunUnofficialInitialization = true;
                isPopulating = false;
            }
        }
    }

    public void handleException(Throwable throwable, Object details) {
        if (throwable instanceof InterruptedException)
            throw new RuntimeInterruptedException((InterruptedException) throwable);
        if (throwable instanceof RuntimeInterruptedException)
            throw (RuntimeInterruptedException) throwable;

        if (details instanceof CatalogItem) {
            if (((CatalogItem<?,?>)details).getCatalogItemId() != null) {
                details = ((CatalogItem<?,?>)details).getCatalogItemId();
            }
        }
        PropagatedRuntimeException wrap = new PropagatedRuntimeException("Error loading catalog item "+details, throwable);
        log.warn(Exceptions.collapseText(wrap));
        log.debug("Trace for: "+wrap, wrap);

        getManagementContext().errors().add(wrap);
        
        if (isStartingUp && failOnStartupErrors) {
            throw new FatalRuntimeException("Unable to load catalog item "+details, wrap);
        }
    }
    
    private void installBundles(ManagementNodeState mode, PersistedCatalogState persistedState, RebindExceptionHandler exceptionHandler, RebindLogger rebindLogger) {
        List<OsgiBundleInstallationResult> installs = MutableList.of();

        // Install the bundles
        for (String bundleId : persistedState.getBundleIds()) {
            rebindLogger.debug("RebindManager installing bundle {}", bundleId);
            InstallableManagedBundle installableBundle = persistedState.getInstallableManagedBundle(bundleId);
            try (InputStream in = installableBundle.getInputStream()) {
                installs.add(installBundle(installableBundle.getManagedBundle(), in));
            } catch (Exception e) {
                exceptionHandler.onCreateFailed(BrooklynObjectType.MANAGED_BUNDLE, bundleId, installableBundle.getManagedBundle().getSymbolicName(), e);
            }
        }
        
        // Start the bundles (now that we've installed them all)
        Set<RegisteredType> installedTypes = MutableSet.of();
        for (OsgiBundleInstallationResult br: installs) {
            try {
                startBundle(br);
                Iterables.addAll(installedTypes, managementContext.getTypeRegistry().getMatching(
                    RegisteredTypePredicates.containingBundle(br.getVersionedName())));
            } catch (Exception e) {
                exceptionHandler.onCreateFailed(BrooklynObjectType.MANAGED_BUNDLE, br.getMetadata().getId(), br.getMetadata().getSymbolicName(), e);
            }
        }
        
        // Validate that they all started successfully
        if (!installedTypes.isEmpty()) {
            validateAllTypes(installedTypes, exceptionHandler);
        }
    }
    
    private void validateAllTypes(Set<RegisteredType> installedTypes, RebindExceptionHandler exceptionHandler) {
        Stopwatch sw = Stopwatch.createStarted();
        log.debug("Getting catalog to validate all types");
        final BrooklynCatalog catalog = this.managementContext.getCatalog();
        log.debug("Got catalog in {} now validate", sw.toString());
        sw.reset(); sw.start();
        Map<RegisteredType, Collection<Throwable>> validationErrors = catalog.validateTypes( installedTypes );
        log.debug("Validation done in {}", sw.toString());
        if (!validationErrors.isEmpty()) {
            Map<VersionedName, Map<RegisteredType,Collection<Throwable>>> errorsByBundle = MutableMap.of();
            for (RegisteredType t: validationErrors.keySet()) {
                VersionedName vn = VersionedName.fromString(t.getContainingBundle());
                Map<RegisteredType, Collection<Throwable>> errorsInBundle = errorsByBundle.get(vn);
                if (errorsInBundle==null) {
                    errorsInBundle = MutableMap.of();
                    errorsByBundle.put(vn, errorsInBundle);
                }
                errorsInBundle.put(t, validationErrors.get(t));
            }
            for (VersionedName vn: errorsByBundle.keySet()) {
                Map<RegisteredType, Collection<Throwable>> errorsInBundle = errorsByBundle.get(vn);
                ManagedBundle b = managementContext.getOsgiManager().get().getManagedBundle(vn);
                String id = b!=null ? b.getId() : /* just in case it was uninstalled concurrently somehow */ vn.toString();
                exceptionHandler.onCreateFailed(BrooklynObjectType.MANAGED_BUNDLE,
                    id,
                    vn.getSymbolicName(),
                    Exceptions.create("Failed to install "+vn+", types "+errorsInBundle.keySet()+" gave errors",
                        Iterables.concat(errorsInBundle.values())));
            }
        }
    }

    /** install the bundles into brooklyn and osgi, but do not start nor validate;
     * caller (rebind) will do that manually, doing each step across all bundles before proceeding 
     * to prevent reference errors */
    public OsgiBundleInstallationResult installBundle(ManagedBundle bundle, InputStream zipInput) {
        return getManagementContext().getOsgiManager().get().installDeferredStart(bundle, zipInput, false).get();
    }
    
    public void startBundle(OsgiBundleInstallationResult br) throws BundleException {
        if (br.getDeferredStart()!=null) {
            br.getDeferredStart().run();
        }
    }

    public interface RebindLogger {
        void debug(String message, Object... args);
    }
    
    public interface InstallableManagedBundle {
        public ManagedBundle getManagedBundle();
        /** The caller is responsible for closing the returned stream. */
        public InputStream getInputStream() throws IOException;
    }
    
    public interface PersistedCatalogState {

        /**
         * Whether the persisted state is entirely empty.
         */
        public boolean isEmpty();

        /**
         * The persisted catalog items (from the {@code /catalog} sub-directory of the persisted state).
         */
        public Collection<CatalogItem<?,?>> getLegacyCatalogItems();
        
        /**
         * The persisted bundles (from the {@code /bundles} sub-directory of the persisted state).
         */
        public Set<String> getBundleIds();
        
        /**
         * Loads the details of a particular bundle, so it can be installed.
         */
        public InstallableManagedBundle getInstallableManagedBundle(String id);
    }
}
