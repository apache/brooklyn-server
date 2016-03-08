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
package org.apache.brooklyn.core;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import groovy.lang.GroovyClassLoader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeoutException;
import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityManager;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.mgmt.ha.ManagementPlaneSyncRecordPersister;
import org.apache.brooklyn.api.mgmt.rebind.PersistenceExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.ha.HighAvailabilityManagerImpl;
import org.apache.brooklyn.core.mgmt.ha.ManagementPlaneSyncRecordPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.internal.BrooklynShutdownHooks;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.BrooklynPersistenceUtils;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore;
import org.apache.brooklyn.core.mgmt.rebind.PersistenceExceptionHandlerImpl;
import org.apache.brooklyn.core.mgmt.rebind.RebindManagerImpl;
import org.apache.brooklyn.core.objs.BrooklynTypes;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.FatalConfigurationRuntimeException;
import org.apache.brooklyn.util.exceptions.FatalRuntimeException;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.io.FileUtil;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializer for brooklyn-core when running in an OSGi environment.
 *
 * Temporarily here; should be totally contained in blueprint beans' init-methods.
 */
public class OsgiLauncher {

    private static final Logger log = LoggerFactory.getLogger(OsgiLauncher.class);

    private ManagementContext managementContext;

    private boolean ignoreCatalogErrors = true;
    private boolean ignorePersistenceErrors = true;

    private HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.DISABLED;

    private PersistMode persistMode = PersistMode.DISABLED;
    private String persistenceDir = null;
    private String persistenceLocation = null;
    private Duration persistPeriod = Duration.ONE_SECOND;

    public void start() {
        // Configure launcher
//        ShutdownHandler shutdownHandler = new AppShutdownHandler();
        try {
//            initBrooklynProperties();
            initCatalog();

            // TODO
//            launcher.stopWhichAppsOnShutdown(stopWhichAppsOnShutdownMode);
//            launcher.shutdownHandler(shutdownHandler);
//
//            computeAndSetApp(launcher, utils, loader);
//            customize(launcher);
//        } catch (FatalConfigurationRuntimeException e) {
//            throw e;
//        } catch (Exception e) {
//            throw new FatalConfigurationRuntimeException("Fatal error configuring Brooklyn launch: " + e.getMessage(), e);
//        }
//
//        try {
            launcherStart();
        } catch (FatalRuntimeException e) {
            // rely on caller logging this propagated exception
            throw e;
        } catch (Exception e) {
            // for other exceptions we log it, possibly redundantly but better too much than too little
            Exceptions.propagateIfFatal(e);
            log.error("Error launching brooklyn: " + Exceptions.collapseText(e), e);
            try {
                shutdown();
            } catch (Exception e2) {
                log.warn("Subsequent error during termination: " + e2);
                log.debug("Details of subsequent error during termination: " + e2, e2);
            }
            Exceptions.propagate(e);
        }

        // TODO
//        if (verbose) {
//            Entities.dumpInfo(launcher.getApplications());
//        }
    }

    private void initCatalog() {
        // TODO
//            StopWhichAppsOnShutdown stopWhichAppsOnShutdownMode = computeStopWhichAppsOnShutdown();
//
//            computeLocations();

//            ResourceUtils utils = ResourceUtils.create(this);
//            GroovyClassLoader loader = new GroovyClassLoader(getClass().getClassLoader());
//
//            // First, run a setup script if the user has provided one
//            if (script != null) {
//                execGroovyScript(utils, loader, script);
//            }
        // TODO: extract method for catalog initialization
        CatalogInitialization catInit = new CatalogInitialization();
        catInit.addPopulationCallback(new Function<CatalogInitialization, Void>() {
            @Override
            public Void apply(CatalogInitialization catInit) {
                try {
                    populateCatalog(catInit.getManagementContext().getCatalog());
                } catch (Throwable e) {
                    catInit.handleException(e, "overridden main class populate catalog");
                }

                // Force load of catalog
                confirmCatalog(catInit);
                return null;
            }
        });
        catInit.setFailOnStartupErrors(!ignoreCatalogErrors);
//            launcher.catalogInitialization(catInit);
    }

//    private void initBrooklynProperties() {
//        BrooklynProperties.Factory.Builder builder = BrooklynProperties.Factory.builderDefault();
//
//        if (globalBrooklynPropertiesFile != null) {
//            File globalProperties = new File(Os.tidyPath(globalBrooklynPropertiesFile));
//            if (globalProperties.exists()) {
//                globalProperties = resolveSymbolicLink(globalProperties);
//                checkFileReadable(globalProperties);
//                    // brooklyn.properties stores passwords (web-console and cloud credentials),
//                // so ensure it has sensible permissions
//                checkFilePermissionsX00(globalProperties);
//                log.debug("Using global properties file " + globalProperties);
//            } else {
//                log.debug("Global properties file " + globalProperties + " does not exist, will ignore");
//            }
//            builder.globalPropertiesFile(globalProperties.getAbsolutePath());
//        } else {
//            log.debug("Global properties file disabled");
//            builder.globalPropertiesFile(null);
//        }
//
//        if (localBrooklynPropertiesFile != null) {
//            File localProperties = new File(Os.tidyPath(localBrooklynPropertiesFile));
//            localProperties = resolveSymbolicLink(localProperties);
//            checkFileReadable(localProperties);
//            checkFilePermissionsX00(localProperties);
//            builder.localPropertiesFile(localProperties.getAbsolutePath());
//        }
//
//        managementContext = new LocalManagementContext(builder, brooklynAdditionalProperties);
//
//        // not needed: called by osgi framework
////            // We created the management context, so we are responsible for terminating it
////            BrooklynShutdownHooks.invokeTerminateOnShutdown(managementContext);
//    }
//    /**
//     * @return The canonical path of the argument.
//     */
//    private File resolveSymbolicLink(File f) {
//        File f2 = f;
//        try {
//            f2 = f.getCanonicalFile();
//            if (Files.isSymbolicLink(f.toPath())) {
//                log.debug("Resolved symbolic link: {} -> {}", f, f2);
//            }
//        } catch (IOException e) {
//            log.warn("Could not determine canonical name of file " + f + "; returning original file", e);
//        }
//        return f2;
//    }
//
//    private void checkFileReadable(File f) {
//        if (!f.exists()) {
//            throw new FatalRuntimeException("File " + f + " does not exist");
//        }
//        if (!f.isFile()) {
//            throw new FatalRuntimeException(f + " is not a file");
//        }
//        if (!f.canRead()) {
//            throw new FatalRuntimeException(f + " is not readable");
//        }
//    }
//
//    private void checkFilePermissionsX00(File f) {
//
//        Maybe<String> permission = FileUtil.getFilePermissions(f);
//        if (permission.isAbsent()) {
//            log.debug("Could not determine permissions of file; assuming ok: " + f);
//        } else {
//            if (!permission.get().subSequence(4, 10).equals("------")) {
//                throw new FatalRuntimeException("Invalid permissions for file " + f + "; expected ?00 but was " + permission.get());
//            }
//        }
//    }

    /* equivalent of launcher.start() in Main.java */
    private void launcherStart() {
        // not needed: management context already running (courtesy of blueprint.xml)
//        // Create the management context
//        initManagementContext();

        // Inform catalog initialization that it is starting up
        CatalogInitialization catInit = ((ManagementContextInternal) managementContext).getCatalogInitialization();
        catInit.setStartingUp(true);

        // not needed: webapps are started as different features (brooklyn-rest-resources-cxf, brooklyn-jsgui etc)
//        // Start webapps as soon as mgmt context available -- can use them to detect progress of other processes
//        if (startWebApps) {
//            try {
//                startWebApps();
//            } catch (Exception e) {
//                handleSubsystemStartupError(ignoreWebErrors, "core web apps", e);
//            }
//        }
        // not needed: camp is a separate karaf feature - let it initialize itself
//        // Add a CAMP platform
//        campPlatform = new BrooklynCampPlatformLauncherNoServer()
//                .useManagementContext(managementContext)
//                .launch()
//                .getCampPlatform();
        try {
            initPersistence();
            startPersistence();
        } catch (Exception e) {
            handleSubsystemStartupError(ignorePersistenceErrors, "persistence", e);
        }

        try {
            // run cat init now if it hasn't yet been run;
            // will also run if there was an ignored error in catalog above, allowing it to fail startup here if requested
            if (!catInit.hasRunOfficialInitialization()) {
                if (persistMode == PersistMode.DISABLED) {
                    log.debug("Loading catalog as part of launch sequence (it was not loaded as part of any rebind sequence)");
                    catInit.populateCatalog(ManagementNodeState.MASTER, true, true, null);
                } else {
                    // should have loaded during rebind
                    ManagementNodeState state = managementContext.getHighAvailabilityManager().getNodeState();
                    log.warn("Loading catalog for " + state + " as part of launch sequence (it was not loaded as part of the rebind sequence)");
                    catInit.populateCatalog(state, true, true, null);
                }
            }
        } catch (Exception e) {
            handleSubsystemStartupError(ignoreCatalogErrors, "initial catalog", e);
        }
        catInit.setStartingUp(false);

        // no need for command-line locations
//        // Create the locations. Must happen after persistence is started in case the
//        // management context's catalog is loaded from persisted state. (Location
//        // resolution uses the catalog's classpath to scan for resolvers.)
//        locations.addAll(managementContext.getLocationRegistry().resolve(locationSpecs));
        // Already rebinded successfully, so previous apps are now available.
        // Allow the startup to be visible in console for newly created apps.
        ((LocalManagementContext) managementContext).noteStartupComplete();

//        // TODO create apps only after becoming master, analogously to catalog initialization
//        try {
//            createApps();
//            startApps();
//        } catch (Exception e) {
//            handleSubsystemStartupError(ignoreAppErrors, "brooklyn autostart apps", e);
//        }
//        if (startBrooklynNode) {
//            try {
//                startBrooklynNode();
//            } catch (Exception e) {
//                handleSubsystemStartupError(ignoreAppErrors, "brooklyn node / self entity", e);
//            }
//        }
//
        if (persistMode != PersistMode.DISABLED) {
            // Make sure the new apps are persisted in case process exits immediately.
            managementContext.getRebindManager().forcePersistNow(false, null);
        }
    }

    public void shutdown() {
        // not needed: webserver is in other bundles
//        if (webServer != null) {
//            try {
//                webServer.stop();
//            } catch (Exception e) {
//                LOG.warn("Error stopping web-server; continuing with termination", e);
//            }
//        }

        // TODO Do we want to do this as part of managementContext.terminate, so after other threads are terminated etc?
        // Otherwise the app can change between this persist and the terminate.
        if (persistMode != PersistMode.DISABLED) {
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                if (managementContext.getHighAvailabilityManager().getPersister() != null) {
                    managementContext.getHighAvailabilityManager().getPersister().waitForWritesCompleted(Duration.TEN_SECONDS);
                }
                managementContext.getRebindManager().waitForPendingComplete(Duration.TEN_SECONDS, true);
                log.info("Finished waiting for persist; took " + Time.makeTimeStringRounded(stopwatch));
            } catch (RuntimeInterruptedException e) {
                Thread.currentThread().interrupt(); // keep going with shutdown
                log.warn("Persistence interrupted during shutdown: " + e, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // keep going with shutdown
                log.warn("Persistence interrupted during shutdown: " + e, e);
            } catch (TimeoutException e) {
                log.warn("Timeout after 10 seconds waiting for persistence to write all data; continuing");
            }
        }

        // not needed; called using blueprint.xml
//        if (managementContext instanceof ManagementContextInternal) {
//            ((ManagementContextInternal) managementContext).terminate();
//        }
        // TODO
//        for (Location loc : locations) {
//            if (loc instanceof Closeable) {
//                Streams.closeQuietly((Closeable) loc);
//            }
//        }
    }

    /**
     * method intended for subclassing, to add custom items to the catalog
     *
     * @todo Remove ?
     */
    protected void populateCatalog(BrooklynCatalog catalog) {
        // nothing else added here
    }

    protected void confirmCatalog(CatalogInitialization catInit) {
        Stopwatch time = Stopwatch.createStarted();
        BrooklynCatalog catalog = catInit.getManagementContext().getCatalog();
        Iterable<CatalogItem<Object, Object>> items = catalog.getCatalogItems();
        for (CatalogItem<Object, Object> item : items) {
            try {
                if (item.getCatalogItemType() == CatalogItem.CatalogItemType.TEMPLATE) {
                    // skip validation of templates, they might contain instructions,
                    // and additionally they might contain multiple items in which case
                    // the validation below won't work anyway (you need to go via a deployment plan)
                } else {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    Object spec = catalog.createSpec((CatalogItem) item);
                    if (spec instanceof EntitySpec) {
                        BrooklynTypes.getDefinedEntityType(((EntitySpec<?>) spec).getType());
                    }
                    log.debug("Catalog loaded spec " + spec + " for item " + item);
                }
            } catch (Throwable throwable) {
                catInit.handleException(throwable, item);
            }
        }
        log.debug("Catalog (size " + Iterables.size(items) + ") confirmed in " + Duration.of(time));
    }

    private void handleSubsystemStartupError(boolean ignoreSuchErrors, String system, Exception e) {
        Exceptions.propagateIfFatal(e);
        if (ignoreSuchErrors) {
            log.error("Subsystem for " + system + " had startup error (continuing with startup): " + e, e);
            if (managementContext != null) {
                ((ManagementContextInternal) managementContext).errors().add(e);
            }
        } else {
            throw Exceptions.propagate(e);
        }
    }

    protected void initPersistence() {
        // Prepare the rebind directory, and initialise the RebindManager as required
        final PersistenceObjectStore objectStore;
        if (persistMode == PersistMode.DISABLED) {
            log.info("Persistence disabled");
            objectStore = null;

        } else {
            try {
                BrooklynProperties brooklynProperties = ((ManagementContextInternal) managementContext).getBrooklynProperties();
                if (persistenceLocation == null) {
                    persistenceLocation = brooklynProperties.getConfig(BrooklynServerConfig.PERSISTENCE_LOCATION_SPEC);
                }
                persistenceDir = BrooklynServerPaths.newMainPersistencePathResolver(brooklynProperties).location(persistenceLocation).dir(persistenceDir).resolve();
                objectStore = BrooklynPersistenceUtils.newPersistenceObjectStore(managementContext, persistenceLocation, persistenceDir,
                        persistMode, highAvailabilityMode);

                RebindManager rebindManager = managementContext.getRebindManager();

                BrooklynMementoPersisterToObjectStore persister = new BrooklynMementoPersisterToObjectStore(
                        objectStore,
                        ((ManagementContextInternal) managementContext).getBrooklynProperties(),
                        managementContext.getCatalogClassLoader());
                PersistenceExceptionHandler persistenceExceptionHandler = PersistenceExceptionHandlerImpl.builder().build();
                ((RebindManagerImpl) rebindManager).setPeriodicPersistPeriod(persistPeriod);
                rebindManager.setPersister(persister, persistenceExceptionHandler);
            } catch (FatalConfigurationRuntimeException e) {
                throw e;
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                log.debug("Error initializing persistence subsystem (rethrowing): " + e, e);
                throw new FatalRuntimeException("Error initializing persistence subsystem: "
                        + Exceptions.collapseText(e), e);
            }
        }

        // Initialise the HA manager as required
        if (highAvailabilityMode == HighAvailabilityMode.DISABLED) {
            log.info("High availability disabled");
        } else {
            if (objectStore == null) {
                throw new FatalConfigurationRuntimeException("Cannot run in HA mode when no persistence configured.");
            }

            HighAvailabilityManager haManager = managementContext.getHighAvailabilityManager();
            ManagementPlaneSyncRecordPersister persister
                    = new ManagementPlaneSyncRecordPersisterToObjectStore(managementContext,
                            objectStore,
                            managementContext.getCatalogClassLoader());
            // not needed: set from config keys
//            ((HighAvailabilityManagerImpl) haManager).setHeartbeatTimeout(haHeartbeatTimeoutOverride);
//            ((HighAvailabilityManagerImpl) haManager).setPollPeriod(haHeartbeatPeriodOverride);
            haManager.setPersister(persister);
        }
    }

    protected void startPersistence() {
        // Now start the HA Manager and the Rebind manager, as required
        if (highAvailabilityMode == HighAvailabilityMode.DISABLED) {
            HighAvailabilityManager haManager = managementContext.getHighAvailabilityManager();
            haManager.disabled();

            if (persistMode != PersistMode.DISABLED) {
                startPersistenceWithoutHA();
            }

        } else {
            // Let the HA manager decide when objectstore.prepare and rebindmgr.rebind need to be called
            // (based on whether other nodes in plane are already running).

            HighAvailabilityMode startMode = null;
            switch (highAvailabilityMode) {
                case AUTO:
                case MASTER:
                case STANDBY:
                case HOT_STANDBY:
                case HOT_BACKUP:
                    startMode = highAvailabilityMode;
                    break;
                case DISABLED:
                    throw new IllegalStateException("Unexpected code-branch for high availability mode " + highAvailabilityMode);
            }
            if (startMode == null) {
                throw new IllegalStateException("Unexpected high availability mode " + highAvailabilityMode);
            }

            log.debug("Management node (with HA) starting");
            HighAvailabilityManager haManager = managementContext.getHighAvailabilityManager();
            // prepare after HA mode is known, to prevent backups happening in standby mode
            haManager.start(startMode);
        }
    }

    private void startPersistenceWithoutHA() {
        RebindManager rebindManager = managementContext.getRebindManager();
        if (Strings.isNonBlank(persistenceLocation)) {
            log.info("Management node (no HA) rebinding to entities at " + persistenceLocation + " in " + persistenceDir);
        } else {
            log.info("Management node (no HA) rebinding to entities on file system in " + persistenceDir);
        }

        ClassLoader classLoader = managementContext.getCatalogClassLoader();
        try {
            rebindManager.rebind(classLoader, null, ManagementNodeState.MASTER);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.debug("Error rebinding to persisted state (rethrowing): " + e, e);
            throw new FatalRuntimeException("Error rebinding to persisted state: "
                    + Exceptions.collapseText(e), e);
        }
        rebindManager.startPersistence();
    }

    public void setHighAvailabilityMode(HighAvailabilityMode highAvailabilityMode) {
        this.highAvailabilityMode = highAvailabilityMode;
    }

    public void setIgnoreCatalogErrors(boolean ignoreCatalogErrors) {
        this.ignoreCatalogErrors = ignoreCatalogErrors;
    }

    public void setIgnorePersistenceErrors(boolean ignorePersistenceErrors) {
        this.ignorePersistenceErrors = ignorePersistenceErrors;
    }

    public void setManagementContext(ManagementContext managementContext) {
        this.managementContext = managementContext;
    }

    public void setPersistMode(PersistMode persistMode) {
        this.persistMode = persistMode;
    }

    public void setPersistenceDir(String persistenceDir) {
        this.persistenceDir = persistenceDir;
    }

    public void setPersistenceLocation(String persistenceLocation) {
        this.persistenceLocation = persistenceLocation;
    }

    public void setPersistPeriod(String persistPeriodDescription) {
        this.persistPeriod = Duration.parse(persistPeriodDescription);
    }

}
