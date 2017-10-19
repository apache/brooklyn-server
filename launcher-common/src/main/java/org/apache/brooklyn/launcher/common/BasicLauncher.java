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
package org.apache.brooklyn.launcher.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityManager;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.mgmt.ha.ManagementPlaneSyncRecord;
import org.apache.brooklyn.api.mgmt.ha.ManagementPlaneSyncRecordPersister;
import org.apache.brooklyn.api.mgmt.rebind.PersistenceExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.camp.CampPlatform;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatformLauncherNoServer;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
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
import org.apache.brooklyn.core.mgmt.rebind.transformer.CompoundTransformer;
import org.apache.brooklyn.core.mgmt.rebind.transformer.impl.DeleteOrphanedStateTransformer;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.entity.brooklynnode.BrooklynNode;
import org.apache.brooklyn.entity.brooklynnode.LocalBrooklynNode;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.FatalConfigurationRuntimeException;
import org.apache.brooklyn.util.exceptions.FatalRuntimeException;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Example usage is:
 *  * <pre>
 * {@code
 * BasicLauncher launcher = BasicLauncher.newInstance()
 *     .application(new WebClusterDatabaseExample().appDisplayName("Web-cluster example"))
 *     .location("localhost")
 *     .start();
 * 
 * Dumper.dumpInfo(launcher.getApplications());
 * </pre>
 */
public class BasicLauncher<T extends BasicLauncher<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(BasicLauncher.class);

    private final Map<String,Object> brooklynAdditionalProperties = Maps.newLinkedHashMap();
    private BrooklynProperties brooklynProperties;
    private ManagementContext managementContext;
    
    private final List<String> locationSpecs = new ArrayList<String>();
    private final List<Location> locations = new ArrayList<Location>();

    private final List<EntitySpec<? extends Application>> appSpecsToManage = new ArrayList<>();
    private final List<String> yamlAppsToManage = new ArrayList<String>();
    private final List<Application> apps = new ArrayList<Application>();
    
    private boolean startBrooklynNode = false;
    
    private boolean ignorePersistenceErrors = true;
    private boolean ignoreCatalogErrors = true;
    private boolean ignoreAppErrors = true;
    
    private CatalogInitialization catalogInitialization = null;
    
    private PersistMode persistMode = PersistMode.DISABLED;
    private HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.DISABLED;
    private String persistenceDir;
    private String persistenceLocation;
    private Duration persistPeriod = Duration.ONE_SECOND;
    // these default values come from config in HighAvailablilityManagerImpl
    private Duration haHeartbeatTimeoutOverride = null;
    private Duration haHeartbeatPeriodOverride = null;
    
    protected boolean startedPartTwo;
    protected boolean started;
    
    private BrooklynProperties.Factory.Builder brooklynPropertiesBuilder;

    private CampPlatform campPlatform;

    public ManagementContext getManagementContext() {
        return managementContext;
    }

    public List<Application> getApplications() {
        if (!started) throw new IllegalStateException("Cannot retrieve application until started");
        return ImmutableList.copyOf(apps);
    }

    /** 
     * Specifies that the launcher should build and manage the Brooklyn application
     * described by the given spec.
     * The application will not be started as part of this call (callers can
     * subsequently call {@link #start()} or {@link #getApplications()}.
     */
    public T application(EntitySpec<? extends Application> appSpec) {
        appSpecsToManage.add(appSpec);
        return self();
    }

    /**
     * Specifies that the launcher should build and manage the Brooklyn application
     * described by the given YAML blueprint.
     * The application will not be started as part of this call (callers can
     * subsequently call {@link #start()} or {@link #getApplications()}.
     *
     * @see #application(Application)
     */
    public T application(String yaml) {
        this.yamlAppsToManage.add(yaml);
        return self();
    }

    /**
     * Adds a location to be passed in on {@link #start()}, when that calls
     * {@code application.start(locations)}.
     */
    public T location(Location location) {
        locations.add(checkNotNull(location, "location"));
        return self();
    }

    /**
     * Give the spec of an application, to be created.
     * 
     * @see #location(Location)
     */
    public T location(String spec) {
        locationSpecs.add(checkNotNull(spec, "spec"));
        return self();
    }
    
    public T locations(List<String> specs) {
        locationSpecs.addAll(checkNotNull(specs, "specs"));
        return self();
    }

    public T persistenceLocation(@Nullable String persistenceLocationSpec) {
        persistenceLocation = persistenceLocationSpec;
        return self();
    }

    /** 
     * Specifies the management context this launcher should use. 
     * If not specified a new one is created automatically.
     */
    public T managementContext(ManagementContext context) {
        if (brooklynProperties != null) throw new IllegalStateException("Cannot set brooklynProperties and managementContext");
        this.managementContext = context;
        return self();
    }

    /**
     * Specifies the brooklyn properties to be used. 
     * Must not be set if managementContext is explicitly set.
     */
    public T brooklynProperties(BrooklynProperties brooklynProperties){
        if (managementContext != null) throw new IllegalStateException("Cannot set brooklynProperties and managementContext");
        if (this.brooklynProperties!=null && brooklynProperties!=null && this.brooklynProperties!=brooklynProperties)
            LOG.warn("Brooklyn properties being reset in "+self()+"; set null first if you wish to clear it", new Throwable("Source of brooklyn properties reset"));
        this.brooklynProperties = brooklynProperties;
        return self();
    }
    
    /**
     * Specifies a property to be added to the brooklyn properties
     */
    public T brooklynProperties(String field, Object value) {
        brooklynAdditionalProperties.put(checkNotNull(field, "field"), value);
        return self();
    }
    public <C> T brooklynProperties(ConfigKey<C> key, C value) {
        return brooklynProperties(key.getName(), value);
    }

    public T ignorePersistenceErrors(boolean ignorePersistenceErrors) {
        this.ignorePersistenceErrors = ignorePersistenceErrors;
        return self();
    }

    public T ignoreCatalogErrors(boolean ignoreCatalogErrors) {
        this.ignoreCatalogErrors = ignoreCatalogErrors;
        return self();
    }

    public T ignoreAppErrors(boolean ignoreAppErrors) {
        this.ignoreAppErrors = ignoreAppErrors;
        return self();
    }

    @Beta
    public T catalogInitialization(CatalogInitialization catInit) {
        if (started)
            throw new IllegalStateException("Cannot set catalog customization after start()");
        if (this.catalogInitialization!=null)
            throw new IllegalStateException("Initial catalog customization already set.");
        this.catalogInitialization = catInit;
        return self();
    }

    public T persistMode(PersistMode persistMode) {
        this.persistMode = checkNotNull(persistMode, "persistMode");
        return self();
    }

    public T highAvailabilityMode(HighAvailabilityMode highAvailabilityMode) {
        this.highAvailabilityMode = checkNotNull(highAvailabilityMode, "highAvailabilityMode");
        return self();
    }

    public T persistenceDir(@Nullable String persistenceDir) {
        this.persistenceDir = persistenceDir;
        return self();
    }

    public T persistenceDir(@Nullable File persistenceDir) {
        if (persistenceDir==null) return persistenceDir((String)null);
        return persistenceDir(persistenceDir.getAbsolutePath());
    }

    public T persistPeriod(Duration persistPeriod) {
        this.persistPeriod = persistPeriod;
        return self();
    }

    public T haHeartbeatTimeout(Duration val) {
        this.haHeartbeatTimeoutOverride = val;
        return self();
    }

    public T startBrooklynNode(boolean val) {
        this.startBrooklynNode = val;
        return self();
    }

    /**
     * Controls both the frequency of heartbeats, and the frequency of checking the health of other nodes.
     */
    public T haHeartbeatPeriod(Duration val) {
        this.haHeartbeatPeriodOverride = val;
        return self();
    }

    /**
     * @param destinationDir Directory for state to be copied to
     */
    public void copyPersistedState(String destinationDir) {
        copyPersistedState(destinationDir, null, null);
    }

    /**
     * @param destinationDir Directory for state to be copied to
     * @param destinationLocation Optional location if target for copied state is a blob store.
     */
    public void copyPersistedState(String destinationDir, @Nullable String destinationLocation) {
        copyPersistedState(destinationDir, destinationLocation, null);
    }

    /**
     * @param destinationDir Directory for state to be copied to
     * @param destinationLocationSpec Optional location if target for copied state is a blob store.
     * @param transformer Optional transformations to apply to retrieved state before it is copied.
     */
    public void copyPersistedState(String destinationDir, @Nullable String destinationLocationSpec, @Nullable CompoundTransformer transformer) {
        initManagementContext();
        try {
            highAvailabilityMode = HighAvailabilityMode.HOT_STANDBY;
            initPersistence();
        } catch (Exception e) {
            handleSubsystemStartupError(ignorePersistenceErrors, "persistence", e);
        }
        
        try {
            BrooklynMementoRawData memento = managementContext.getRebindManager().retrieveMementoRawData();
            if (transformer != null) memento = transformer.transform(memento);
            
            ManagementPlaneSyncRecord planeState = managementContext.getHighAvailabilityManager().loadManagementPlaneSyncRecord(true);
            
            LOG.info("Copying persisted state to "+destinationDir+(destinationLocationSpec!=null ? " @ "+destinationLocationSpec : ""));
            PersistenceObjectStore destinationObjectStore = BrooklynPersistenceUtils.newPersistenceObjectStore(
                managementContext, destinationLocationSpec, destinationDir);
            BrooklynPersistenceUtils.writeMemento(managementContext, memento, destinationObjectStore);
            BrooklynPersistenceUtils.writeManagerMemento(managementContext, planeState, destinationObjectStore);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            LOG.debug("Error copying persisted state (rethrowing): " + e, e);
            throw new FatalRuntimeException("Error copying persisted state: " +
                Exceptions.collapseText(e), e);
        }
    }

    public void cleanOrphanedState(String destinationDir, @Nullable String destinationLocationSpec) {
        copyPersistedState(destinationDir, destinationLocationSpec, DeleteOrphanedStateTransformer.builder().build());
    }

    /** @deprecated since 0.7.0 use {@link #copyPersistedState} instead */
    // Make private after deprecation
    @Deprecated
    public BrooklynMementoRawData retrieveState() {
        initManagementContext();
        initPersistence();
        return managementContext.getRebindManager().retrieveMementoRawData();
    }

    /**
     * @param memento The state to copy
     * @param destinationDir Directory for state to be copied to
     * @param destinationLocationSpec Optional location if target for copied state is a blob store.
     * @deprecated since 0.7.0 use {@link #copyPersistedState} instead
     */
    // Make private after deprecation
    @Deprecated
    public void persistState(BrooklynMementoRawData memento, String destinationDir, @Nullable String destinationLocationSpec) {
        initManagementContext();
        PersistenceObjectStore destinationObjectStore = BrooklynPersistenceUtils.newPersistenceObjectStore(
            managementContext, destinationLocationSpec, destinationDir);
        BrooklynPersistenceUtils.writeMemento(managementContext, memento, destinationObjectStore);
    }

    /**
     * Starts the web server (with web console) and Brooklyn applications, as per the specifications configured. 
     * @return An object containing details of the web server and the management context.
     */
    public T start() {
        startPartOne();
        startPartTwo();
        return self();
    }

    /**
     * Starts the web server (with web console) and Brooklyn applications, as per the specifications configured. 
     * @return An object containing details of the web server and the management context.
     */
    protected T startPartOne() {
        if (started) throw new IllegalStateException("Cannot start() or launch() multiple times");
        started = true;

        checkConfiguration();
        
        initManagementContext();

        CatalogInitialization catInit = ((ManagementContextInternal)managementContext).getCatalogInitialization();
        if (catalogInitialization != null && catalogInitialization != catInit) { 
            throw new IllegalStateException("Unexpected catalog initialization: " + catInit + " != " + catalogInitialization);
        }
        catalogInitialization = catInit;

        markCatalogStartingUp(catInit);
        
        // note: web console is started by subclass overriding this method
        startingUp();
        
        initCamp();
        
        return self();
    }

    private void checkConfiguration() {
        if (highAvailabilityMode != HighAvailabilityMode.DISABLED) {
            if (persistMode == PersistMode.DISABLED) {
                if (highAvailabilityMode == HighAvailabilityMode.AUTO) {
                    highAvailabilityMode = HighAvailabilityMode.DISABLED;
                } else {
                    throw new FatalConfigurationRuntimeException("Cannot specify highAvailability when persistence is disabled");
                }
            } else if (persistMode == PersistMode.CLEAN && 
                    (highAvailabilityMode == HighAvailabilityMode.STANDBY 
                            || highAvailabilityMode == HighAvailabilityMode.HOT_STANDBY
                            || highAvailabilityMode == HighAvailabilityMode.HOT_BACKUP)) {
                throw new FatalConfigurationRuntimeException("Cannot specify highAvailability "+highAvailabilityMode+" when persistence is CLEAN");
            }
        }
    }

    /**
     * Starts the web server (with web console) and Brooklyn applications, as per the specifications configured. 
     * @return An object containing details of the web server and the management context.
     */
    protected T startPartTwo() {
        if (startedPartTwo) throw new IllegalStateException("Cannot start() or launch() multiple times");
        startedPartTwo = true;
        
        if (persistMode != PersistMode.DISABLED) {
            handlePersistence();
        } else {
            ((LocalManagementContext)managementContext).generateManagementPlaneId();
            populateInitialCatalogNoPersistence(catalogInitialization);
            managementContext.getHighAvailabilityManager().disabled(false);
        }
        markCatalogStarted(catalogInitialization);
        addLocations();
        markStartupComplete();
        initApps();
        initBrooklynNode();

        persist();
        return self();
    }


    protected void persist() {
        if (persistMode != PersistMode.DISABLED) {
            // Make sure the new apps are persisted in case process exits immediately.
            managementContext.getRebindManager().forcePersistNow(false, null);
        }
    }

    protected void initBrooklynNode() {
        if (startBrooklynNode) {
            try {
                startBrooklynNode();
            } catch (Exception e) {
                handleSubsystemStartupError(ignoreAppErrors, "brooklyn node / self entity", e);
            }
        }
    }

    protected void initApps() {
        // TODO create apps only after becoming master, analogously to catalog initialization
        try {
            createApps();
            startApps();
        } catch (Exception e) {
            handleSubsystemStartupError(ignoreAppErrors, "brooklyn autostart apps", e);
        }
    }

    protected void markStartupComplete() {
        // Already rebinded successfully, so previous apps are now available.
        // Allow the startup to be visible in console for newly created apps.
        ((LocalManagementContext)managementContext).noteStartupComplete();
    }

    protected void addLocations() {
        // Create the locations. Must happen after persistence is started in case the
        // management context's catalog is loaded from persisted state. (Location
        // resolution uses the catalog's classpath to scan for resolvers.)
        locations.addAll(managementContext.getLocationRegistry().getListOfLocationsManaged(locationSpecs));
    }

    protected void startingUp() {
    }

    protected void populateInitialCatalogNoPersistence(CatalogInitialization catInit) {
        assert persistMode == PersistMode.DISABLED : "persistMode="+persistMode;
        
        try {
            LOG.debug("Initializing initial catalog as part of launch sequence (prior to any persistence rebind)");
            catInit.populateInitialCatalogOnly();
        } catch (Exception e) {
            handleSubsystemStartupError(ignoreCatalogErrors, "initial catalog", e);
        }
    }

    private void markCatalogStarted(CatalogInitialization catInit) {
        catInit.setStartingUp(false);
    }

    protected void handlePersistence() {
        try {
            initPersistence();
            startPersistence();
        } catch (Exception e) {
            handleSubsystemStartupError(ignorePersistenceErrors, "persistence", e);
        }
    }

    protected void initCamp() {
        // Add a CAMP platform
        campPlatform = new BrooklynCampPlatformLauncherNoServer()
                .useManagementContext(managementContext)
                .launch()
                .getCampPlatform();
        // TODO start CAMP rest _server_ in the below (at /camp) ?
    }

    protected void markCatalogStartingUp(CatalogInitialization catInit) {
        // Inform catalog initialization that it is starting up
        catInit.setStartingUp(true);
    }

    protected void initManagementContext() {
        // Create the management context
        if (managementContext == null) {
            managementContext = new LocalManagementContext(
                    brooklynPropertiesBuilder,
                    brooklynAdditionalProperties);

            brooklynProperties = ((ManagementContextInternal)managementContext).getBrooklynProperties();

            // We created the management context, so we are responsible for terminating it
            BrooklynShutdownHooks.invokeTerminateOnShutdown(getManagementContext());
        } else if (brooklynProperties == null) {
            brooklynProperties = ((ManagementContextInternal)managementContext).getBrooklynProperties();
            brooklynProperties.addFromMap(brooklynAdditionalProperties);
        }
        
        if (catalogInitialization!=null) {
            ((ManagementContextInternal)managementContext).setCatalogInitialization(catalogInitialization);
        }
    }

    protected void handleSubsystemStartupError(boolean ignoreSuchErrors, String system, Exception e) {
        Exceptions.propagateIfFatal(e);
        if (ignoreSuchErrors) {
            LOG.error("Subsystem for "+system+" had startup error (continuing with startup): "+e, e);
            if (managementContext!=null)
                ((ManagementContextInternal)managementContext).errors().add(e);
        } else {
            throw Exceptions.propagate(e);
        }
    }

    protected void initPersistence() {
        assert persistMode != PersistMode.DISABLED : "persistMode="+persistMode+"; haMode="+highAvailabilityMode;

        // Prepare the rebind directory, and initialise the RebindManager
        final PersistenceObjectStore objectStore;
        try {
            if (persistenceLocation == null) {
                persistenceLocation = brooklynProperties.getConfig(BrooklynServerConfig.PERSISTENCE_LOCATION_SPEC);
            }
            persistenceDir = BrooklynServerPaths.newMainPersistencePathResolver(brooklynProperties).location(persistenceLocation).dir(persistenceDir).resolve();
            objectStore = BrooklynPersistenceUtils.newPersistenceObjectStore(managementContext, persistenceLocation, persistenceDir, 
                persistMode, highAvailabilityMode);
                
            RebindManager rebindManager = managementContext.getRebindManager();
            
            BrooklynMementoPersisterToObjectStore persister = new BrooklynMementoPersisterToObjectStore(
                objectStore, managementContext);
            PersistenceExceptionHandler persistenceExceptionHandler = PersistenceExceptionHandlerImpl.builder().build();
            ((RebindManagerImpl) rebindManager).setPeriodicPersistPeriod(persistPeriod);
            rebindManager.setPersister(persister, persistenceExceptionHandler);
        } catch (FatalConfigurationRuntimeException e) {
            throw e;
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            LOG.debug("Error initializing persistence subsystem (rethrowing): "+e, e);
            throw new FatalRuntimeException("Error initializing persistence subsystem: "+
                Exceptions.collapseText(e), e);
        }
        
        // Initialise the HA manager as required
        if (highAvailabilityMode == HighAvailabilityMode.DISABLED) {
            LOG.info("High availability disabled");
        } else {
            HighAvailabilityManager haManager = managementContext.getHighAvailabilityManager();
            ManagementPlaneSyncRecordPersister persister =
                new ManagementPlaneSyncRecordPersisterToObjectStore(managementContext,
                    objectStore,
                    managementContext.getCatalogClassLoader());
            ((HighAvailabilityManagerImpl)haManager).setHeartbeatTimeout(haHeartbeatTimeoutOverride);
            ((HighAvailabilityManagerImpl)haManager).setPollPeriod(haHeartbeatPeriodOverride);
            haManager.setPersister(persister);
        }
    }
    
    protected void startPersistence() {
        assert persistMode != PersistMode.DISABLED : "persistMode="+persistMode+"; haMode="+highAvailabilityMode;
        
        // Now start the HA Manager and the Rebind manager, as required
        if (highAvailabilityMode == HighAvailabilityMode.DISABLED) {
            HighAvailabilityManager haManager = managementContext.getHighAvailabilityManager();
            haManager.disabled(true);

            startPersistenceWithoutHA();
            
        } else {
            // Let the HA manager decide when objectstore.prepare and rebindmgr.rebind need to be called 
            // (based on whether other nodes in plane are already running).
            
            HighAvailabilityMode startMode;
            switch (highAvailabilityMode) {
                case AUTO:
                case MASTER:
                case STANDBY:
                case HOT_STANDBY:
                case HOT_BACKUP:
                    startMode = highAvailabilityMode;
                    break;
                case DISABLED:
                    throw new IllegalStateException("Unexpected code-branch for high availability mode "+highAvailabilityMode);
                default:
                    throw new IllegalStateException("Unexpected high availability mode "+highAvailabilityMode);
            }
            
            LOG.debug("Management node (with HA) starting");
            HighAvailabilityManager haManager = managementContext.getHighAvailabilityManager();
            // prepare after HA mode is known, to prevent backups happening in standby mode
            haManager.start(startMode);
        }
    }

    private void startPersistenceWithoutHA() {
        RebindManager rebindManager = managementContext.getRebindManager();
        if (Strings.isNonBlank(persistenceLocation))
            LOG.info("Management node (no HA) rebinding to entities at "+persistenceLocation+" in "+persistenceDir);
        else
            LOG.info("Management node (no HA) rebinding to entities on file system in "+persistenceDir);

        ClassLoader classLoader = managementContext.getCatalogClassLoader();
        try {
            rebindManager.rebind(classLoader, null, ManagementNodeState.MASTER);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            LOG.debug("Error rebinding to persisted state (rethrowing): "+e, e);
            throw new FatalRuntimeException("Error rebinding to persisted state: "+
                Exceptions.collapseText(e), e);
        }
        rebindManager.startPersistence();
    }

    protected void createApps() {
        for (EntitySpec<? extends Application> spec : appSpecsToManage) {
            Application app = managementContext.getEntityManager().createEntity(spec);
            apps.add(app);
        }
        for (String blueprint : yamlAppsToManage) {
            Application app = EntityManagementUtils.createUnstarted(managementContext, blueprint);
            // Note: BrooklynAssemblyTemplateInstantiator automatically puts applications under management.
            apps.add(app);
        }
    }

    protected void startBrooklynNode() {
        final String classpath = System.getenv("INITIAL_CLASSPATH");
        if (Strings.isBlank(classpath)) {
            LOG.warn("Cannot find INITIAL_CLASSPATH environment variable, skipping BrooklynNode entity creation");
            return;
        }

        EntitySpec<LocalBrooklynNode> brooklynNodeSpec = EntitySpec.create(LocalBrooklynNode.class)
            .configure(SoftwareProcess.ENTITY_STARTED, true)
            .configure(BrooklynNode.CLASSPATH, Splitter.on(":").splitToList(classpath))
            .displayName("Brooklyn Console");

        brooklynNodeSpec = customizeBrooklynNodeSpec(brooklynNodeSpec);

        if (brooklynNodeSpec != null) {
            EntityManagementUtils.createStarting(managementContext,
                    EntitySpec.create(BasicApplication.class)
                        .displayName("Brooklyn")
                        .child(brooklynNodeSpec));
        }
    }

    /**
     * Configure the node so it can connect to Brooklyn's REST API. Return null to skip registering the node
     */
    protected EntitySpec<LocalBrooklynNode> customizeBrooklynNodeSpec(EntitySpec<LocalBrooklynNode> brooklynNodeSpec) {
        LOG.error("Skipping BrooklynNode registration. Configure a loopback REST endpoint configured for the node.");
        return null;
    }

    protected void startApps() {
        List<Throwable> appExceptions = Lists.newArrayList();
        for (Application app : apps) {
            if (app instanceof Startable) {

                try {
                    LOG.info("Starting brooklyn application {} in location{} {}", new Object[] { app, locations.size()!=1?"s":"", locations });
                    ((Startable)app).start(locations);
                    Dumper.dumpInfo(app);
                    String sensors = "";
                    if (app.getAttribute(Attributes.MAIN_URI_MAPPED_PUBLIC)!=null) {
                        sensors = ": "+app.getAttribute(Attributes.MAIN_URI_MAPPED_PUBLIC);
                    } else if (app.getAttribute(Attributes.MAIN_URI)!=null) {
                        sensors += ": "+app.getAttribute(Attributes.MAIN_URI);
                    }
                    LOG.info("Started brooklyn application {} in location{} {}{}", new Object[] { app, locations.size()!=1?"s":"", locations,
                        sensors });
                } catch (Exception e) {
                    LOG.error("Error starting "+app+": "+Exceptions.collapseText(e), Exceptions.getFirstInteresting(e));
                    appExceptions.add(Exceptions.collapse(e));

                    if (Thread.currentThread().isInterrupted()) {
                        LOG.error("Interrupted while starting applications; aborting");
                        break;
                    }
                }
            }
        }
        if (!appExceptions.isEmpty()) {
            Throwable t = Exceptions.create(appExceptions);
            throw new FatalRuntimeException("Error starting applications: "+Exceptions.collapseText(t), t);
        }
    }

    public boolean isStarted() {
        return started;
    }

    public PersistMode getPersistMode() {
        return persistMode;
    }

    public List<Location> getLocations() {
        return locations;
    }

    public CampPlatform getCampPlatform() {
        return campPlatform;
    }

    @SuppressWarnings("unchecked")
    private T self() {
        return (T) this;
    }

    public void setBrooklynPropertiesBuilder(BrooklynProperties.Factory.Builder brooklynPropertiesBuilder) {
        this.brooklynPropertiesBuilder = brooklynPropertiesBuilder;
    }
    
    public BrooklynProperties.Factory.Builder getBrooklynPropertiesBuilder() {
        return brooklynPropertiesBuilder;
    }

    public BrooklynProperties getBrooklynProperties() {
        return brooklynProperties;
    }

}
