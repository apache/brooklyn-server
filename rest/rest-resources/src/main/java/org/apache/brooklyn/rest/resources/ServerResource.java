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
package org.apache.brooklyn.rest.resources;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipFile;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;

import com.google.common.io.ByteSource;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityManager;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.mgmt.ha.ManagementPlaneSyncRecord;
import org.apache.brooklyn.api.mgmt.ha.MementoCopyMode;
import org.apache.brooklyn.api.mgmt.rebind.PersistenceExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.ShutdownHandler;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.*;
import org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializer.XmlMementoSerializerBuilder;
import org.apache.brooklyn.core.mgmt.rebind.PersistenceExceptionHandlerImpl;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.rest.api.ServerApi;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.domain.BrooklynFeatureSummary;
import org.apache.brooklyn.rest.domain.HighAvailabilitySummary;
import org.apache.brooklyn.rest.domain.VersionSummary;
import org.apache.brooklyn.rest.transform.BrooklynFeatureTransformer;
import org.apache.brooklyn.rest.transform.HighAvailabilityTransformer;
import org.apache.brooklyn.rest.util.MultiSessionAttributeAdapter;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.file.ArchiveBuilder;
import org.apache.brooklyn.util.core.file.ArchiveUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.InputStreamSource;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

public class ServerResource extends AbstractBrooklynRestResource implements ServerApi {

    private static final int SHUTDOWN_TIMEOUT_CHECK_INTERVAL = 200;

    private static final Logger log = LoggerFactory.getLogger(ServerResource.class);

    private static final String BUILD_SHA_1_PROPERTY = "git-sha-1";
    private static final String BUILD_BRANCH_PROPERTY = "git-branch-name";
    
    @Context
    private ContextResolver<ShutdownHandler> shutdownHandler;

    @Context
    private HttpServletRequest request;
    
    @Override
    public void reloadBrooklynProperties() {
        if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ALL_SERVER_INFO, null)) {
            brooklyn().reloadBrooklynProperties();
        } else {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        }
    }

    private boolean isMaster() {
        return ManagementNodeState.MASTER.equals(mgmt().getHighAvailabilityManager().getNodeState());
    }

    @Override
    public void shutdown(final boolean stopAppsFirst, final boolean forceShutdownOnError,
            String shutdownTimeoutRaw, String requestTimeoutRaw, String delayForHttpReturnRaw,
            Long delayMillis) {
        
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ALL_SERVER_INFO, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SHUTDOWN, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized for shutdown", Entitlements.getEntitlementContext().user());

        log.info("REST call to shutdown server, stopAppsFirst="+stopAppsFirst+", delayForHttpReturn="+shutdownTimeoutRaw);

        if (stopAppsFirst && !isMaster()) {
            log.warn("REST call to shutdown non-master server while stopping apps is disallowed");
            throw WebResourceUtils.forbidden("Not allowed to stop all apps when server is not master");
        }
        final Duration shutdownTimeout = parseDuration(shutdownTimeoutRaw, Duration.of(20, TimeUnit.SECONDS));
        Duration requestTimeout = parseDuration(requestTimeoutRaw, Duration.of(20, TimeUnit.SECONDS));
        final Duration delayForHttpReturn;
        if (delayMillis == null) {
            delayForHttpReturn = parseDuration(delayForHttpReturnRaw, Duration.FIVE_SECONDS);
        } else {
            log.warn("'delayMillis' is deprecated, use 'delayForHttpReturn' instead.");
            delayForHttpReturn = Duration.of(delayMillis, TimeUnit.MILLISECONDS);
        }

        Preconditions.checkState(delayForHttpReturn.nanos() >= 0, "Only positive or 0 delay allowed for delayForHttpReturn");

        boolean isSingleTimeout = shutdownTimeout.equals(requestTimeout);
        final AtomicBoolean completed = new AtomicBoolean();
        final AtomicBoolean hasAppErrorsOrTimeout = new AtomicBoolean();

        //shutdownHandler & mgmt is thread local
        final ShutdownHandler handler = shutdownHandler.getContext(ShutdownHandler.class);
        final ManagementContext mgmt = mgmt();
        new Thread("shutdown") {
            @Override
            public void run() {
                boolean terminateTried = false;
                try {
                    if (stopAppsFirst) {
                        CountdownTimer shutdownTimeoutTimer = null;
                        if (!shutdownTimeout.equals(Duration.ZERO)) {
                            shutdownTimeoutTimer = shutdownTimeout.countdownTimer();
                        }

                        log.debug("Stopping applications");
                        List<Task<?>> stoppers = new ArrayList<Task<?>>();
                        int allStoppableApps = 0;
                        for (Application app: mgmt.getApplications()) {
                            allStoppableApps++;
                            Lifecycle appState = app.getAttribute(Attributes.SERVICE_STATE_ACTUAL);
                            if (app instanceof StartableApplication &&
                                    // Don't try to stop an already stopping app. Subsequent stops will complete faster
                                    // cancelling the first stop task.
                                    appState != Lifecycle.STOPPING) {
                                stoppers.add(Entities.invokeEffector(app, app, StartableApplication.STOP));
                            } else {
                                log.debug("App " + app + " is already stopping, will not stop second time. Will wait for original stop to complete.");
                            }
                        }

                        log.debug("Waiting for " + allStoppableApps + " apps to stop, of which " + stoppers.size() + " stopped explicitly.");
                        for (Task<?> t: stoppers) {
                            if (!waitAppShutdown(shutdownTimeoutTimer, t)) {
                                //app stop error
                                hasAppErrorsOrTimeout.set(true);
                            }
                        }

                        // Wait for apps which were already stopping when we tried to shut down.
                        if (hasStoppableApps(mgmt)) {
                            log.debug("Apps are still stopping, wait for proper unmanage.");
                            while (hasStoppableApps(mgmt) && (shutdownTimeoutTimer == null || !shutdownTimeoutTimer.isExpired())) {
                                Duration wait;
                                if (shutdownTimeoutTimer != null) {
                                    wait = Duration.min(shutdownTimeoutTimer.getDurationRemaining(), Duration.ONE_SECOND);
                                } else {
                                    wait = Duration.ONE_SECOND;
                                }
                                Time.sleep(wait);
                            }
                            if (hasStoppableApps(mgmt)) {
                                hasAppErrorsOrTimeout.set(true);
                            }
                        }
                    }

                    terminateTried = true;
                    ((ManagementContextInternal)mgmt).terminate(); 

                } catch (Throwable e) {
                    Throwable interesting = Exceptions.getFirstInteresting(e);
                    if (interesting instanceof TimeoutException) {
                        //timeout while waiting for apps to stop
                        log.warn("Timeout shutting down: "+Exceptions.collapseText(e));
                        log.debug("Timeout shutting down: "+e, e);
                        hasAppErrorsOrTimeout.set(true);
                        
                    } else {
                        // swallow fatal, so we notify the outer loop to continue with shutdown
                        log.error("Unexpected error shutting down: "+Exceptions.collapseText(e), e);
                        
                    }
                    hasAppErrorsOrTimeout.set(true);
                    
                    if (!terminateTried) {
                        mgmtInternal().terminate(); 
                    }
                } finally {

                    complete();
                
                    if (!hasAppErrorsOrTimeout.get() || forceShutdownOnError) {
                        //give the http request a chance to complete gracefully, the server will be stopped in a shutdown hook
                        Time.sleep(delayForHttpReturn);

                        if (handler != null) {
                            handler.onShutdownRequest();
                        } else {
                            log.warn("ShutdownHandler not set, exiting process");
                            System.exit(0);
                        }
                        
                    } else {
                        // There are app errors, don't exit the process, allowing any exception to continue throwing
                        log.warn("Abandoning shutdown because there were errors and shutdown was not forced.");
                        
                    }
                }
            }

            private boolean hasStoppableApps(ManagementContext mgmt) {
                for (Application app : mgmt.getApplications()) {
                    if (app instanceof StartableApplication) {
                        Lifecycle state = app.getAttribute(Attributes.SERVICE_STATE_ACTUAL);
                        if (state != Lifecycle.STOPPING && state != Lifecycle.STOPPED) {
                            log.warn("Shutting down, expecting all apps to be in stopping state, but found application " + app + " to be in state " + state + ". Just started?");
                        }
                        return true;
                    }
                }
                return false;
            }

            private void complete() {
                synchronized (completed) {
                    completed.set(true);
                    completed.notifyAll();
                }
            }

            private boolean waitAppShutdown(CountdownTimer shutdownTimeoutTimer, Task<?> t) throws TimeoutException {
                Duration waitInterval = null;
                //wait indefinitely if no shutdownTimeoutTimer (shutdownTimeout == 0)
                if (shutdownTimeoutTimer != null) {
                    waitInterval = Duration.of(SHUTDOWN_TIMEOUT_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
                }
                // waitInterval == null - blocks indefinitely
                while(!t.blockUntilEnded(waitInterval)) {
                    if (shutdownTimeoutTimer.isExpired()) {
                        log.warn("Timeout while waiting for applications to stop at "+t+".\n"+t.getStatusDetail(true));
                        throw new TimeoutException();
                    }
                }
                if (t.isError()) {
                    log.warn("Error stopping application "+t+" during shutdown (ignoring)\n"+t.getStatusDetail(true));
                    return false;
                } else {
                    return true;
                }
            }
        }.start();

        synchronized (completed) {
            if (!completed.get()) {
                try {
                    long waitTimeout = 0;
                    //If the timeout for both shutdownTimeout and requestTimeout is equal
                    //then better wait until the 'completed' flag is set, rather than timing out
                    //at just about the same time (i.e. always wait for the shutdownTimeout in this case).
                    //This will prevent undefined behaviour where either one of shutdownTimeout or requestTimeout
                    //will be first to expire and the error flag won't be set predictably, it will
                    //toggle depending on which expires first.
                    //Note: shutdownTimeout is checked at SHUTDOWN_TIMEOUT_CHECK_INTERVAL interval, meaning it is
                    //practically rounded up to the nearest SHUTDOWN_TIMEOUT_CHECK_INTERVAL.
                    if (!isSingleTimeout) {
                        waitTimeout = requestTimeout.toMilliseconds();
                    }
                    completed.wait(waitTimeout);
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            }
        }

        if (hasAppErrorsOrTimeout.get()) {
            WebResourceUtils.badRequest("Error or timeout while stopping applications. See log for details.");
        }
    }

    private Duration parseDuration(String str, Duration defaultValue) {
        if (Strings.isEmpty(str)) {
            return defaultValue;
        } else {
            return Duration.parse(str);
        }
    }

    @Override
    public VersionSummary getVersion() {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SERVER_STATUS, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        
        // TODO
        // * "build-metadata.properties" is probably the wrong name
        // * we should include brooklyn.version and a build timestamp in this file
        // * the authority for brooklyn should probably be core rather than brooklyn-rest-server
        InputStream input = ResourceUtils.create(this).getResourceFromUrl("classpath://build-metadata.properties");
        Properties properties = new Properties();
        String gitSha1 = null, gitBranch = null;
        try {
            properties.load(input);
            gitSha1 = properties.getProperty(BUILD_SHA_1_PROPERTY);
            gitBranch = properties.getProperty(BUILD_BRANCH_PROPERTY);
        } catch (IOException e) {
            log.error("Failed to load build-metadata.properties", e);
        }
        gitSha1 = BrooklynVersion.INSTANCE.getSha1FromOsgiManifest();

        FluentIterable<BrooklynFeatureSummary> features = FluentIterable.from(BrooklynVersion.getFeatures(mgmt()))
                .transform(BrooklynFeatureTransformer.FROM_FEATURE);

        return new VersionSummary(BrooklynVersion.get(), gitSha1, gitBranch, features.toList());
    }

    @Override
    public String getPlaneId() {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SERVER_STATUS, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());

        Maybe<ManagementContext> mm = mgmtMaybe();
        Maybe<String> result = (mm.isPresent()) ? mm.get().getManagementPlaneIdMaybe() : Maybe.absent();
        return result.isPresent() ? result.get() : "";
    }

    @Override
    public boolean isUp() {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SERVER_STATUS, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());

        Maybe<ManagementContext> mm = mgmtMaybe();
        return !mm.isAbsent() && mm.get().isStartupComplete() && mm.get().isRunning();
    }
    
    @Override
    public boolean isShuttingDown() {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SERVER_STATUS, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        Maybe<ManagementContext> mm = mgmtMaybe();
        return !mm.isAbsent() && mm.get().isStartupComplete() && !mm.get().isRunning();
    }
    
    @Override
    public boolean isHealthy() {
        return isUp() && mgmtInternal().errors().isEmpty();
    }
    
    @Override
    public Map<String,Object> getUpExtended() {
        // force creation of a session to demonstrate health, help the UI be more efficient
        MultiSessionAttributeAdapter.of(request);
        
        return MutableMap.<String,Object>of(
            "up", isUp(),
            "shuttingDown", isShuttingDown(),
            "healthy", isHealthy(),
            "ha", getHighAvailabilityPlaneStates());
    }

    @Override
    public String getConfig(String configKey) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ALL_SERVER_INFO, null)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        }
        ConfigKey<String> config = ConfigKeys.newStringConfigKey(configKey);
        return (String) WebResourceUtils.getValueForDisplay(mapper(), mgmt().getConfig().getConfig(config), true, true);
    }

    @Override
    public ManagementNodeState getHighAvailabilityNodeState() {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SERVER_STATUS, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        
        Maybe<ManagementContext> mm = mgmtMaybe();
        if (mm.isAbsent()) return ManagementNodeState.INITIALIZING;
        return mm.get().getHighAvailabilityManager().getNodeState();
    }

    @Override
    public ManagementNodeState setHighAvailabilityNodeState(HighAvailabilityMode mode) {
        if (mode==null)
            throw new IllegalStateException("Missing parameter: mode");
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.HA_ADMIN, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());

        HighAvailabilityManager haMgr = mgmt().getHighAvailabilityManager();
        ManagementNodeState existingState = haMgr.getNodeState();
        haMgr.changeMode(mode);
        return existingState;
    }

    @Override
    public Map<String, Object> getHighAvailabilityMetrics() {
        if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.HA_STATS, null))
            return mgmt().getHighAvailabilityManager().getMetrics();
        else
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
    }
    
    @Override
    public long getHighAvailabitlityPriority() {
        if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.HA_STATS, null)) {
            return mgmt().getHighAvailabilityManager().getPriority();
        } else {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        }
    }

    @Override
    public long setHighAvailabilityPriority(long priority) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.HA_ADMIN, null)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        }

        HighAvailabilityManager haMgr = mgmt().getHighAvailabilityManager();
        long oldPrio = haMgr.getPriority();
        haMgr.setPriority(priority);
        return oldPrio;
    }

    @Override
    public HighAvailabilitySummary getHighAvailabilityPlaneStates() {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SERVER_STATUS, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());

        ManagementPlaneSyncRecord memento = mgmt().getHighAvailabilityManager().getLastManagementPlaneSyncRecord();
        if (memento==null) memento = mgmt().getHighAvailabilityManager().loadManagementPlaneSyncRecord(true);
        if (memento==null) return null;
        // This may be the case if this method was called before persistence was properly initialised.
        // Retry so that the server doesn't get stuck. See https://issues.apache.org/jira/browse/BROOKLYN-167.
        if (memento.getMasterNodeId() == null) {
            memento = mgmt().getHighAvailabilityManager().loadManagementPlaneSyncRecord(true);
        }

        return HighAvailabilityTransformer.highAvailabilitySummary(mgmt().getManagementNodeId(), memento);
    }

    @Override
    public Response clearHighAvailabilityPlaneStates() {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SYSTEM_ADMIN, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        mgmt().getHighAvailabilityManager().publishClearNonMaster();
        return Response.ok().build();
    }

    @Override
    public Response clearHighAvailabilityPlaneStates(String nodeId) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SYSTEM_ADMIN, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        HighAvailabilityManager haMan = mgmt().getHighAvailabilityManager();
        haMan.setNodeIdToRemove(nodeId);
        haMan.publishClearNonMaster();
        return Response.ok().build();
    }

    @Override
    public String getUser() {
        // force creation of session to help ui, ensure continuity of user info
        MultiSessionAttributeAdapter.of(request);
        
        EntitlementContext entitlementContext = Entitlements.getEntitlementContext();
        if (entitlementContext!=null && entitlementContext.user()!=null){
            return (String) WebResourceUtils.getValueForDisplay(mapper(), entitlementContext.user(), true, true);
        } else {
            return null; //User can be null if no authentication was requested
        }
    }

    @Override
    public Response exportPersistenceData(String preferredOrigin) {
        return exportPersistenceData(TypeCoercions.coerce(preferredOrigin, MementoCopyMode.class));
    }
    
    protected Response exportPersistenceData(MementoCopyMode preferredOrigin) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ALL_SERVER_INFO, null))
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());

        File dir = null;
        try {
            String label = mgmt().getManagementNodeId()+"-"+Time.makeDateSimpleStampString();
            PersistenceObjectStore targetStore = BrooklynPersistenceUtils.newPersistenceObjectStore(mgmt(), null, 
                "tmp/web-persistence-"+label+"-"+Identifiers.makeRandomId(4));
            dir = ((FileBasedObjectStore)targetStore).getBaseDir();
            // only register the parent dir because that will prevent leaks for the random ID
            Os.deleteOnExitEmptyParentsUpTo(dir.getParentFile(), dir.getParentFile());
            BrooklynPersistenceUtils.writeMemento(mgmt(), targetStore, preferredOrigin);
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ArchiveBuilder.zip().addDirContentsAt( ((FileBasedObjectStore)targetStore).getBaseDir(), ((FileBasedObjectStore)targetStore).getBaseDir().getName() ).stream(baos);
            Os.deleteRecursively(dir);
            String filename = "brooklyn-state-"+label+".zip";
            return Response.ok(baos.toByteArray(), MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .header("Content-Disposition","attachment; filename = "+filename)
                .build();
        } catch (Exception e) {
            log.warn("Unable to serve persistence data (rethrowing): "+e, e);
            if (dir!=null) {
                try {
                    Os.deleteRecursively(dir);
                } catch (Exception e2) {
                    log.warn("Ignoring error deleting '"+dir+"' after another error, throwing original error ("+e+"); ignored error deleting is: "+e2);
                }
            }
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public Response importPersistenceData(byte[] persistenceData) {
        File tempZipFile = null;
        try {
            // create a temporary archive where persistence data supplied is written to
            tempZipFile = File.createTempFile("persistence-import",null);
            Files.write(tempZipFile.toPath(), persistenceData, StandardOpenOption.TRUNCATE_EXISTING);

            // set path where the data is extracted to - saved in the root of persistence location
            // this directory is deleted at end of the method
            String unzippedPath = BrooklynServerPaths.newMainPersistencePathResolver(mgmt()).dir(tempZipFile.getName()).resolve();

            // extract to the location specified
            ArchiveUtils.extractZip(new ZipFile(tempZipFile),unzippedPath);

            // create a temporary mgmt context and load the persistence data
            LocalManagementContext tempMgmt = new LocalManagementContext(BrooklynProperties.Factory.builderDefault().build());
            PersistenceObjectStore tempPersistenceStore = BrooklynPersistenceUtils.newPersistenceObjectStore(tempMgmt,null, unzippedPath + "/data/");
            tempPersistenceStore.prepareForSharedUse(PersistMode.REBIND,HighAvailabilityMode.AUTO);
            BrooklynMementoPersisterToObjectStore persister = new BrooklynMementoPersisterToObjectStore(
                    tempPersistenceStore, tempMgmt);
            PersistenceExceptionHandler persistenceExceptionHandler = PersistenceExceptionHandlerImpl.builder().build();
            RebindManager rebindManager = tempMgmt.getRebindManager();
            rebindManager.setPersister(persister, persistenceExceptionHandler);
            rebindManager.forcePersistNow(false, null);

            // create raw memento of persisted state to be imported
            BrooklynMementoRawData newMementoRawData = tempMgmt.getRebindManager().retrieveMementoRawData();

            // install bundles to active management context
            for (Map.Entry<String, ByteSource> bundleJar : newMementoRawData.getBundleJars().entrySet()){
                ReferenceWithError<OsgiBundleInstallationResult> result = ((ManagementContextInternal)mgmt()).getOsgiManager().get()
                        .install(InputStreamSource.of("Persistence import - bundle install", bundleJar.getValue().read()), "", false);

                if (result.hasError()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Unable to create, format '', returning 400: "+result.getError().getMessage(), result.getError());
                    }
                    String errorMsg = "";
                    if (result.getWithoutError()!=null) {
                        errorMsg = result.getWithoutError().getMessage();
                    } else {
                        errorMsg = Strings.isNonBlank(result.getError().getMessage()) ? result.getError().getMessage() : result.getError().toString();
                    }
                    throw new Exception(errorMsg);
                }
            }

            // write persisted items and rebind to load applications
            BrooklynMementoRawData.Builder result = BrooklynMementoRawData.builder();
            MementoSerializer<Object> rawSerializer = XmlMementoSerializerBuilder.from(mgmt())
                    .withBrooklynDeserializingClassRenames()
                    .build();
            RetryingMementoSerializer<Object> serializer = new RetryingMementoSerializer<Object>(rawSerializer, 1);

            result.planeId(mgmt().getManagementPlaneIdMaybe().orNull());
            result.entities(newMementoRawData.getEntities());
            result.locations(newMementoRawData.getLocations());
            result.policies(newMementoRawData.getPolicies());
            result.enrichers(newMementoRawData.getEnrichers());
            result.feeds(newMementoRawData.getFeeds());
            result.catalogItems(newMementoRawData.getCatalogItems());

            PersistenceObjectStore currentPersistenceStore = ((BrooklynMementoPersisterToObjectStore) mgmt().getRebindManager().getPersister()).getObjectStore();
            BrooklynPersistenceUtils.writeMemento(mgmt(),result.build(),currentPersistenceStore);
            mgmt().getRebindManager().rebind(mgmt().getCatalogClassLoader(),null, mgmt().getHighAvailabilityManager().getNodeState());

            // clean up the temporary management context
            rebindManager.stop();
            persister.stop(true);
            tempPersistenceStore.close();
            tempMgmt.terminate();

            // delete the dir with the import data
            FileUtils.deleteDirectory(new File(unzippedPath));
        } catch (Exception e){
            Exceptions.propagateIfFatal(e);
            ApiError.Builder error = ApiError.builder().errorCode(Response.Status.BAD_REQUEST);
            error.message(e.getMessage());
            return error.build().asJsonResponse();
        } finally {
            if (tempZipFile!=null) {
                try {
                    tempZipFile.delete();
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    log.warn("Failed to delete temp file "+tempZipFile+" (ignoring): "+e, e);
                }
            }
        }
        return Response.ok().build();
    }
}
