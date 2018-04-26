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
package org.apache.brooklyn.core.mgmt.rebind;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.rebind.RebindExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoManifest;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.FileBasedObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class RebindTestFixture<T extends StartableApplication> {

    private static final Logger LOG = LoggerFactory.getLogger(RebindTestFixture.class);

    protected static final Duration TIMEOUT_MS = Duration.TEN_SECONDS;

    protected ClassLoader classLoader = getClass().getClassLoader();
    protected LocalManagementContext origManagementContext;
    protected File mementoDir;
    protected File mementoDirBackup;
    
    protected T origApp;
    protected T newApp;
    protected ManagementContext newManagementContext;


    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        mementoDir = Os.newTempDir(getClass());
        File mementoDirParent = mementoDir.getParentFile();
        mementoDirBackup = new File(mementoDirParent, mementoDir.getName()+"."+Identifiers.makeRandomId(4)+".bak");

        origApp = null;
        newApp = null;
        newManagementContext = null;
        
        origManagementContext = createOrigManagementContext();
        origApp = createApp();
        
        LOG.info("Test "+getClass()+" persisting to "+mementoDir);
    }

    protected BrooklynProperties createBrooklynProperties() {
        BrooklynProperties result;
        if (useLiveManagementContext()) {
            result = BrooklynProperties.Factory.newDefault();
        } else {
            result = BrooklynProperties.Factory.newEmpty();
        }
        // By default, will not persist "org.apache.brooklyn.*" bundles; therefore explicitly whitelist 
        // such things commonly used in tests.
        String whitelistRegex = OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FULL 
                + "|" + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SYMBOLIC_NAME_FULL;
        result.put(BrooklynServerConfig.PERSIST_MANAGED_BUNDLE_WHITELIST_REGEX, whitelistRegex);
        return result;
    }

    /** @return A started management context */
    protected LocalManagementContext createOrigManagementContext() {
        return RebindTestUtils.managementContextBuilder(mementoDir, classLoader)
                .persistPeriodMillis(getPersistPeriodMillis())
                .haMode(getHaMode())
                .forLive(useLiveManagementContext())
                .enablePersistenceBackups(enablePersistenceBackups())
                .emptyCatalog(useEmptyCatalog())
                .properties(createBrooklynProperties())
                .setOsgiEnablementAndReuse(useOsgi(), !disallowOsgiReuse())
                .buildStarted();
    }

    protected HighAvailabilityMode getHaMode() {
        return HighAvailabilityMode.DISABLED;
    }
    
    protected boolean enablePersistenceBackups() {
        return true;
    }
    
    /** As {@link #createNewManagementContext(File)} using the default memento dir */
    protected LocalManagementContext createNewManagementContext() {
        return createNewManagementContext(mementoDir, null);
    }
    
    /** @return An unstarted management context using the specified mementoDir (or default if null) */
    protected LocalManagementContext createNewManagementContext(File mementoDir, Map<?, ?> additionalProperties) {
        return createNewManagementContext(mementoDir, getHaMode(), additionalProperties);
    }

    protected LocalManagementContext createNewManagementContext(File mementoDir, HighAvailabilityMode haMode, Map<?, ?> additionalProperties) {
        if (mementoDir==null) mementoDir = this.mementoDir;
        BrooklynProperties brooklynProperties = createBrooklynProperties();
        if (additionalProperties != null) {
            brooklynProperties.addFrom(additionalProperties);
        }
        return RebindTestUtils.managementContextBuilder(mementoDir, classLoader)
                .forLive(useLiveManagementContext())
                .haMode(haMode)
                .emptyCatalog(useEmptyCatalog())
                .properties(brooklynProperties)
                .setOsgiEnablementAndReuse(useOsgi(), !disallowOsgiReuse())
                .buildUnstarted();
    }

    /** terminates the original management context (not destroying items) and points it at the new one (and same for apps); 
     * then clears the variables for the new one, so you can re-rebind */
    protected void switchOriginalToNewManagementContext() {
        origManagementContext.getRebindManager().stopPersistence();
        for (Application e: origManagementContext.getApplications()) ((Startable)e).stop();
        waitForTaskCountToBecome(origManagementContext, 0, true);
        origManagementContext.terminate();
        origManagementContext = (LocalManagementContext) newManagementContext;
        origApp = newApp;
        newManagementContext = null;
        newApp = null;
    }

    protected ManagementContext mgmt() {
        return (newManagementContext != null) ? newManagementContext : origManagementContext;
    }
    
    protected T app() {
        return (newApp != null) ? newApp : origApp;
    }
    
    public static void waitForTaskCountToBecome(final ManagementContext mgmt, final int allowedMax) {
        waitForTaskCountToBecome(mgmt, allowedMax, false);
    }
    
    public static void waitForTaskCountToBecome(final ManagementContext mgmt, final int allowedMax, final boolean skipKnownBackgroundTasks) {
        Repeater.create().every(Duration.millis(20)).limitTimeTo(Duration.TEN_SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ((LocalManagementContext)mgmt).getGarbageCollector().gcIteration();
                long taskCountAfterAtOld = ((BasicExecutionManager)mgmt.getExecutionManager()).getNumIncompleteTasks();
                List<Task<?>> tasks = ((BasicExecutionManager)mgmt.getExecutionManager()).getAllTasks();
                int unendedTasks = 0, extraAllowedMax = 0;
                for (Task<?> t: tasks) {
                    if (!t.isDone()) {
                        if (skipKnownBackgroundTasks) {
                            if (t.toString().indexOf("ssh-location cache cleaner")>=0) {
                                extraAllowedMax++;
                            }
                        }
                        unendedTasks++;
                    }
                }
                LOG.info("Count of incomplete tasks now "+taskCountAfterAtOld+", "+unendedTasks+" unended"
                    + (extraAllowedMax>0 ? " ("+extraAllowedMax+" allowed)" : "")
                    + "; tasks remembered are: "+
                    tasks);
                return taskCountAfterAtOld<=allowedMax+extraAllowedMax;
            }
        }).runRequiringTrue();
    }

    protected boolean useLiveManagementContext() {
        return false;
    }

    protected boolean useEmptyCatalog() {
        return true;
    }

    protected boolean useOsgi() {
        return false;
    }
    
    protected boolean disallowOsgiReuse() {
        return false;
    }

    protected int getPersistPeriodMillis() {
        return 1;
    }
    
    /** optionally, create the app as part of every test; can be no-op if tests wish to set origApp themselves */
    protected abstract T createApp();

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (origApp != null) Entities.destroyAll(origApp.getManagementContext());
        if (newApp != null) Entities.destroyAll(newApp.getManagementContext());
        if (newManagementContext != null) Entities.destroyAll(newManagementContext);
        origApp = null;
        newApp = null;
        newManagementContext = null;

        if (origManagementContext != null) Entities.destroyAll(origManagementContext);
        if (mementoDir != null) FileBasedObjectStore.deleteCompletely(mementoDir);
        if (mementoDirBackup != null) FileBasedObjectStore.deleteCompletely(mementoDir);
        origManagementContext = null;
    }

    /** rebinds, and sets newApp */
    protected T rebind() throws Exception {
        return rebind(RebindOptions.create());
    }

    @SuppressWarnings("unchecked")
    protected T rebind(RebindOptions options) throws Exception {
        if (newApp != null || newManagementContext != null) {
            throw new IllegalStateException("already rebound - use switchOriginalToNewManagementContext() if you are trying to rebind multiple times");
        }
        
        options = RebindOptions.create(options);
        if (options.classLoader == null) options.classLoader(classLoader);
        if (options.mementoDir == null) options.mementoDir(mementoDir);
        if (options.origManagementContext == null) options.origManagementContext(origManagementContext);
        if (options.newManagementContext == null) options.newManagementContext(createNewManagementContext(options.mementoDir, options.additionalProperties));
        if (options.applicationChooserOnRebind == null && origApp != null) options.applicationChooserOnRebind(EntityPredicates.idEqualTo(origApp.getId()));
        
        RebindTestUtils.stopPersistence(options.origManagementContext);
        
        newManagementContext = options.newManagementContext;
        newApp = (T) RebindTestUtils.rebind(options);
        return newApp;
    }

    protected ManagementContext hotStandby() throws Exception {
        return hotStandby(RebindOptions.create());
    }

    protected ManagementContext hotStandby(RebindOptions options) throws Exception {
        if (newApp != null || newManagementContext != null) {
            throw new IllegalStateException("already rebound - use switchOriginalToNewManagementContext() if you are trying to rebind multiple times");
        }
        if (options.haMode != null && options.haMode != HighAvailabilityMode.HOT_STANDBY) {
            throw new IllegalStateException("hotStandby called, but with HA Mode set to "+options.haMode);
        }
        if (options.terminateOrigManagementContext) {
            throw new IllegalStateException("hotStandby called with terminateOrigManagementContext==true");
        }
        
        options = RebindOptions.create(options);
        options.terminateOrigManagementContext(false);
        if (options.haMode == null) options.haMode(HighAvailabilityMode.HOT_STANDBY);
        if (options.classLoader == null) options.classLoader(classLoader);
        if (options.mementoDir == null) options.mementoDir(mementoDir);
        if (options.origManagementContext == null) options.origManagementContext(origManagementContext);
        if (options.newManagementContext == null) options.newManagementContext(createNewManagementContext(options.mementoDir, options.haMode, options.additionalProperties));
        
        RebindTestUtils.stopPersistence(origApp);
        
        newManagementContext = options.newManagementContext;
        RebindTestUtils.rebind(options);
        return newManagementContext;
    }

    /**
     * Dumps out the persisted mementos that are at the given directory.
     * 
     * @param dir The directory containing the persisted state (e.g. {@link #mementoDir} or {@link #mementoDirBackup})
     */
    protected void dumpMementoDir(File dir) {
        RebindTestUtils.dumpMementoDir(dir);
    }
    
    protected BrooklynMementoManifest loadMementoManifest() throws Exception {
        return loadFromPersistedState((persister) -> {
            RebindExceptionHandler exceptionHandler = new RecordingRebindExceptionHandler(RebindManager.RebindFailureMode.FAIL_AT_END, RebindManager.RebindFailureMode.FAIL_AT_END);
            try {
                return persister.loadMementoManifest(null, exceptionHandler);
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        });
    }
    
    protected BrooklynMementoRawData loadMementoRawData() throws Exception {
        return loadFromPersistedState((persister) -> {
            RebindExceptionHandler exceptionHandler = new RecordingRebindExceptionHandler(RebindManager.RebindFailureMode.FAIL_AT_END, RebindManager.RebindFailureMode.FAIL_AT_END);
            return persister.loadMementoRawData(exceptionHandler);
        });
    }
    
    protected <U> U loadFromPersistedState(Function<BrooklynMementoPersisterToObjectStore, U> loader) throws Exception {
        newManagementContext = createNewManagementContext();
        FileBasedObjectStore objectStore = new FileBasedObjectStore(mementoDir);
        objectStore.injectManagementContext(newManagementContext);
        objectStore.prepareForSharedUse(PersistMode.AUTO, HighAvailabilityMode.DISABLED);
        BrooklynMementoPersisterToObjectStore persister = new BrooklynMementoPersisterToObjectStore(
                objectStore,
                newManagementContext,
                classLoader);
        U result;
        try {
            result = loader.apply(persister);
        } finally {
            persister.stop(false);
        }
        return result;
    }
    
//    protected void assertCatalogContains(BrooklynCatalog catalog, CatalogItem<?, ?> item) {
//        CatalogItem<?, ?> found = catalog.getCatalogItem(item.getSymbolicName(), item.getVersion());
//        assertNotNull(found);
//        assertCatalogItemsEqual(found, item);
//    }
//    
//    protected void assertCatalogDoesNotContain(BrooklynCatalog catalog, String symbolicName, String version) {
//        CatalogItem<?, ?> found = catalog.getCatalogItem(symbolicName, version);
//        assertNull(found);
//    }
}
