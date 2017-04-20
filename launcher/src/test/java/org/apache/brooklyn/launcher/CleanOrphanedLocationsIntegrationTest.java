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
package org.apache.brooklyn.launcher;

import java.io.File;
import java.util.Set;
import java.util.zip.ZipFile;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.rebind.PersistenceExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.BrooklynPersistenceUtils;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore;
import org.apache.brooklyn.core.mgmt.rebind.PersistenceExceptionHandlerImpl;
import org.apache.brooklyn.core.mgmt.rebind.RebindManagerImpl;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.file.ArchiveUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;

public class CleanOrphanedLocationsIntegrationTest extends AbstractCleanOrphanedStateTest {

    private static final Logger LOG = LoggerFactory.getLogger(CleanOrphanedLocationsIntegrationTest.class);

    private final String PERSISTED_STATE_PATH_WITH_ORPHANED_LOCATIONS = "/orphaned-locations-test-data/data-with-orphaned-locations";
    private final String PERSISTED_STATE_PATH_WITH_MULTIPLE_LOCATIONS_OCCURRENCE = "/orphaned-locations-test-data/fake-multiple-location-for-multiple-search-tests";
    private final String PERSISTED_STATE_PATH_WITHOUT_ORPHANED_LOCATIONS = "/orphaned-locations-test-data/data-without-orphaned-locations";

    private String persistenceDirWithOrphanedLocations;
    private String persistenceDirWithoutOrphanedLocations;
    private String persistenceDirWithMultipleLocationsOccurrence;
    private File destinationDir;
    private Set<ManagementContext> mgmts;
    private ManagementContext mgmt;

    private String copyResourcePathToTempPath(String resourcePath) {
        BundleMaker bm = new BundleMaker(ResourceUtils.create(this));
        bm.setDefaultClassForLoading(CleanOrphanedLocationsIntegrationTest.class);
        File jar = bm.createJarFromClasspathDir(resourcePath);
        File output = Os.newTempDir("brooklyn-test-resouce-from-"+resourcePath);
        try {
            ArchiveUtils.extractZip(new ZipFile(jar), output.getAbsolutePath());
            return output.getAbsolutePath();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        persistenceDirWithOrphanedLocations = copyResourcePathToTempPath(PERSISTED_STATE_PATH_WITH_ORPHANED_LOCATIONS);
        persistenceDirWithoutOrphanedLocations = copyResourcePathToTempPath(PERSISTED_STATE_PATH_WITHOUT_ORPHANED_LOCATIONS);
        persistenceDirWithMultipleLocationsOccurrence = copyResourcePathToTempPath(PERSISTED_STATE_PATH_WITH_MULTIPLE_LOCATIONS_OCCURRENCE);

        destinationDir = Os.newTempDir(getClass());
        
        mgmts = Sets.newLinkedHashSet();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        
        // contents of method copied from BrooklynMgmtUnitTestSupport
        for (ManagementContext mgmt : mgmts) {
            try {
                if (mgmt != null) Entities.destroyAll(mgmt);
            } catch (Throwable t) {
                LOG.error("Caught exception in tearDown method", t);
                // we should fail here, except almost always that masks a primary failure in the test itself,
                // so it would be extremely unhelpful to do so. if we could check if test has not already failed,
                // that would be ideal, but i'm not sure if that's possible with TestNG. ?
            }
        }
        if (destinationDir != null) Os.deleteRecursively(destinationDir);
        mgmts.clear();
    }
    
    private void initManagementContextAndPersistence(String persistenceDir) {
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.builderDefault().build();
        brooklynProperties.put(BrooklynServerConfig.MGMT_BASE_DIR.getName(), "");
        brooklynProperties.put(BrooklynServerConfig.OSGI_CACHE_DIR, "target/" + BrooklynServerConfig.OSGI_CACHE_DIR.getDefaultValue());

        mgmt = LocalManagementContextForTests.newInstance(brooklynProperties);
        mgmts.add(mgmt);
        
        persistenceDir = BrooklynServerPaths.newMainPersistencePathResolver(brooklynProperties).dir(persistenceDir).resolve();
        PersistenceObjectStore objectStore = BrooklynPersistenceUtils.newPersistenceObjectStore(mgmt, null, persistenceDir,
                PersistMode.AUTO, HighAvailabilityMode.HOT_STANDBY);

        BrooklynMementoPersisterToObjectStore persister = new BrooklynMementoPersisterToObjectStore(
                objectStore, mgmt);

        RebindManager rebindManager = mgmt.getRebindManager();

        PersistenceExceptionHandler persistenceExceptionHandler = PersistenceExceptionHandlerImpl.builder().build();
        ((RebindManagerImpl) rebindManager).setPeriodicPersistPeriod(Duration.ONE_SECOND);
        rebindManager.setPersister(persister, persistenceExceptionHandler);
        ((RebindManagerImpl) rebindManager).forcePersistNow();
    }

    @Test
    public void testSelectionWithOrphanedLocationsInData() throws Exception {
        final Set<String> orphanedLocations = MutableSet.of(
                "msyp655po0",
                "ppamsemxgo"
        );

        initManagementContextAndPersistence(persistenceDirWithOrphanedLocations);
        BrooklynMementoRawData mementoRawData = mgmt.getRebindManager().retrieveMementoRawData();

        assertTransformDeletes(new Deletions().locations(orphanedLocations), mementoRawData);
    }

    @Test
    public void testSelectionWithoutOrphanedLocationsInData() throws Exception {
        initManagementContextAndPersistence(persistenceDirWithoutOrphanedLocations);
        BrooklynMementoRawData mementoRawData = mgmt.getRebindManager().retrieveMementoRawData();

        assertTransformIsNoop(mementoRawData);
    }

    @Test
    public void testCleanedCopiedPersistedState() throws Exception {
        LOG.info(JavaClassNames.niceClassAndMethod()+" taking persistence from "+persistenceDirWithOrphanedLocations);
        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .webconsole(false)
                .brooklynProperties(OsgiManager.USE_OSGI, false)
                .persistMode(PersistMode.AUTO)
                .persistenceDir(persistenceDirWithOrphanedLocations)
                .highAvailabilityMode(HighAvailabilityMode.DISABLED);

        ManagementContext mgmtForCleaning = null;
        try {
            launcher.cleanOrphanedState(destinationDir.getAbsolutePath(), null);
            mgmtForCleaning = launcher.getManagementContext();
        } finally {
            launcher.terminate();
            if (mgmtForCleaning != null) Entities.destroyAll(mgmtForCleaning);
        }

        initManagementContextAndPersistence(destinationDir.getAbsolutePath());
        BrooklynMementoRawData mementoRawDataFromCleanedState = mgmt.getRebindManager().retrieveMementoRawData();
        Asserts.assertTrue(mementoRawDataFromCleanedState.getEntities().size() != 0);
        Asserts.assertTrue(mementoRawDataFromCleanedState.getLocations().size() != 0);

        initManagementContextAndPersistence(persistenceDirWithoutOrphanedLocations);
        BrooklynMementoRawData mementoRawData = mgmt.getRebindManager().retrieveMementoRawData();

        assertRawData(mementoRawData, mementoRawDataFromCleanedState);
    }

    @Test
    public void testMultipleLocationOccurrenceInEntity() throws Exception {
        initManagementContextAndPersistence(persistenceDirWithMultipleLocationsOccurrence);
        BrooklynMementoRawData mementoRawData = mgmt.getRebindManager().retrieveMementoRawData();
        
        assertTransformIsNoop(mementoRawData);
    }
}
