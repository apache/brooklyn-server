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

import java.util.Set;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.rebind.PersistenceExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.RebindManager;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.BrooklynPersistenceUtils;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore;
import org.apache.brooklyn.core.mgmt.rebind.PersistenceExceptionHandlerImpl;
import org.apache.brooklyn.core.mgmt.rebind.RebindManagerImpl;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CleanOrphanedLocationsIntegrationTest extends AbstractCleanOrphanedStateTest {

    private String PERSISTED_STATE_PATH_WITH_ORPHANED_LOCATIONS = "/orphaned-locations-test-data/data-with-orphaned-locations";
    private String PERSISTED_STATE_PATH_WITH_MULTIPLE_LOCATIONS_OCCURRENCE = "/orphaned-locations-test-data/fake-multiple-location-for-multiple-search-tests";
    private String PERSISTED_STATE_PATH_WITHOUT_ORPHANED_LOCATIONS = "/orphaned-locations-test-data/data-without-orphaned-locations";
    private String PERSISTED_STATE_DESTINATION_PATH = "/orphaned-locations-test-data/copy-persisted-state-destination";


    private String persistenceDirWithOrphanedLocations;
    private String persistenceDirWithoutOrphanedLocations;
    private String persistenceDirWithMultipleLocationsOccurrence;
    private String destinationDir;
    private String persistenceLocation;

    private ManagementContext managementContext;

    @BeforeMethod(alwaysRun = true)
    public void initialize() throws Exception {
        persistenceDirWithOrphanedLocations = getClass().getResource(PERSISTED_STATE_PATH_WITH_ORPHANED_LOCATIONS).getFile();
        persistenceDirWithoutOrphanedLocations = getClass().getResource(PERSISTED_STATE_PATH_WITHOUT_ORPHANED_LOCATIONS).getFile();
        persistenceDirWithMultipleLocationsOccurrence = getClass().getResource(PERSISTED_STATE_PATH_WITH_MULTIPLE_LOCATIONS_OCCURRENCE).getFile();

        destinationDir = getClass().getResource(PERSISTED_STATE_DESTINATION_PATH).getFile();
    }

    private void initManagementContextAndPersistence(String persistenceDir) {

        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.builderDefault().build();
        brooklynProperties.put(BrooklynServerConfig.MGMT_BASE_DIR.getName(), "");

        managementContext = new LocalManagementContext(brooklynProperties);

        persistenceDir = BrooklynServerPaths.newMainPersistencePathResolver(brooklynProperties).location(persistenceLocation).dir(persistenceDir).resolve();
        PersistenceObjectStore objectStore = BrooklynPersistenceUtils.newPersistenceObjectStore(managementContext, persistenceLocation, persistenceDir,
                PersistMode.AUTO, HighAvailabilityMode.HOT_STANDBY);

        BrooklynMementoPersisterToObjectStore persister = new BrooklynMementoPersisterToObjectStore(
                objectStore,
                ((ManagementContextInternal)managementContext).getBrooklynProperties(),
                managementContext.getCatalogClassLoader());

        RebindManager rebindManager = managementContext.getRebindManager();

        PersistenceExceptionHandler persistenceExceptionHandler = PersistenceExceptionHandlerImpl.builder().build();
        ((RebindManagerImpl) rebindManager).setPeriodicPersistPeriod(Duration.ONE_SECOND);
        rebindManager.setPersister(persister, persistenceExceptionHandler);
    }

    @Test
    public void testSelectionWithOrphanedLocationsInData() throws Exception {
        final Set<String> orphanedLocations = MutableSet.of(
                "msyp655po0",
                "ppamsemxgo"
        );

        initManagementContextAndPersistence(persistenceDirWithOrphanedLocations);
        BrooklynMementoRawData mementoRawData = managementContext.getRebindManager().retrieveMementoRawData();

        assertTransformDeletes(new Deletions().locations(orphanedLocations), mementoRawData);
    }

    @Test
    public void testSelectionWithoutOrphanedLocationsInData() throws Exception {
        initManagementContextAndPersistence(persistenceDirWithoutOrphanedLocations);
        BrooklynMementoRawData mementoRawData = managementContext.getRebindManager().retrieveMementoRawData();

        assertTransformIsNoop(mementoRawData);
    }

    @Test
    public void testCleanedCopiedPersistedState() throws Exception {
        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .webconsole(false)
                .brooklynProperties(OsgiManager.USE_OSGI, false)
                .persistMode(PersistMode.AUTO)
                .persistenceDir(persistenceDirWithOrphanedLocations)
                .persistenceLocation(persistenceLocation)
                .highAvailabilityMode(HighAvailabilityMode.DISABLED);

        try {
            launcher.cleanOrphanedState(destinationDir, null);
        } finally {
            launcher.terminate();
        }

        initManagementContextAndPersistence(destinationDir);
        BrooklynMementoRawData mementoRawDataFromCleanedState = managementContext.getRebindManager().retrieveMementoRawData();
        Asserts.assertTrue(mementoRawDataFromCleanedState.getEntities().size() != 0);
        Asserts.assertTrue(mementoRawDataFromCleanedState.getLocations().size() != 0);

        initManagementContextAndPersistence(persistenceDirWithoutOrphanedLocations);
        BrooklynMementoRawData mementoRawData = managementContext.getRebindManager().retrieveMementoRawData();

        assertRawData(mementoRawData, mementoRawDataFromCleanedState);
    }

    @Test
    public void testMultipleLocationOccurrenceInEntity() throws Exception {
        initManagementContextAndPersistence(persistenceDirWithMultipleLocationsOccurrence);
        BrooklynMementoRawData mementoRawData = managementContext.getRebindManager().retrieveMementoRawData();
        
        assertTransformIsNoop(mementoRawData);
    }

    @AfterMethod
    public void cleanCopiedPersistedState() {

    }
}
