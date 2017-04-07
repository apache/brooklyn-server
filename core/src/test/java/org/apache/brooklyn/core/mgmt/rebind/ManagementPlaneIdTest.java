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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.FileBasedObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.server.BrooklynServerPaths;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagementPlaneIdTest {
    private File mementoDir;

    protected ClassLoader classLoader = getClass().getClassLoader();

    private Collection<ManagementContext> managementContextForTermination;

    @BeforeMethod
    public void setUp() {
        mementoDir = Os.newTempDir(getClass());
        managementContextForTermination = new ArrayList<>();
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (managementContextForTermination != null) {
            for (ManagementContext mgmt : managementContextForTermination) {
                Entities.destroyAll(mgmt);
            }
        }
        if (mementoDir != null) FileBasedObjectStore.deleteCompletely(mementoDir);
    }

    @Test
    public void testUninitializedThrows() {
        ManagementContext mgmt = new LocalManagementContext(BrooklynProperties.Factory.newEmpty());
        assertFalse(mgmt.getOptionalManagementPlaneId().isPresent(), "expected managementPlaneId to be absent");
    }
    
    @Test
    public void testPlaneIdPersists() throws Exception {
        final ManagementContext mgmt = createManagementContext(PersistMode.AUTO, HighAvailabilityMode.DISABLED);
        checkPlaneIdPersisted(mgmt);
    }

    @Test
    public void testPlaneIdRolledBack() throws Exception {
        final LocalManagementContext mgmt = createManagementContext(PersistMode.AUTO, HighAvailabilityMode.AUTO);

        checkPlaneIdPersisted(mgmt);
        final String oldPlaneId = mgmt.getOptionalManagementPlaneId().get();
        mgmt.setManagementPlaneId(Strings.makeRandomId(8));
        assertNotEquals(oldPlaneId, mgmt.getOptionalManagementPlaneId().get());
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertEquals(oldPlaneId, mgmt.getOptionalManagementPlaneId().get());
            }
        });
    }

    @Test
    public void testColdRebindInitialisesPlaneId() throws Exception {
        final LocalManagementContext origMgmt = createManagementContext(PersistMode.AUTO, HighAvailabilityMode.DISABLED);
        checkPlaneIdPersisted(origMgmt);
        Entities.destroyAll(origMgmt);

        LocalManagementContext rebindMgmt = createManagementContext(PersistMode.AUTO, HighAvailabilityMode.DISABLED);

        assertEquals(origMgmt.getOptionalManagementPlaneId(), rebindMgmt.getOptionalManagementPlaneId());
    }


    @DataProvider
    public Object[][] haSlaveModes() {
        return new Object[][] {
            {HighAvailabilityMode.AUTO},
            {HighAvailabilityMode.STANDBY},
            {HighAvailabilityMode.HOT_STANDBY},
            {HighAvailabilityMode.HOT_BACKUP},
        };
    }

    @Test(dataProvider="haSlaveModes")
    public void testHaRebindInitialisesPlaneId(HighAvailabilityMode slaveMode) throws Exception {
        final LocalManagementContext origMgmt = createManagementContext(PersistMode.AUTO, HighAvailabilityMode.AUTO);
        final LocalManagementContext rebindMgmt = createManagementContext(PersistMode.AUTO, slaveMode);

        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertEquals(origMgmt.getOptionalManagementPlaneId(), rebindMgmt.getOptionalManagementPlaneId());
            }
        });
    }

    @Test
    public void testHaFailoverKeepsPlaneId() throws Exception {
        final LocalManagementContext origMgmt = createManagementContext(PersistMode.AUTO, HighAvailabilityMode.MASTER);
        final LocalManagementContext rebindMgmt = createManagementContext(PersistMode.AUTO, HighAvailabilityMode.STANDBY);

        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertEquals(origMgmt.getOptionalManagementPlaneId(), rebindMgmt.getOptionalManagementPlaneId());
            }
        });

        Entities.destroyAll(origMgmt);

        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertEquals(rebindMgmt.getHighAvailabilityManager().getNodeState(), ManagementNodeState.MASTER);
                if (rebindMgmt.getRebindManager().isAwaitingInitialRebind()) {
                    throw new IllegalStateException("still rebinding");
                }
            }
        });

        assertEquals(origMgmt.getOptionalManagementPlaneId(), rebindMgmt.getOptionalManagementPlaneId());
    }
    
    @Test
    public void testPlaneIdBackedUp() throws Exception {
        final LocalManagementContext origMgmt = createManagementContext(PersistMode.AUTO, HighAvailabilityMode.AUTO);
        checkPlaneIdPersisted(origMgmt);
        Entities.destroyAll(origMgmt);

        LocalManagementContext rebindMgmt = createManagementContextWithBackups(PersistMode.AUTO, HighAvailabilityMode.AUTO);

        assertEquals(origMgmt.getOptionalManagementPlaneId(), rebindMgmt.getOptionalManagementPlaneId());

        String backupContainer = BrooklynServerPaths.newBackupPersistencePathResolver(rebindMgmt).resolve();
        
        File[] promotionFolders = new File(backupContainer).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("promotion");
            }
        });
        
        assertEquals(promotionFolders.length, 1);
        
        File planeIdFile = new File(promotionFolders[0], BrooklynMementoPersisterToObjectStore.PLANE_ID_FILE_NAME);
        String planeId = readFile(planeIdFile);
        assertEquals(origMgmt.getOptionalManagementPlaneId().get(), planeId);
    }
    
    @Test
    public void testFullPersist() throws Exception {
        final LocalManagementContext origMgmt = createManagementContext(PersistMode.DISABLED, HighAvailabilityMode.DISABLED);
        origMgmt.getRebindManager().getPersister().enableWriteAccess();
        origMgmt.getRebindManager().forcePersistNow(true, null);
        checkPlaneIdPersisted(origMgmt);
    }
    
    protected LocalManagementContext createManagementContext(PersistMode persistMode, HighAvailabilityMode haMode) {
        return createManagementContext(persistMode, haMode, false);
    }
    
    protected LocalManagementContext createManagementContextWithBackups(PersistMode persistMode, HighAvailabilityMode haMode) {
        return createManagementContext(persistMode, haMode, true);
    }
    
    protected LocalManagementContext createManagementContext(PersistMode persistMode, HighAvailabilityMode haMode, boolean backedUp) {
        BrooklynProperties props = BrooklynProperties.Factory.newEmpty();
        props.put(BrooklynServerConfig.PERSISTENCE_BACKUPS_DIR, mementoDir);
        LocalManagementContext mgmt = RebindTestUtils.managementContextBuilder(mementoDir, classLoader)
                .persistPeriodMillis(1)
                .persistMode(persistMode)
                .haMode(haMode)
                .enablePersistenceBackups(backedUp)
                .emptyCatalog(true)
                .properties(props)
                .enableOsgi(false)
                .buildStarted();
        markForTermination(mgmt);
        return mgmt;
    }

    private void markForTermination(ManagementContext mgmt) {
        managementContextForTermination.add(mgmt);
    }

    protected static String readFile(File planeIdFile) throws IOException {
        return new String(Files.readAllBytes(planeIdFile.toPath()), StandardCharsets.UTF_8);
    }

    protected void checkPlaneIdPersisted(final ManagementContext mgmt) {
        final File planeIdFile = new File(mementoDir, BrooklynMementoPersisterToObjectStore.PLANE_ID_FILE_NAME);
        Asserts.succeedsEventually(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String planeId = readFile(planeIdFile);
                assertEquals(mgmt.getOptionalManagementPlaneId().get(), planeId);
                return null;
            }
        });
    }

}
