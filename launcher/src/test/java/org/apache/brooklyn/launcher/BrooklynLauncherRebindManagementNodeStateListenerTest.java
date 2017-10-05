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

import static org.apache.brooklyn.api.mgmt.ha.ManagementNodeState.INITIALIZING;
import static org.apache.brooklyn.api.mgmt.ha.ManagementNodeState.MASTER;
import static org.apache.brooklyn.api.mgmt.ha.ManagementNodeState.STANDBY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.mgmt.rebind.RebindExceptionHandler;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.mgmt.usage.ManagementNodeStateListenerTest.RecordingStaticManagementNodeStateListener;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class BrooklynLauncherRebindManagementNodeStateListenerTest extends AbstractBrooklynLauncherRebindTest {

    private AtomicReference<CountDownLatch> populateInitialCatalogOnlyLatch = new AtomicReference<>();
    private AtomicReference<CountDownLatch> populateInitialAndPersistedCatalogLatch = new AtomicReference<>();
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingStaticManagementNodeStateListener.clearInstances();
        populateInitialCatalogOnlyLatch.set(null);
        populateInitialAndPersistedCatalogLatch.set(null);
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingStaticManagementNodeStateListener.clearInstances();
    }

    @Override
    protected BrooklynLauncher newLauncherForTests(PersistMode persistMode, HighAvailabilityMode haMode) {
        BrooklynProperties brooklynProperties = LocalManagementContextForTests.builder(true).buildProperties();
        brooklynProperties.put(BrooklynServerConfig.MANAGEMENT_NODE_STATE_LISTENERS, RecordingStaticManagementNodeStateListener.class.getName());
        
        // If latches are set, then blocks startup
        CatalogInitialization catInit = new CatalogInitialization() {
            @Override
            public void populateInitialAndPersistedCatalog(ManagementNodeState mode, PersistedCatalogState persistedState, RebindExceptionHandler exceptionHandler, RebindLogger rebindLogger) {
                awaitIfNotNull(populateInitialAndPersistedCatalogLatch);
                super.populateInitialAndPersistedCatalog(mode, persistedState, exceptionHandler, rebindLogger);
            }
            @Override
            public void populateInitialCatalogOnly() {
                awaitIfNotNull(populateInitialCatalogOnlyLatch);
                super.populateInitialCatalogOnly();
            }
            private void awaitIfNotNull(AtomicReference<CountDownLatch> latchRef) {
                CountDownLatch latch = latchRef.get();
                if (latch != null) {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw Exceptions.propagate(e);
                    }
                }
            }
        };

        return super.newLauncherForTests(persistMode, haMode)
                .brooklynProperties(brooklynProperties)
                .catalogInitialization(catInit);
    }

    @Test
    public void testNotifiesWhenPersistenceOff() throws Exception {
        BrooklynLauncher launcher = newLauncherForTests(PersistMode.DISABLED, HighAvailabilityMode.DISABLED);
        launcher.start();
        RecordingStaticManagementNodeStateListener.getInstance().assertEventsEventually(ImmutableList.of(INITIALIZING, MASTER));
    }

    @Test
    public void testNotifiesOnRebind() throws Exception {
        // Starting with no persisted state
        BrooklynLauncher launcher = newLauncherForTests();
        launcher.start();
        RecordingStaticManagementNodeStateListener.getInstance().assertEventsEventually(ImmutableList.of(INITIALIZING, MASTER));
        RecordingStaticManagementNodeStateListener.clearInstances();

        launcher.terminate();

        // Starting with a populated persisted state dir
        BrooklynLauncher newLauncher = newLauncherForTests();
        newLauncher.start();
        RecordingStaticManagementNodeStateListener.getInstance().assertEventsEventually(ImmutableList.of(INITIALIZING, MASTER));
    }

    @Test
    public void testNotifiesOnHighAvailabilityPromotion() throws Exception {
        // Start master
        BrooklynLauncher launcher = newLauncherForTests(PersistMode.AUTO, HighAvailabilityMode.AUTO);
        launcher.start();
        RecordingStaticManagementNodeStateListener.getInstance().assertEventsEventually(ImmutableList.of(INITIALIZING, STANDBY, MASTER));
        RecordingStaticManagementNodeStateListener.clearInstances();

        // Start standby
        BrooklynLauncher launcher2 = newLauncherForTests(PersistMode.AUTO, HighAvailabilityMode.AUTO);
        launcher2.start();
        RecordingStaticManagementNodeStateListener.getInstance().assertEventsEventually(ImmutableList.of(INITIALIZING, STANDBY));
        
        // Promote standby to master
        launcher.terminate();
        RecordingStaticManagementNodeStateListener.getInstance().assertEventsEventually(ImmutableList.of(INITIALIZING, STANDBY, MASTER));
    }

    @Test
    public void testNotifiesOfMasterOnlyAfterRebindingAppsWhenHaDisabled() throws Exception {
        runNotifiesOfMasterOnlyAfterRebindingApps(HighAvailabilityMode.DISABLED);
    }
    
    @Test
    public void testNotifiesOfMasterOnlyAfterRebindingAppsWhenHaEnabled() throws Exception {
        runNotifiesOfMasterOnlyAfterRebindingApps(HighAvailabilityMode.AUTO);
    }

    protected void runNotifiesOfMasterOnlyAfterRebindingApps(HighAvailabilityMode haMode) throws Exception {
        // Populate the persisted state with an app
        BrooklynLauncher origLauncher = newLauncherForTests(PersistMode.AUTO, haMode);
        origLauncher.start();
        TestApplication origApp = origLauncher.getManagementContext().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        origLauncher.terminate();
        RecordingStaticManagementNodeStateListener.clearInstances();
        
        // Start an app async (causing its rebind to block until we release the latch)
        populateInitialAndPersistedCatalogLatch.set(new CountDownLatch(1));
        
        BrooklynLauncher launcher2 = newLauncherForTests(PersistMode.AUTO, haMode);
        Thread t = new Thread() {
            public void run() {
                launcher2.start();
            }
        };
        try {
            List<ManagementNodeState> expectedInitialStates = (haMode == HighAvailabilityMode.DISABLED) ?
                    ImmutableList.of(INITIALIZING) :
                    ImmutableList.of(INITIALIZING, STANDBY);
            List<ManagementNodeState> expectedFinalStates = (haMode == HighAvailabilityMode.DISABLED) ?
                    ImmutableList.of(INITIALIZING, MASTER) :
                    ImmutableList.of(INITIALIZING, STANDBY, MASTER);

            t.start();
            RecordingStaticManagementNodeStateListener.getInstanceEventually().assertEventsEventually(expectedInitialStates);
            RecordingStaticManagementNodeStateListener.getInstance().assertEventsContinually(expectedInitialStates);
            assertTrue(t.isAlive());
            
            // Let it complete; now expect to get the callback that we are master
            populateInitialAndPersistedCatalogLatch.get().countDown();
            RecordingStaticManagementNodeStateListener.getInstance().assertEventsEventually(expectedFinalStates);
            Application newApp = Iterables.getOnlyElement(launcher2.getManagementContext().getApplications());
            assertEquals(newApp.getId(), origApp.getId());
            
            t.join(Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds());
        } finally {
            t.interrupt();
        }
    }
}
