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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.os.Os;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;

/**
 * See {@link CleanOrphanedLocationsTest} for more thorough testing. This test is to ensure that
 * it works through the launcher.
 */
public class BrooklynLauncherCleanStateTest extends AbstractBrooklynLauncherRebindTestFixture {

    @Override
    protected String newTempPersistenceContainerName() {
        File persistenceDirF = Files.createTempDir();
        Os.deleteOnExitRecursively(persistenceDirF);
        return persistenceDirF.getAbsolutePath();
    }
    
    @Test(groups="Integration")
    public void testCleanStateState() throws Exception {
        final AtomicReference<Entity> appToKeep = new AtomicReference<>();
        final AtomicReference<Location> locToKeep = new AtomicReference<>();
        final AtomicReference<Location> locToDelete = new AtomicReference<>();
        populatePersistenceDir(persistenceDir, new Function<ManagementContext, Void>() {
            @Override
            public Void apply(ManagementContext mgmt) {
                TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
                SshMachineLocation loc = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
                SshMachineLocation loc2 = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
                app.addLocations(ImmutableList.of(loc));
                appToKeep.set(app);
                locToKeep.set(loc);
                locToDelete.set(loc2);
                return null;
            }});

        File destinationDir = Files.createTempDir();
        String destination = destinationDir.getAbsolutePath();
        String destinationLocation = null; // i.e. file system, rather than object store
        try {
            // Clean the state
            BrooklynLauncher launcher = newLauncherDefault(PersistMode.AUTO)
                    .highAvailabilityMode(HighAvailabilityMode.MASTER)
                    .restServer(false);
            launcher.cleanOrphanedState(destination, destinationLocation);
            launcher.terminate();

            // Sanity checks (copied from BrooklynLauncherRebindTestToFiles#testCopyPersistedState)
            File entities = new File(Os.mergePaths(destination), "entities");
            assertTrue(entities.isDirectory(), "entities directory should exist");
            assertEquals(entities.listFiles().length, 1, "entities directory should contain one file (contained: "+
                    Joiner.on(", ").join(entities.listFiles()) +")");

            File nodes = new File(Os.mergePaths(destination, "nodes"));
            assertTrue(nodes.isDirectory(), "nodes directory should exist");
            assertNotEquals(nodes.listFiles().length, 0, "nodes directory should not be empty");

            // Should now have a usable copy in the destinationDir
            newLauncherDefault(PersistMode.AUTO)
                    .restServer(false)
                    .persistenceDir(destinationDir)
                    .start();
            
            Entity restoredApp = Iterables.getOnlyElement(lastMgmt().getEntityManager().getEntities());
            Location restoredLoc = Iterables.getOnlyElement(lastMgmt().getLocationManager().getLocations());
            assertEquals(restoredApp.getId(), appToKeep.get().getId());
            assertEquals(restoredLoc.getId(), locToKeep.get().getId());
        } finally {
            Os.deleteRecursively(destinationDir);
        }
    }
}
