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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public abstract class BrooklynLauncherRebindTestFixture extends AbstractBrooklynLauncherRebindTestFixture {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(BrooklynLauncherRebindTestFixture.class);
    
    @Test
    public void testRebindsToExistingApp() throws Exception {
        populatePersistenceDir(persistenceDir, EntitySpec.create(TestApplication.class).displayName("myorig"));
        
        // Rebind to the app we just started last time
        
        newLauncherDefault(PersistMode.REBIND).start();
        
        assertOnlyApp(lastMgmt(), TestApplication.class);
        assertNotNull(Iterables.find(lastMgmt().getApplications(), EntityPredicates.displayNameEqualTo("myorig"), null), "apps="+lastMgmt().getApplications());
    }

    @Test
    public void testRebindCanAddNewApps() throws Exception {
        populatePersistenceDir(persistenceDir, EntitySpec.create(TestApplication.class).displayName("myorig"));
        
        // Rebind to the app we started last time
        newLauncherDefault(PersistMode.REBIND)
                .application(EntitySpec.create(TestApplication.class).displayName("mynew"))
                .start();
        
        // New app was added, and orig app was rebound
        assertEquals(lastMgmt().getApplications().size(), 2, "apps="+lastMgmt().getApplications());
        assertNotNull(Iterables.find(lastMgmt().getApplications(), EntityPredicates.displayNameEqualTo("mynew"), null), "apps="+lastMgmt().getApplications());

        // And subsequently can create new apps
        StartableApplication app3 = lastMgmt().getEntityManager().createEntity(
                EntitySpec.create(TestApplication.class).displayName("mynew2"));
        app3.start(ImmutableList.<Location>of());
    }

    @Test
    public void testAutoRebindsToExistingApp() throws Exception {
        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class);
        populatePersistenceDir(persistenceDir, appSpec);
        
        // Auto will rebind if the dir exists
        newLauncherDefault(PersistMode.AUTO).start();
        
        assertOnlyApp(lastMgmt(), TestApplication.class);
    }

    @Test
    public void testCleanDoesNotRebindToExistingApp() throws Exception {
        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class);
        populatePersistenceDir(persistenceDir, appSpec);
        
        // Auto will rebind if the dir exists
        newLauncherDefault(PersistMode.CLEAN).start();
        
        assertTrue(lastMgmt().getApplications().isEmpty(), "apps="+lastMgmt().getApplications());
    }

    @Test
    public void testAutoRebindCreatesNewIfEmptyDir() throws Exception {
        // Auto will rebind if the dir exists
        newLauncherDefault(PersistMode.AUTO)
                .application(EntitySpec.create(TestApplication.class))
                .start();
        
        assertOnlyApp(lastMgmt(), TestApplication.class);
        assertMementoContainerNonEmptyForTypeEventually("entities");
    }

    @Test
    public void testRebindRespectsPersistenceDirSetInProperties() throws Exception {
        String persistenceDir2 = newTempPersistenceContainerName();
        
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newDefault();
        brooklynProperties.put(BrooklynServerConfig.PERSISTENCE_DIR, persistenceDir2);
        LocalManagementContextForTests mgmt = newManagementContextForTests(brooklynProperties);
        
        // Rebind to the app we started last time
        newLauncherBase()
                .persistMode(PersistMode.AUTO)
                .persistPeriod(Duration.millis(10))
                .managementContext(mgmt)
                .start();
        
        checkPersistenceContainerNameIs(persistenceDir2);
    }

    // assumes default persistence dir is rebindable
    @Test(groups="Integration")
    public void testRebindRespectsDefaultPersistenceDir() throws Exception {
        newLauncherDefault(PersistMode.AUTO)
                .persistenceDir((String)null)
                .start();
        
        checkPersistenceContainerNameIsDefault();
    }
    
    protected abstract void checkPersistenceContainerNameIsDefault();
    protected abstract void checkPersistenceContainerNameIs(String expected);

    @Test
    public void testPersistenceFailsIfNoDir() throws Exception {
        runRebindFails(PersistMode.REBIND, badContainerName(), "does not exist");
    }

    protected abstract String badContainerName();

    @Test
    public void testExplicitRebindFailsIfEmpty() throws Exception {
        runRebindFails(PersistMode.REBIND, persistenceDir, "directory is empty");
    }
}
