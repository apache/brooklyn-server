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

import java.util.List;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.mgmt.persist.PersistenceObjectStore;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.FatalConfigurationRuntimeException;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public abstract class AbstractBrooklynLauncherRebindTestFixture {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(AbstractBrooklynLauncherRebindTestFixture.class);
    
    protected String persistenceDir;
    protected String persistenceLocationSpec;
    protected List<BrooklynLauncher> launchers = MutableList.of();
    
    protected abstract String newTempPersistenceContainerName();

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        persistenceDir = newTempPersistenceContainerName();
    }
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        for (BrooklynLauncher l: launchers) {
            if (l.isStarted()) {
                l.terminate();
                PersistenceObjectStore store = getPersistenceStore(l.getServerDetails().getManagementContext());
                if (store!=null) store.deleteCompletely();
            }
        }
    }

    protected BrooklynLauncher newLauncherBase() {
        BrooklynLauncher l = BrooklynLauncher.newInstance()
            .webconsole(false);
        launchers.add(l);
        return l;
    }
    
    protected BrooklynLauncher newLauncherDefault(PersistMode mode) {
        return newLauncherBase()
                .managementContext(newManagementContextForTests(null))
                .persistMode(mode)
                .persistenceDir(persistenceDir)
                .persistPeriod(Duration.millis(10));
    }
    
    protected LocalManagementContextForTests newManagementContextForTests(BrooklynProperties props) {
        if (props==null)
            return new LocalManagementContextForTests();
        else
            return new LocalManagementContextForTests(props);
    }

    protected ManagementContext lastMgmt() {
        return Iterables.getLast(launchers).getServerDetails().getManagementContext();
    }
    
    protected void runRebindFails(PersistMode persistMode, String dir, String errmsg) throws Exception {
        try {
            newLauncherDefault(persistMode)
                    .persistenceDir(dir)
                    .start();
        } catch (FatalConfigurationRuntimeException e) {
            if (!e.toString().contains(errmsg)) {
                throw e;
            }
        }
    }

    protected void populatePersistenceDir(String dir, EntitySpec<? extends StartableApplication> appSpec) throws Exception {
        BrooklynLauncher launcher = newLauncherDefault(PersistMode.CLEAN)
                .highAvailabilityMode(HighAvailabilityMode.MASTER)
                .persistenceDir(dir)
                .application(appSpec)
                .start();
        launcher.terminate();
        assertMementoContainerNonEmptyForTypeEventually("entities");
    }
    
    protected void populatePersistenceDir(String dir, Function<ManagementContext, Void> populator) throws Exception {
        BrooklynLauncher launcher = newLauncherDefault(PersistMode.CLEAN)
                .highAvailabilityMode(HighAvailabilityMode.MASTER)
                .persistenceDir(dir)
                .start();
        populator.apply(launcher.getManagementContext());
        launcher.terminate();
        assertMementoContainerNonEmptyForTypeEventually("entities");
    }
    
    protected void assertOnlyApp(ManagementContext managementContext, Class<? extends Application> expectedType) {
        assertEquals(managementContext.getApplications().size(), 1, "apps="+managementContext.getApplications());
        assertNotNull(Iterables.find(managementContext.getApplications(), Predicates.instanceOf(TestApplication.class), null), "apps="+managementContext.getApplications());
    }
    
    protected void assertMementoContainerNonEmptyForTypeEventually(final String type) {
        Asserts.succeedsEventually(ImmutableMap.of("timeout", Duration.TEN_SECONDS), new Runnable() {
            @Override public void run() {
                getPersistenceStore(lastMgmt()).listContentsWithSubPath(type);
            }});
    }

    static PersistenceObjectStore getPersistenceStore(ManagementContext managementContext) {
        if (managementContext==null) return null;
        BrooklynMementoPersisterToObjectStore persister = (BrooklynMementoPersisterToObjectStore)managementContext.getRebindManager().getPersister();
        if (persister==null) return null;
        return persister.getObjectStore();
    }

}
