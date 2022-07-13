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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatformLauncherNoServer;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BrooklynBomYamlCatalogBundleResolver;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcessDriver;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcessImpl;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.rest.domain.HighAvailabilitySummary;
import org.apache.brooklyn.rest.domain.VersionSummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Suppliers;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.cxf.jaxrs.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

@Test(singleThreaded = true,
        // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "ServerResourceTest")
public class ServerResourceTest extends BrooklynRestResourceTest {

    private static final Logger log = LoggerFactory.getLogger(ServerResourceTest.class);

    @Override
    protected boolean useOsgi() { return true; }

    @Test
    public void testGetVersion() throws Exception {
        VersionSummary version = client().path("/server/version").get(VersionSummary.class);
        assertEquals(version.getVersion(), BrooklynVersion.get());
    }

    @Test
    public void testGetPlaneId() throws Exception {
        String planeId = client().path("/server/planeid").get(String.class);
        assertEquals(planeId, manager.getManagementPlaneIdMaybe().get());
    }

    @Test
    public void testGetStatus() throws Exception {
        ManagementNodeState nodeState = client().path("/server/ha/state").get(ManagementNodeState.class);
        assertEquals(nodeState.name(), "MASTER");
    }

    @Test
    public void testExportPersistedState() throws Exception {
        BasicApplication app = manager.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        Location loc = manager.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        byte[] zip = client().path("/server/ha/persist/export").get(byte[].class);
        
        List<String> entryNames = listEntryNames(zip);
        assertTrue(Iterables.tryFind(entryNames, StringPredicates.containsLiteral(app.getId())).isPresent(), "entries="+entryNames);
        assertTrue(Iterables.tryFind(entryNames, StringPredicates.containsLiteral(loc.getId())).isPresent(), "entries="+entryNames);

        Entities.unmanage(app);
    }

    @Test
    public void testExportPersistedStateWithBundlesThenReimport() throws Exception {
        // export seems to preserve install order probably to minimise conflicts, so use deferred start to get the problematic order
        OsgiBundleInstallationResult r2 = ((ManagementContextInternal) manager).getOsgiManager().get().installDeferredStart(null,
                () -> new ByteArrayInputStream(Strings.lines(
                        "brooklyn.catalog:",
                        "  bundle: b2",
                        "  version: 1",
                        "  id: b2",
                        "  itemType: entity",
                        "  item:",
                        "    type: b1"
                ).getBytes()),
                false).get();

        ((ManagementContextInternal) manager).getOsgiManager().get().install(
                () -> new ByteArrayInputStream(Strings.lines(
                        "brooklyn.catalog:",
                        "  bundle: b1",
                        "  version: 1",
                        "  id: b1",
                        "  itemType: entity",
                        "  item:",
                        "    type: org.apache.brooklyn.core.test.entity.TestEntity"
                ).getBytes()),
                BrooklynBomYamlCatalogBundleResolver.FORMAT, false, null).get();

        r2.getDeferredStart().run();

        String yaml = "services: [ { type: b2 } ]";
        Response response = client().path("/applications")
                .post(Entity.entity(yaml, "application/x-yaml"));
        HttpAsserts.assertHealthyStatusCode(response.getStatus());

        org.apache.brooklyn.api.entity.Entity b2 = Iterables.getOnlyElement(Iterables.getOnlyElement(manager.getApplications()).getChildren());
        Asserts.assertInstanceOf(b2, TestEntity.class);

        byte[] zip = client().path("/server/ha/persist/export").get(byte[].class);

        // restart the server, so it has nothing, then try importing
        destroyClass();

        File mementoDir = Os.newTempDir(getClass());
        manager = RebindTestUtils.managementContextBuilder(mementoDir, getClass().getClassLoader())
                .persistPeriodMillis(Duration.ONE_MINUTE.toMilliseconds())
                .haMode(HighAvailabilityMode.MASTER)
                .forLive(true)
                .enablePersistenceBackups(false)
                .emptyCatalog(true)
//                .properties(false)
                .setOsgiEnablementAndReuse(useOsgi(), true)
                .buildStarted();
        new BrooklynCampPlatformLauncherNoServer()
                .useManagementContext(manager)
                .launch();

        initClass();

        Asserts.assertNull(manager.getTypeRegistry().get("b2"));
        Asserts.assertSize(manager.getApplications(), 0);

        Response importResponse = client().path("/server/ha/persist/import").post(Entity.entity(zip, MediaType.APPLICATION_OCTET_STREAM_TYPE));
        HttpAsserts.assertHealthyStatusCode(importResponse.getStatus());

        Asserts.assertNotNull(manager.getTypeRegistry().get("b1"));
        Asserts.assertNotNull(manager.getTypeRegistry().get("b2"));
        org.apache.brooklyn.api.entity.Entity b2b = Iterables.getOnlyElement(Iterables.getOnlyElement(manager.getApplications()).getChildren());
        Asserts.assertInstanceOf(b2b, TestEntity.class);
        Asserts.assertEquals(b2b.getId(), b2.getId());
    }

    private List<String> listEntryNames(byte[] zip) throws Exception {
        List<String> result = new ArrayList<>();
        ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(zip));
        ZipEntry entry;
        while ((entry = zipInputStream.getNextEntry()) != null) {
            result.add(entry.getName());
        }
        return result;
    }
    
    @Test
    public void testGetHighAvailability() throws Exception {
        // Note by default management context from super is started without HA enabled.
        // Therefore can only assert a minimal amount of stuff.
        HighAvailabilitySummary summary = client().path("/server/ha/states").get(HighAvailabilitySummary.class);
        log.info("HA summary is: "+summary);
        
        String planeId = getManagementContext().getManagementPlaneIdMaybe().get();
        String ownNodeId = getManagementContext().getManagementNodeId();
        assertEquals(summary.getPlaneId(), planeId);
        assertEquals(summary.getOwnId(), ownNodeId);
        assertEquals(summary.getMasterId(), ownNodeId);
        assertEquals(summary.getNodes().keySet(), ImmutableSet.of(ownNodeId));
        assertEquals(summary.getNodes().get(ownNodeId).getNodeId(), ownNodeId);
        assertEquals(summary.getNodes().get(ownNodeId).getStatus(), "MASTER");
        assertNotNull(summary.getNodes().get(ownNodeId).getLocalTimestamp());
        // remote will also be non-null if there is no remote backend (local is re-used)
        assertNotNull(summary.getNodes().get(ownNodeId).getRemoteTimestamp());
        assertEquals(summary.getNodes().get(ownNodeId).getLocalTimestamp(), summary.getNodes().get(ownNodeId).getRemoteTimestamp());
    }

    @SuppressWarnings("serial")
    @Test
    public void testReloadsBrooklynProperties() throws Exception {
        final AtomicInteger reloadCount = new AtomicInteger();
        getManagementContext().addPropertiesReloadListener(new ManagementContext.PropertiesReloadListener() {
            @Override public void reloaded() {
                reloadCount.incrementAndGet();
            }});
        client().path("/server/properties/reload").post(null);
        assertEquals(reloadCount.get(), 1);
    }

    @Test
    void testGetConfig() throws Exception {
        ((ManagementContextInternal)getManagementContext()).getBrooklynProperties().put("foo.bar.baz", "quux");
        try {
            assertEquals(client().path("/server/config/foo.bar.baz").get(String.class), "\"quux\"");
        } finally {
            ((ManagementContextInternal)getManagementContext()).getBrooklynProperties().remove("foo.bar.baz");
        }
    }

    @Test
    void testGetMissingConfigThrowsException() throws Exception {
        final String key = "foo.bar.baz";
        BrooklynProperties properties = ((ManagementContextInternal)getManagementContext()).getBrooklynProperties();
        Object existingValue = null;
        boolean keyAlreadyPresent = false;
        if (properties.containsKey(key)) {
            existingValue = properties.remove(key);
            keyAlreadyPresent = true;
        }
        try {
            final WebClient webClient = client().path("/server/config/" + key);
            Response response = webClient.get();
            assertEquals(response.getStatus(), 204);
        } finally {
            if (keyAlreadyPresent) {
                properties.put(key, existingValue);
            }
        }
    }

    // Alternatively could reuse a blocking location, see org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest.ReleaseLatchLocation
    @ImplementedBy(StopLatchEntityImpl.class)
    public interface StopLatchEntity extends EmptySoftwareProcess {
        public void unblock();
        public boolean isBlocked();
    }

    public static class StopLatchEntityImpl extends EmptySoftwareProcessImpl implements StopLatchEntity {
        private CountDownLatch lock = new CountDownLatch(1);
        private volatile boolean isBlocked;

        @Override
        public void unblock() {
            lock.countDown();
        }

        @Override
        protected void postStop() {
            super.preStop();
            try {
                isBlocked = true;
                lock.await();
                isBlocked = false;
            } catch (InterruptedException e) {
                throw Exceptions.propagate(e);
            }
        }

        @Override
        public Class<?> getDriverInterface() {
            return EmptySoftwareProcessDriver.class;
        }

        @Override
        public boolean isBlocked() {
            return isBlocked;
        }

    }

}
