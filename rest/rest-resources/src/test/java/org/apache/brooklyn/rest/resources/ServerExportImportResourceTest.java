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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.FileBasedObjectStore;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
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
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.cxf.jaxrs.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.testng.Assert.*;

@Test(singleThreaded = true,
        // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "ServerExportImportResourceTest")
public class ServerExportImportResourceTest extends BrooklynRestResourceTest {

    private static final Logger log = LoggerFactory.getLogger(ServerExportImportResourceTest.class);

    @Override
    protected boolean useOsgi() { return true; }

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
    public void testExportPersistedStateWithBundlesThenReimportTwice() throws Exception {
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

        for (int i = 0; i < 2; i++) {
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

            Asserts.assertNotNull(manager.getTypeRegistry().get("b1"), "Failed to find type 'b1' after import (iteration " + (i + 1) + ")");
            Asserts.assertNotNull(manager.getTypeRegistry().get("b2"));
            org.apache.brooklyn.api.entity.Entity b2b = Iterables.getOnlyElement(Iterables.getOnlyElement(manager.getApplications()).getChildren());
            Asserts.assertInstanceOf(b2b, TestEntity.class);
            Asserts.assertEquals(b2b.getId(), b2.getId());

            // assert it is persisted (export makes a copy from mgmt so might not be identical, but it should!)
            RebindTestUtils.waitForPersisted(manager);
            File bundlesDir = RebindTestUtils.getLocalPersistenceDir(manager).toPath().resolve("bundles").toFile();
            // the error below _has_ been seen, when we didn't persist after import; unlike some of the other tests (eg looping and not finding type after import) which are just to be safe
            Asserts.assertNotNull(bundlesDir, "No bundles dir after import on iteration "+(i+1));
            Asserts.assertThat(Arrays.asList(bundlesDir.list()),
                    l -> l.stream().anyMatch(f -> f.toLowerCase().endsWith(".jar")),
                    "Bundles dir does not contain JAR on iteration "+(i+1));
        }
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

}
