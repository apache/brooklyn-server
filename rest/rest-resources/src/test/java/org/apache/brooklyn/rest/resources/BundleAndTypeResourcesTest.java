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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.awt.Image;
import java.awt.Toolkit;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.api.objs.Identifiable;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.enricher.stock.Aggregator;
import org.apache.brooklyn.policy.autoscaling.AutoScalerPolicy;
import org.apache.brooklyn.rest.domain.BundleInstallationRestResult;
import org.apache.brooklyn.rest.domain.BundleSummary;
import org.apache.brooklyn.rest.domain.TypeDetail;
import org.apache.brooklyn.rest.domain.TypeSummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.http.HttpHeaders;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.reporters.Files;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class BundleAndTypeResourcesTest extends BrooklynRestResourceTest {

    private static final Logger log = LoggerFactory.getLogger(BundleAndTypeResourcesTest.class);
    
    private static String TEST_VERSION = "0.1.2";
    private static String TEST_LASTEST_VERSION = "0.1.3";

    private Collection<ManagedBundle> initialBundles;

    @Override
    protected boolean useLocalScannedCatalog() {
        return true;
    }

    @Override
    protected void initClass() throws Exception {
        super.initClass();
        // cache initially installed bundles
        OsgiManager osgi = ((ManagementContextInternal)getManagementContext()).getOsgiManager().get();
        initialBundles = osgi.getManagedBundles().values();
    }
    
    protected void initMethod() throws Exception {
        super.initMethod();
        
        // and reset OSGi container
        OsgiManager osgi = ((ManagementContextInternal)getManagementContext()).getOsgiManager().get();
        for (ManagedBundle b: osgi.getManagedBundles().values()) {
            if (!initialBundles.contains(b)) {
                osgi.uninstallUploadedBundle(b);
            }
        }
    }
    
    @Test
    /** based on CampYamlLiteTest */
    public void testRegisterCustomEntityTopLevelSyntaxWithBundleWhereEntityIsFromCoreAndIconFromBundle() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = "my.catalog.entity.id";
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  libraries:",
                "  - url: " + bundleUrl,
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity");

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/yaml")
                .post(yaml);

        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());
        BundleInstallationRestResult installed = response.readEntity(BundleInstallationRestResult.class);
        Asserts.assertSize(installed.getTypes().values(), 1);
        TypeSummary installedItem = installed.getTypes().get(symbolicName+":"+TEST_VERSION);
        Assert.assertNotNull(installedItem, ""+installed.getTypes());

        TypeDetail entityItem = client().path("/catalog/types/"+symbolicName + "/" + TEST_VERSION)
                .get(TypeDetail.class);

        Assert.assertEquals(new TypeSummary(entityItem), installedItem);
        Assert.assertNotNull(entityItem.getPlan());
        Assert.assertTrue(((String)entityItem.getPlan().getData()).contains("org.apache.brooklyn.core.test.entity.TestEntity"));

        assertEquals(entityItem.getSymbolicName(), symbolicName);
        assertEquals(entityItem.getVersion(), TEST_VERSION);

        // also check it's included in various lists
        List<TypeSummary> list1 = client().path("/catalog/types/"+symbolicName).get(new GenericType<List<TypeSummary>>() {});
        assertEquals(list1, MutableList.of(installedItem));
        List<TypeSummary> list2 = client().path("/catalog/types").get(new GenericType<List<TypeSummary>>() {});
        Assert.assertTrue(list2.contains(installedItem), ""+list2);
        List<TypeSummary> list3 = client().path("/catalog/types").query("supertype", "entity").get(new GenericType<List<TypeSummary>>() {});
        Assert.assertTrue(list3.contains(installedItem), ""+list3);
        
        // and internally let's check we have libraries
        RegisteredType item = getManagementContext().getTypeRegistry().get(symbolicName, TEST_VERSION);
        Assert.assertNotNull(item);
        Collection<OsgiBundleWithUrl> libs = item.getLibraries();
        assertEquals(libs.size(), 1);
        assertEquals(Iterables.getOnlyElement(libs).getUrl(), bundleUrl);

        // now let's check other things on the item
        URI expectedIconUrl = URI.create(getEndpointAddress() + "/catalog/types/" + symbolicName + "/" + entityItem.getVersion()+"/icon").normalize();
        assertEquals(entityItem.getDisplayName(), "My Catalog App");
        assertEquals(entityItem.getDescription(), "My description");
        assertEquals(entityItem.getIconUrl(), expectedIconUrl.getPath());
        assertEquals(item.getIconUrl(), "classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif");

        // an InterfacesTag should be created for every catalog item
        if (checkTraits(false)) {
            @SuppressWarnings("unchecked")
            Map<String, List<String>> traitsMapTag = Iterables.getOnlyElement(Iterables.filter(entityItem.getTags(), Map.class));
            List<String> actualInterfaces = traitsMapTag.get("traits");
            List<Class<?>> expectedInterfaces = Reflections.getAllInterfaces(TestEntity.class);
            assertEquals(actualInterfaces.size(), expectedInterfaces.size());
            for (Class<?> expectedInterface : expectedInterfaces) {
                assertTrue(actualInterfaces.contains(expectedInterface.getName()));
            }
        }

        byte[] iconData = client().path("/catalog/types/" + symbolicName + "/" + TEST_VERSION+"/icon").get(byte[].class);
        assertEquals(iconData.length, 43);
    }

    @Test
    public void testRegisterOsgiPolicyTopLevelSyntax() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = "my.catalog.entity.id."+JavaClassNames.niceClassAndMethod();
        String policyType = "org.apache.brooklyn.test.osgi.entities.SimplePolicy";
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;

        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: policy",
                "  name: My Catalog App",
                "  description: My description",
                "  libraries:",
                "  - url: " + bundleUrl,
                "  item:",
                "    type: " + policyType);

        TypeSummary installedItem = Iterables.getOnlyElement( client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/yaml")
                .post(yaml, BundleInstallationRestResult.class).getTypes().values() );

        assertEquals(installedItem.getSymbolicName(), symbolicName);
        assertEquals(installedItem.getVersion(), TEST_VERSION);
        Assert.assertTrue(installedItem.getSupertypes().contains(Policy.class.getName()), ""+installedItem.getSupertypes());
    }

    @Test
    public void testFilterListOfEntitiesByName() {
        List<TypeSummary> entities = client().path("/catalog/types")
                .query("fragment", "vaNIllasOFTWAREpROCESS").get(new GenericType<List<TypeSummary>>() {});
        log.info("Matching entities: " + entities);
        assertEquals(entities.size(), 1);

        entities = client().path("/catalog/types").query("supertype", "entity")
                .query("fragment", "vaNIllasOFTWAREpROCESS").get(new GenericType<List<TypeSummary>>() {});
        log.info("Matching entities: " + entities);
        assertEquals(entities.size(), 1);

        List<TypeSummary> entities2 = client().path("/catalog/types")
                .query("regex", "[Vv]an.[alS]+oftware\\w+").get(new GenericType<List<TypeSummary>>() {});
        assertEquals(entities2.size(), 1);

        assertEquals(entities, entities2);
    
        entities = client().path("/catalog/types").query("supertype", "entity")
                .query("fragment", "bweqQzZ").get(new GenericType<List<TypeSummary>>() {});
        Asserts.assertSize(entities, 0);

        entities = client().path("/catalog/types").query("supertype", "entity")
                .query("regex", "bweq+z+").get(new GenericType<List<TypeSummary>>() {});
        Asserts.assertSize(entities, 0);
    }

    @Test
    public void testGetCatalogEntityIconDetails() throws IOException {
        String catalogItemId = "testGetCatalogEntityIconDetails";
        addTestCatalogItemAsEntity(catalogItemId);
        Response response = client().path(URI.create("/catalog/types/" + catalogItemId + "/" + TEST_VERSION + "/icon"))
                .get();
        response.bufferEntity();
        Assert.assertEquals(response.getStatus(), 200);
        Assert.assertEquals(response.getMediaType(), MediaType.valueOf("image/png"));
        Image image = Toolkit.getDefaultToolkit().createImage(Files.readFile(response.readEntity(InputStream.class)));
        Assert.assertNotNull(image);
    }

    private void addTestCatalogItemAsEntity(String catalogItemId) {
        addTestCatalogItem(catalogItemId, "entity", TEST_VERSION, "org.apache.brooklyn.rest.resources.DummyIconEntity");
    }

    private void addTestCatalogItem(String catalogItemId, String itemType, String version, String service) {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + catalogItemId,
                "  itemType: " + checkNotNull(itemType),
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:///bridge-small.png",
                "  version: " + version,
                "  item:",
                "    type: " + service);

        client().path("/catalog/bundles").header(HttpHeaders.CONTENT_TYPE, "application/yaml").post(yaml);
    }

    @Test
    public void testListPolicies() {
        Set<TypeSummary> policies = client().path("/catalog/types").query("supertype", "policy")
                .get(new GenericType<Set<TypeSummary>>() {});

        assertTrue(policies.size() > 0);
        TypeSummary asp = null;
        for (TypeSummary p : policies) {
            if (AutoScalerPolicy.class.getName().equals(p.getSymbolicName()))
                asp = p;
        }
        Assert.assertNotNull(asp, "didn't find AutoScalerPolicy");
    }

    @Test
    public void testLocationAddGetAndRemove() {
        String symbolicName = "my.catalog.location.id";
        String locationType = "localhost";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: location",
                "  name: My Catalog Location",
                "  description: My description",
                "  item:",
                "    type: " + locationType);

        // Create location item
        Map<String, TypeSummary> items = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/yaml")
                .post(yaml, BundleInstallationRestResult.class).getTypes();
        TypeSummary locationItem = Iterables.getOnlyElement(items.values());

        assertEquals(locationItem.getSymbolicName(), symbolicName);
        assertEquals(locationItem.getVersion(), TEST_VERSION);

        // Retrieve location item
        TypeDetail location = client().path("/catalog/types/"+symbolicName+"/"+TEST_VERSION).get(TypeDetail.class);
        assertEquals(location.getSymbolicName(), symbolicName);

        // Retrieve all locations
        Set<TypeSummary> locations = client().path("/catalog/types").query("supertype", "location")
                .get(new GenericType<Set<TypeSummary>>() {});
        boolean found = false;
        for (TypeSummary contender : locations) {
            if (contender.getSymbolicName().equals(symbolicName)) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found, "contenders="+locations);
        
        // Delete
        Response deleteResponse = client().path("/catalog/bundles/"+locationItem.getContainingBundle().replaceAll(":", "/"))
                .delete();
        assertEquals(deleteResponse.getStatus(), Response.Status.OK.getStatusCode());
        BundleInstallationRestResult deletionResponse = deleteResponse.readEntity(BundleInstallationRestResult.class);
        Assert.assertEquals(deletionResponse.getBundle(), symbolicName+":"+TEST_VERSION);
        Assert.assertEquals(deletionResponse.getTypes().keySet(), MutableSet.of(symbolicName+":"+TEST_VERSION));

        Response getPostDeleteResponse = client().path("/catalog/types/"+symbolicName+"/"+TEST_VERSION)
                .get();
        assertEquals(getPostDeleteResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void testListEnrichers() {
        Set<TypeSummary> enrichers = client().path("/catalog/types").query("supertype", "enricher")
                .get(new GenericType<Set<TypeSummary>>() {});

        assertTrue(enrichers.size() > 0);
        TypeSummary asp = null;
        for (TypeSummary p : enrichers) {
            if (Aggregator.class.getName().equals(p.getSymbolicName()))
                asp = p;
        }
        Assert.assertNotNull(asp, "didn't find Aggregator");
    }

    @Test
    public void testEnricherAddGet() {
        String symbolicName = "my.catalog.enricher.id";
        String enricherType = "org.apache.brooklyn.enricher.stock.Aggregator";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: enricher",
                "  name: My Catalog Enricher",
                "  description: My description",
                "  item:",
                "    type: " + enricherType);

        // Create location item
        Map<String, TypeSummary> items = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/yaml")
                .post(yaml, BundleInstallationRestResult.class).getTypes();
        TypeSummary enricherItem = Iterables.getOnlyElement(items.values());

        assertEquals(enricherItem.getSymbolicName(), symbolicName);
        assertEquals(enricherItem.getVersion(), TEST_VERSION);

        // Retrieve location item
        TypeSummary enricher = client().path("/catalog/types/"+symbolicName+"/"+TEST_VERSION)
                .get(TypeSummary.class);
        assertEquals(enricher.getSymbolicName(), symbolicName);

        // Retrieve all locations
        Set<TypeSummary> enrichers = client().path("/catalog/types").query("supertype", "enricher")
                .get(new GenericType<Set<TypeSummary>>() {});
        boolean found = false;
        for (TypeSummary contender : enrichers) {
            if (contender.getSymbolicName().equals(symbolicName)) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found, "contenders="+enrichers);
    }

    @Test
    // osgi may fail in IDE, typically works on mvn CLI though
    public void testRegisterOsgiEnricherTopLevelSyntax() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = "my.catalog.enricher.id";
        String enricherType = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENRICHER;
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;

        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: enricher",
                "  name: My Catalog Enricher",
                "  description: My description",
                "  libraries:",
                "  - url: " + bundleUrl,
                "  item:",
                "    type: " + enricherType);

        TypeSummary installedItem = Iterables.getOnlyElement( client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/yaml")
                .post(yaml, BundleInstallationRestResult.class).getTypes().values() );

        assertEquals(installedItem.getSymbolicName(), symbolicName);
        assertEquals(installedItem.getVersion(), TEST_VERSION);
    }

    @Test
    public void testDeleteCustomEntityFromCatalog() {
        String symbolicName = "my.catalog.app.id.to.subsequently.delete";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  name: My Catalog App To Be Deleted",
                "  description: My description",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity");

        client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/yaml")
                .post(yaml);

        BundleSummary getInstalledBundle = client().path("/catalog/bundles/"+symbolicName+"/"+TEST_VERSION)
            .get(BundleSummary.class);
        assertEquals(getInstalledBundle.getSymbolicName(), symbolicName);
        assertEquals(getInstalledBundle.getVersion(), TEST_VERSION);
        Asserts.assertNotNull(getInstalledBundle.getTypes(), "expected 'types' in: "+getInstalledBundle.getExtraFields());
        Asserts.assertStringContains(""+getInstalledBundle.getTypes(), "My Catalog App");

        Response deleteResponse = client().path("/catalog/bundles/"+symbolicName+"/"+TEST_VERSION)
                .delete();

        assertEquals(deleteResponse.getStatus(), Response.Status.OK.getStatusCode());
        // contents of delete tested in delete location method

        Response getPostDeleteResponse = client().path("/catalog/bundles/"+symbolicName+"/"+TEST_VERSION)
                .get();
        assertEquals(getPostDeleteResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    private void addCatalogItemWithInvalidBundleUrl(String bundleUrl) {
        String symbolicName = "my.catalog.entity.id";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  libraries:",
                "  - url: " + bundleUrl,
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity");

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-yaml")
                .post(yaml);

        assertEquals(response.getStatus(), HttpStatus.BAD_REQUEST_400);
    }
    
    @Test
    public void testAddUnreachableItem() {
        addCatalogItemWithInvalidBundleUrl("http://0.0.0.0/can-not-connect");
    }

    @Test
    public void testAddInvalidItem() {
        //equivalent to HTTP response 200 text/html
        addCatalogItemWithInvalidBundleUrl("classpath://not-a-jar-file.txt");
    }

    @Test
    public void testAddMissingItem() {
        //equivalent to HTTP response 404 text/html
        addCatalogItemWithInvalidBundleUrl("classpath://missing-jar-file.txt");
    }

    @Test
    public void testInvalidArchive() throws Exception {
        File f = Os.newTempFile("osgi", "zip");

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        Asserts.assertStringContainsIgnoreCase(response.readEntity(String.class), "zip file is empty");
    }

    @Test
    public void testArchiveWithoutBom() throws Exception {
        File f = createZip(ImmutableMap.<String, String>of());

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        Asserts.assertStringContainsIgnoreCase(response.readEntity(String.class), "Missing bundle symbolic name in BOM or MANIFEST");
    }

    @Test
    public void testArchiveWithoutBundleAndVersion() throws Exception {
        File f = createZip(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity")));

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        Asserts.assertStringContainsIgnoreCase(response.readEntity(String.class), "Missing bundle symbolic name in BOM or MANIFEST");
    }

    @Test
    public void testArchiveWithoutBundle() throws Exception {
        File f = createZip(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.0",
                "  itemType: entity",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity")));

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        Asserts.assertStringContainsIgnoreCase(response.readEntity(String.class), 
            "Missing bundle symbolic name in BOM or MANIFEST");
    }

    @Test
    public void testArchiveWithoutVersion() throws Exception {
        File f = createZip(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: org.apache.brooklyn.test",
                "  itemType: entity",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity")));

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        Asserts.assertStringContainsIgnoreCase(response.readEntity(String.class), "Catalog BOM must define version");
    }

    @Test
    public void testJarWithoutMatchingBundle() throws Exception {
        String name = "My Catalog App";
        String bundle = "org.apache.brooklyn.test";
        String version = "0.1.0";
        String wrongBundleName = "org.apache.brooklyn.test2";
        File f = createJar(ImmutableMap.<String, String>of(
                "catalog.bom", Joiner.on("\n").join(
                        "brooklyn.catalog:",
                        "  bundle: " + bundle,
                        "  version: " + version,
                        "  itemType: entity",
                        "  name: " + name,
                        "  description: My description",
                        "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                        "  item:",
                        "    type: org.apache.brooklyn.core.test.entity.TestEntity"),
                "META-INF/MANIFEST.MF", Joiner.on("\n").join(
                        "Manifest-Version: 1.0",
                        "Bundle-Name: " + name,
                        "Bundle-SymbolicName: "+wrongBundleName,
                        "Bundle-Version: " + version,
                        "Bundle-ManifestVersion: " + version)));

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        Asserts.assertStringContainsIgnoreCase(response.readEntity(String.class), 
            "symbolic name mismatch",
            wrongBundleName, bundle);
    }

    @Test
    public void testJarWithoutMatchingVersion() throws Exception {
        String name = "My Catalog App";
        String bundle = "org.apache.brooklyn.test";
        String version = "0.1.0";
        String wrongVersion = "0.3.0";
        File f = createJar(ImmutableMap.<String, String>of(
                "catalog.bom", Joiner.on("\n").join(
                        "brooklyn.catalog:",
                        "  bundle: " + bundle,
                        "  version: " + version,
                        "  itemType: entity",
                        "  name: " + name,
                        "  description: My description",
                        "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                        "  item:",
                        "    type: org.apache.brooklyn.core.test.entity.TestEntity"),
                "META-INF/MANIFEST.MF", Joiner.on("\n").join(
                        "Manifest-Version: 1.0",
                        "Bundle-Name: " + name,
                        "Bundle-SymbolicName: " + bundle,
                        "Bundle-Version: " + wrongVersion,
                        "Bundle-ManifestVersion: " + wrongVersion)));

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        Asserts.assertStringContainsIgnoreCase(response.readEntity(String.class), 
            "version mismatch",
            wrongVersion, version);
    }

    @Test
    public void testOsgiBundleWithBom() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        final String symbolicName = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_SYMBOLIC_NAME_FULL;
        final String version = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_VERSION;
        final String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        BundleMaker bm = new BundleMaker(manager);
        File f = Os.newTempFile("osgi", "jar");
        Files.copyFile(ResourceUtils.create(this).getResourceFromUrl(bundleUrl), f);
        
        String bom = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + version,
                "  id: " + symbolicName,
                "  itemType: entity",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity");
        
        f = bm.copyAdding(f, MutableMap.of(new ZipEntry("catalog.bom"), (InputStream) new ByteArrayInputStream(bom.getBytes())));

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(f)));
        
        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        TypeDetail entityItem = client().path("/catalog/types/"+symbolicName + "/" + version)
                .get(TypeDetail.class);
        assertEquals(entityItem.getSymbolicName(), symbolicName);
        assertEquals(entityItem.getVersion(), version);

        // assert we can cast it as summary
        TypeSummary entityItemSummary = client().path("/catalog/types/"+symbolicName + "/" + version)
            .get(TypeSummary.class);
        assertEquals(entityItemSummary.getSymbolicName(), symbolicName);
        assertEquals(entityItemSummary.getVersion(), version);

        List<TypeSummary> typesInBundle = client().path("/catalog/bundles/" + symbolicName + "/" + version + "/types")
            .get(new GenericType<List<TypeSummary>>() {});
        assertEquals(Iterables.getOnlyElement(typesInBundle), entityItemSummary);

        TypeDetail entityItemFromBundle = client().path("/catalog/bundles/" + symbolicName + "/" + version + "/types/" + symbolicName + "/" + version)
            .get(TypeDetail.class);
        assertEquals(entityItemFromBundle, entityItem);
        
        // and internally let's check we have libraries
        RegisteredType item = getManagementContext().getTypeRegistry().get(symbolicName, version);
        Assert.assertNotNull(item);
        Collection<OsgiBundleWithUrl> libs = item.getLibraries();
        assertEquals(libs.size(), 1);
        OsgiBundleWithUrl lib = Iterables.getOnlyElement(libs);
        Assert.assertNull(lib.getUrl());

        assertEquals(lib.getSymbolicName(), "org.apache.brooklyn.test.resources.osgi.brooklyn-test-osgi-entities");
        assertEquals(lib.getSuppliedVersionString(), version);

        // now let's check other things on the item
        URI expectedIconUrl = URI.create(getEndpointAddress() + "/catalog/types/" + symbolicName + "/" + entityItem.getVersion()+"/icon").normalize();
        assertEquals(entityItem.getDisplayName(), "My Catalog App");
        assertEquals(entityItem.getDescription(), "My description");
        assertEquals(entityItem.getIconUrl(), expectedIconUrl.getPath());
        assertEquals(item.getIconUrl(), "classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif");

        if (checkTraits(false)) {
            // an InterfacesTag should be created for every catalog item
            @SuppressWarnings("unchecked")
            Map<String, List<String>> traitsMapTag = Iterables.getOnlyElement(Iterables.filter(entityItem.getTags(), Map.class));
            List<String> actualInterfaces = traitsMapTag.get("traits");
            List<Class<?>> expectedInterfaces = Reflections.getAllInterfaces(TestEntity.class);
            assertEquals(actualInterfaces.size(), expectedInterfaces.size());
            for (Class<?> expectedInterface : expectedInterfaces) {
                assertTrue(actualInterfaces.contains(expectedInterface.getName()));
            }
        }

        byte[] iconData = client().path("/catalog/types/" + symbolicName + "/" + version + "/icon").get(byte[].class);
        assertEquals(iconData.length, 43);
    }

    @Test
    public void testOsgiBundleWithBomNotInBrooklynNamespace() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH);
        final String symbolicName = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FULL;
        final String version = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_VERSION;
        final String bundleUrl = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_URL;
        final String entityType = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_ENTITY;
        final String iconPath = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_ICON_PATH;
        BundleMaker bm = new BundleMaker(manager);
        File f = Os.newTempFile("osgi", "jar");
        Files.copyFile(ResourceUtils.create(this).getResourceFromUrl(bundleUrl), f);

        String bom = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + version,
                "  id: " + symbolicName,
                "  itemType: entity",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:" + iconPath,
                "  item:",
                "    type: " + entityType);

        f = bm.copyAdding(f, MutableMap.of(new ZipEntry("catalog.bom"), (InputStream) new ByteArrayInputStream(bom.getBytes())));

        Response response = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));


        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        TypeDetail entityItem = client().path("/catalog/types/"+symbolicName + "/" + version)
                .get(TypeDetail.class);

        Assert.assertNotNull(entityItem.getPlan().getData());
        Assert.assertTrue(entityItem.getPlan().getData().toString().contains(entityType));

        assertEquals(entityItem.getSymbolicName(), symbolicName);
        assertEquals(entityItem.getVersion(), version);

        // and internally let's check we have libraries
        RegisteredType item = getManagementContext().getTypeRegistry().get(symbolicName, version);
        Assert.assertNotNull(item);
        Collection<OsgiBundleWithUrl> libs = item.getLibraries();
        assertEquals(libs.size(), 1);
        OsgiBundleWithUrl lib = Iterables.getOnlyElement(libs);
        Assert.assertNull(lib.getUrl());

        assertEquals(lib.getSymbolicName(), symbolicName);
        assertEquals(lib.getSuppliedVersionString(), version);

        // now let's check other things on the item
        assertEquals(entityItem.getDescription(), "My description");
        URI expectedIconUrl = URI.create(getEndpointAddress() + "/catalog/types/" + symbolicName + "/" + entityItem.getVersion() + "/icon").normalize();
        assertEquals(entityItem.getIconUrl(), expectedIconUrl.getPath());
        assertEquals(item.getIconUrl(), "classpath:" + iconPath);

        if (checkTraits(false)) {
            // an InterfacesTag should be created for every catalog item
            @SuppressWarnings("unchecked")
            Map<String, List<String>> traitsMapTag = Iterables.getOnlyElement(Iterables.filter(entityItem.getTags(), Map.class));
            List<String> actualInterfaces = traitsMapTag.get("traits");
            List<String> expectedInterfaces = ImmutableList.of(Entity.class.getName(), BrooklynObject.class.getName(), Identifiable.class.getName(), Configurable.class.getName());
            assertTrue(actualInterfaces.containsAll(expectedInterfaces), "actual="+actualInterfaces);
        }
    
        byte[] iconData = client().path("/catalog/types/" + symbolicName + "/" + version + "/icon").get(byte[].class);
        assertEquals(iconData.length, 43);

        // Check that the catalog item is useable (i.e. can deploy the entity)
        String appYaml = Joiner.on("\n").join(
                "services:",
                "- type: " + symbolicName + ":" + version,
                "  name: myEntityName");

        Response appResponse = client().path("/applications")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-yaml")
                .post(appYaml);

        assertEquals(appResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        Entity entity = Iterables.tryFind(getManagementContext().getEntityManager().getEntities(), EntityPredicates.displayNameEqualTo("myEntityName")).get();
        assertEquals(entity.getEntityType().getName(), entityType);
    }

    private static File createZip(Map<String, String> files) throws Exception {
        File f = Os.newTempFile("osgi", "zip");

        ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(f));

        for (Map.Entry<String, String> entry : files.entrySet()) {
            ZipEntry ze = new ZipEntry(entry.getKey());
            zip.putNextEntry(ze);
            zip.write(entry.getValue().getBytes());
        }

        zip.closeEntry();
        zip.flush();
        zip.close();

        return f;
    }

    private static File createJar(Map<String, String> files) throws Exception {
        File f = Os.newTempFile("osgi", "jar");

        JarOutputStream zip = new JarOutputStream(new FileOutputStream(f));

        for (Map.Entry<String, String> entry : files.entrySet()) {
            JarEntry ze = new JarEntry(entry.getKey());
            zip.putNextEntry(ze);
            zip.write(entry.getValue().getBytes());
        }

        zip.closeEntry();
        zip.flush();
        zip.close();

        return f;
    }

    @Test
    public void testGetOnlyLatestApplication() {
        String symbolicName = "latest.catalog.application.id";
        String itemType = "template";
        String serviceType = "org.apache.brooklyn.core.test.entity.TestEntity";

        addTestCatalogItem(symbolicName, itemType, TEST_VERSION, serviceType);
        addTestCatalogItem(symbolicName, itemType, TEST_LASTEST_VERSION, serviceType);

        TypeSummary application = client().path("/catalog/types/" + symbolicName + "/latest")
                .get(TypeSummary.class);
        assertEquals(application.getVersion(), TEST_LASTEST_VERSION);
    }

    @Test
    public void testGetOnlyLatestDifferentCases() {
        // depends on installation of this
        testGetOnlyLatestApplication();
        
        String symbolicName = "latest.catalog.application.id";

        TypeSummary application = client().path("/catalog/types/" + symbolicName + "/LaTeSt")
                .get(TypeSummary.class);
        assertEquals(application.getVersion(), TEST_LASTEST_VERSION);

        application = client().path("/catalog/types/" + symbolicName + "/LATEST")
                .get(TypeSummary.class);
        assertEquals(application.getVersion(), TEST_LASTEST_VERSION);
    }

    @Test
    public void testGetOnlyLatestEntity() {
        String symbolicName = "latest.catalog.entity.id";
        String itemType = "entity";
        String serviceType = "org.apache.brooklyn.core.test.entity.TestEntity";

        addTestCatalogItem(symbolicName, itemType, TEST_VERSION, serviceType);
        addTestCatalogItem(symbolicName, itemType, TEST_LASTEST_VERSION, serviceType);

        TypeSummary application = client().path("/catalog/types/" + symbolicName + "/latest")
                .get(TypeSummary.class);
        assertEquals(application.getVersion(), TEST_LASTEST_VERSION);
    }

    @Test
    public void testGetOnlyLatestLocation() {
        String symbolicName = "latest.catalog.location.id";
        String itemType = "location";
        String serviceType = "localhost";

        addTestCatalogItem(symbolicName, itemType, TEST_VERSION, serviceType);
        addTestCatalogItem(symbolicName, itemType, TEST_LASTEST_VERSION, serviceType);

        TypeSummary application = client().path("/catalog/types/" + symbolicName + "/latest")
                .get(TypeSummary.class);
        assertEquals(application.getVersion(), TEST_LASTEST_VERSION);
    }


    @Test
    public void testForceUpdateForYAML() {
        String symbolicName = "force.update.catalog.application.id";
        String itemType = "template";
        String initialName = "My Catalog App";
        String initialDescription = "My description";
        String updatedName = initialName + " 2";
        String updatedDescription = initialDescription + " 2";

        String initialYaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: " + itemType,
                "  name: " + initialName,
                "  description: " + initialDescription,
                "  icon_url: classpath:///bridge-small.png",
                "  version: " + TEST_VERSION,
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity");
        String updatedYaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: " + itemType,
                "  name: " + updatedName,
                "  description: " + updatedDescription,
                "  icon_url: classpath:///bridge-small.png",
                "  version: " + TEST_VERSION,
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity");

        client().path("/catalog/bundles").header(HttpHeaders.CONTENT_TYPE, "application/yaml").post(initialYaml);

        TypeDetail initialApplication = client().path("/catalog/types/" + symbolicName + "/" + TEST_VERSION)
                .get(TypeDetail.class);
        assertEquals(initialApplication.getDisplayName(), initialName);
        assertEquals(initialApplication.getDescription(), initialDescription);

        Response invalidResponse = client().path("/catalog/bundles").header(HttpHeaders.CONTENT_TYPE, "application/yaml").post(updatedYaml);

        assertEquals(invalidResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        Response validResponse = client().path("/catalog/bundles").query("force", true).header(HttpHeaders.CONTENT_TYPE, "application/yaml").post(updatedYaml);

        assertEquals(validResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        TypeSummary application = client().path("/catalog/types/" + symbolicName + "/" + TEST_VERSION)
                .get(TypeSummary.class);
        assertEquals(application.getDisplayName(), updatedName);
        assertEquals(application.getDescription(), updatedDescription);
    }

    @Test
    public void testForceUpdateForZip() throws Exception {
        final String symbolicName = "force.update.zip.catalog.application.id";
        final String initialName = "My Catalog App";
        final String initialDescription = "My Description";
        final String updatedName = initialName + " 2";
        final String updatedDescription = initialDescription  +" 2";

        File initialZip = createZip(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  id: " + symbolicName,
                "  itemType: entity",
                "  name: " + initialName,
                "  description: " + initialDescription,
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity")));
        File updatedZip = createZip(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  id: " + symbolicName,
                "  itemType: entity",
                "  name: " + updatedName,
                "  description: " + updatedDescription,
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity")));

        client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(initialZip)));

        TypeSummary initialEntity = client().path("/catalog/types/" + symbolicName + "/" + TEST_VERSION)
                .get(TypeSummary.class);
        assertEquals(initialEntity.getDisplayName(), initialName);
        assertEquals(initialEntity.getDescription(), initialDescription);

        Response invalidResponse = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(updatedZip)));

        assertEquals(invalidResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        Response validResponse = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .query("force", true)
                .post(Streams.readFully(new FileInputStream(updatedZip)));

        assertEquals(validResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        TypeSummary entity = client().path("/catalog/types/" + symbolicName + "/" + TEST_VERSION)
                .get(TypeSummary.class);
        assertEquals(entity.getDisplayName(), updatedName);
        assertEquals(entity.getDescription(), updatedDescription);
    }

    @Test
    public void testForceUpdateForJar() throws Exception {
        final String symbolicName = "force.update.jar.catalog.application.id";
        final String initialName = "My Catalog App";
        final String initialDescription = "My Description";
        final String updatedName = initialName + " 2";
        final String updatedDescription = initialDescription  +" 2";

        File initialJar = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  id: " + symbolicName,
                "  itemType: entity",
                "  name: " + initialName,
                "  description: " + initialDescription,
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity")));
        File updatedJar = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  id: " + symbolicName,
                "  itemType: entity",
                "  name: " + updatedName,
                "  description: " + updatedDescription,
                "  icon_url: classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif",
                "  item:",
                "    type: org.apache.brooklyn.core.test.entity.TestEntity")));

        client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(initialJar)));

        TypeSummary initialEntity = client().path("/catalog/types/" + symbolicName + "/" + TEST_VERSION)
                .get(TypeSummary.class);
        assertEquals(initialEntity.getDisplayName(), initialName);
        assertEquals(initialEntity.getDescription(), initialDescription);

        Response invalidResponse = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(updatedJar)));

        assertEquals(invalidResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

        Response validResponse = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .query("force", true)
                .post(Streams.readFully(new FileInputStream(updatedJar)));

        assertEquals(validResponse.getStatus(), Response.Status.CREATED.getStatusCode());

        TypeSummary entity = client().path("/catalog/types/" + symbolicName + "/" + TEST_VERSION)
                .get(TypeSummary.class);
        assertEquals(entity.getDisplayName(), updatedName);
        assertEquals(entity.getDescription(), updatedDescription);
    }
    
    // TODO traits no longer always set - we have supertypes so not needed, we should investigate when they are and when they aren't
    // and switch those to setting and using the supertypes
    private boolean checkTraits(boolean currentExpectedToBeWorking) {
        return currentExpectedToBeWorking;
    }

    @Test
    public void testAddSameTypeTwiceInSameBundle_SilentlyDeduped() throws Exception {
        final String symbolicName = "test.duplicate.type."+JavaClassNames.niceClassAndMethod();
        final String entityName = symbolicName+".type";

        File jar = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + entityName,
                "    itemType: entity",
                "    name: T",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  - id: " + entityName,
                "    itemType: entity",
                "    name: T",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity")));

        Response result = client().path("/catalog/bundles")
            .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
            .post(Streams.readFully(new FileInputStream(jar)));
        HttpAsserts.assertHealthyStatusCode(result.getStatus());

        TypeSummary entity = client().path("/catalog/types/" + entityName + "/" + TEST_VERSION)
                .get(TypeSummary.class);
        assertEquals(entity.getDisplayName(), "T");
        
        List<TypeSummary> entities = client().path("/catalog/types/" + entityName)
                .get(new GenericType<List<TypeSummary>>() {});
        Asserts.assertSize(entities, 1);
        assertEquals(Iterables.getOnlyElement(entities), entity);
        
        BundleSummary bundle = client().path("/catalog/bundles/" + symbolicName + "/" + TEST_VERSION)
            .get(BundleSummary.class);
        Asserts.assertSize(bundle.getTypes(), 1);
        assertEquals(Iterables.getOnlyElement(bundle.getTypes()), entity);

    }
    
    @Test
    // different metadata is allowed as that doesn't affect operation (but different definition is not, see below)
    // if in same bundle, it's deduped and last one wins; should warn and could disallow, but if type is pulled in 
    // multiple times from copied files, it feels convenient just to dedupe and forgive minor metadata changes;
    // if in different bundles, see other test below, but in that case both are added
    public void testAddSameTypeTwiceInSameBundleDifferentDisplayName_LastOneWins() throws Exception {
        final String symbolicName = "test.duplicate.type."+JavaClassNames.niceClassAndMethod();
        final String entityName = symbolicName+".type";

        File jar = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + entityName,
                "    itemType: entity",
                "    name: T",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  - id: " + entityName,
                "    itemType: entity",
                "    name: U",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity")));

        Response result = client().path("/catalog/bundles")
            .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
            .post(Streams.readFully(new FileInputStream(jar)));
        HttpAsserts.assertHealthyStatusCode(result.getStatus());

        TypeSummary entity = client().path("/catalog/types/" + entityName + "/" + TEST_VERSION)
                .get(TypeSummary.class);
        assertEquals(entity.getDisplayName(), "U");
        
        List<TypeSummary> entities = client().path("/catalog/types/" + entityName)
                .get(new GenericType<List<TypeSummary>>() {});
        Asserts.assertSize(entities, 1);
        assertEquals(Iterables.getOnlyElement(entities), entity);
        
        BundleSummary bundle = client().path("/catalog/bundles/" + symbolicName + "/" + TEST_VERSION)
            .get(BundleSummary.class);
        Asserts.assertSize(bundle.getTypes(), 1);
        assertEquals(Iterables.getOnlyElement(bundle.getTypes()), entity);
    }
    
    @Test
    // would be confusing if the _definition_ is different however, as one will be ignored
    public void testAddSameTypeTwiceInSameBundleDifferentDefinition_Disallowed() throws Exception {
        final String symbolicName = "test.duplicate.type."+JavaClassNames.niceClassAndMethod();
        final String entityName = symbolicName+".type";
        final String entityNameOkay = symbolicName+".okayType";

        File jar = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + entityNameOkay,
                "    itemType: entity",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  - id: " + entityName,
                "    itemType: entity",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      a.config: first_definition",
                "  - id: " + entityName,
                "    itemType: entity",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      a.config: second_definition_makes_it_different_so_disallowed")));

        Response result = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(jar)));
        HttpAsserts.assertNotHealthyStatusCode(result.getStatus());
        String resultS = Streams.readFullyString((InputStream)result.getEntity());
        Asserts.assertStringContainsIgnoreCase(resultS, "different plan", entityName);
        Asserts.assertStringDoesNotContain(resultS, entityNameOkay);

        // entity not added
        Response get = client().path("/catalog/types/" + entityName + "/" + TEST_VERSION).get();
        assertEquals(get.getStatus(), 404);
        
        List<TypeSummary> entities = client().path("/catalog/types/" + entityName)
                .get(new GenericType<List<TypeSummary>>() {});
        Asserts.assertSize(entities, 0);
        
        // nor is the okay entity
        Response getOkay = client().path("/catalog/types/" + entityNameOkay + "/" + TEST_VERSION).get();
        assertEquals(getOkay.getStatus(), 404);
        
        // and nor is the bundle
        Response getBundle = client().path("/catalog/bundles/" + symbolicName + "/" + TEST_VERSION).get();
        assertEquals(getBundle.getStatus(), 404);
    }
    
    // TODO might in future want to allow this if the user adding the type cannot see the other type due to entitlements;
    // means however there might be another user who _can_ see the two different types 
    private final static boolean DISALLOW_DIFFERENCES_IN_SAME_TYPE_ID_FROM_DIFFERENT_BUNDLES = true;
    
    @Test
    public void testAddSameTypeTwiceInDifferentBundleDifferentDefinition_Disallowed() throws Exception {
        Preconditions.checkArgument(DISALLOW_DIFFERENCES_IN_SAME_TYPE_ID_FROM_DIFFERENT_BUNDLES);
        // if above changed, assert that both types are added
        
        final String symbolicName1 = "test.duplicate.type."+JavaClassNames.niceClassAndMethod()+".1";
        final String symbolicName2 = "test.duplicate.type."+JavaClassNames.niceClassAndMethod()+".2";
        final String entityName = "test.duplicate.type."+JavaClassNames.niceClassAndMethod()+".type";

        File jar1 = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName1,
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + entityName,
                "    itemType: entity",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      a.config: in_bundle1")));
        Response result1 = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(jar1)));
        HttpAsserts.assertHealthyStatusCode(result1.getStatus());
        
        File jar2 = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  bundle: " + symbolicName2,
            "  version: " + TEST_VERSION,
            "  items:",
            "  - id: " + entityName,
            "    itemType: entity",
            "    item:",
            "      type: org.apache.brooklyn.core.test.entity.TestEntity",
            "      a.config: in_bundle2")));
        Response result2 = client().path("/catalog/bundles")
            .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
            .post(Streams.readFully(new FileInputStream(jar2)));
        String resultS = Streams.readFullyString((InputStream)result2.getEntity());
        HttpAsserts.assertNotHealthyStatusCode(result2.getStatus());
        Asserts.assertStringContainsIgnoreCase(resultS, "it is different to", entityName, "different bundle", symbolicName1);
    }
    
    @Test
    public void testAddSameTypeTwiceInDifferentBundleSameDefinition_AllowedAndApiMakesTheDifferentOnesClear() throws Exception {
        final String symbolicName1 = "test.duplicate.type."+JavaClassNames.niceClassAndMethod()+".1";
        final String symbolicName2 = "test.duplicate.type."+JavaClassNames.niceClassAndMethod()+".2";
        final String entityName = "test.duplicate.type."+JavaClassNames.niceClassAndMethod()+".type";

        File jar1 = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  bundle: " + symbolicName1,
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + entityName,
                "    itemType: entity",
                "    name: T",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity")));
        Response result1 = client().path("/catalog/bundles")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(jar1)));
        HttpAsserts.assertHealthyStatusCode(result1.getStatus());
        
        File jar2 = createJar(ImmutableMap.<String, String>of("catalog.bom", Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  bundle: " + symbolicName2,
            "  version: " + TEST_VERSION,
            "  items:",
            "  - id: " + entityName,
            "    itemType: entity",
            "    name: U",
            "    item:",
            "      type: org.apache.brooklyn.core.test.entity.TestEntity")));
        Response result2 = client().path("/catalog/bundles")
            .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
            .post(Streams.readFully(new FileInputStream(jar2)));
        HttpAsserts.assertHealthyStatusCode(result2.getStatus());
        
        TypeSummary entity = client().path("/catalog/types/" + entityName + "/" + TEST_VERSION)
            .get(TypeSummary.class);
        // type should be present twice, but bundle 1 preferred because first alphanumerically; 
        assertEquals(entity.getContainingBundle(), symbolicName1+":"+TEST_VERSION);
        // (however this might be weakened in future)
//        Asserts.assertStringContains("["+symbolicName1+":"+TEST_VERSION+"] OR ["+symbolicName2+":"+TEST_VERSION+"]", 
//            "["+entity.getContainingBundle()+"]");
        TypeSummary entity1 = entity;
        
        List<TypeSummary> entities = client().path("/catalog/types/" + entityName)
                .get(new GenericType<List<TypeSummary>>() {});
        Asserts.assertSize(entities, 2);
        assertEquals(entities.get(0), entity1);
        TypeSummary entity2 = entities.get(1);
        assertEquals(entity2.getContainingBundle(), symbolicName2+":"+TEST_VERSION);
        Assert.assertNotEquals(entity1, entity2);
        
        assertEquals(entity1.getDisplayName(), "T");
        assertEquals(entity2.getDisplayName(), "U");
        
        BundleSummary bundle1 = client().path("/catalog/bundles/" + symbolicName1 + "/" + TEST_VERSION)
            .get(BundleSummary.class);
        Asserts.assertSize(bundle1.getTypes(), 1);
        assertEquals(Iterables.getOnlyElement(bundle1.getTypes()), entity1);        
        
        BundleSummary bundle2 = client().path("/catalog/bundles/" + symbolicName2 + "/" + TEST_VERSION)
            .get(BundleSummary.class);
        Asserts.assertSize(bundle2.getTypes(), 1);
        assertEquals(Iterables.getOnlyElement(bundle2.getTypes()), entity2);
        
        @SuppressWarnings("unchecked")
        String self1 = ((Map<String,String>)entity1.getExtraFields().get("links")).get("self");
        @SuppressWarnings("unchecked")
        String self2 = ((Map<String,String>)entity2.getExtraFields().get("links")).get("self");
        
        Assert.assertNotEquals(self1, self2);
        
        TypeSummary entity1r = client().path(self1).get(TypeSummary.class);
        TypeSummary entity2r = client().path(self2).get(TypeSummary.class);
        Assert.assertEquals(entity1r, entity1);
        Assert.assertEquals(entity2r, entity2);
    }
    
}
