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
import java.io.FileNotFoundException;
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
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.policy.autoscaling.AutoScalerPolicy;
import org.apache.brooklyn.rest.domain.CatalogEntitySummary;
import org.apache.brooklyn.rest.domain.CatalogItemSummary;
import org.apache.brooklyn.rest.domain.CatalogLocationSummary;
import org.apache.brooklyn.rest.domain.CatalogPolicySummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.reporters.Files;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Test( // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "CatalogResourceTest")
public class CatalogResourceTest extends BrooklynRestResourceTest {

    private static final Logger log = LoggerFactory.getLogger(CatalogResourceTest.class);
    
    private static String TEST_VERSION = "0.1.2";

    @Override
    protected boolean useLocalScannedCatalog() {
        return true;
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

        Response response = client().path("/catalog")
                .post(yaml);

        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        CatalogEntitySummary entityItem = client().path("/catalog/entities/"+symbolicName + "/" + TEST_VERSION)
                .get(CatalogEntitySummary.class);

        Assert.assertNotNull(entityItem.getPlanYaml());
        Assert.assertTrue(entityItem.getPlanYaml().contains("org.apache.brooklyn.core.test.entity.TestEntity"));

        assertEquals(entityItem.getId(), ver(symbolicName));
        assertEquals(entityItem.getSymbolicName(), symbolicName);
        assertEquals(entityItem.getVersion(), TEST_VERSION);

        // and internally let's check we have libraries
        RegisteredType item = getManagementContext().getTypeRegistry().get(symbolicName, TEST_VERSION);
        Assert.assertNotNull(item);
        Collection<OsgiBundleWithUrl> libs = item.getLibraries();
        assertEquals(libs.size(), 1);
        assertEquals(Iterables.getOnlyElement(libs).getUrl(), bundleUrl);

        // now let's check other things on the item
        URI expectedIconUrl = URI.create(getEndpointAddress() + "/catalog/icon/" + symbolicName + "/" + entityItem.getVersion()).normalize();
        assertEquals(entityItem.getName(), "My Catalog App");
        assertEquals(entityItem.getDescription(), "My description");
        assertEquals(entityItem.getIconUrl(), expectedIconUrl.getPath());
        assertEquals(item.getIconUrl(), "classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif");

        // an InterfacesTag should be created for every catalog item
        assertEquals(entityItem.getTags().size(), 1);
        Object tag = entityItem.getTags().iterator().next();
        @SuppressWarnings("unchecked")
        List<String> actualInterfaces = ((Map<String, List<String>>) tag).get("traits");
        List<Class<?>> expectedInterfaces = Reflections.getAllInterfaces(TestEntity.class);
        assertEquals(actualInterfaces.size(), expectedInterfaces.size());
        for (Class<?> expectedInterface : expectedInterfaces) {
            assertTrue(actualInterfaces.contains(expectedInterface.getName()));
        }

        byte[] iconData = client().path("/catalog/icon/" + symbolicName + "/" + TEST_VERSION).get(byte[].class);
        assertEquals(iconData.length, 43);
    }

    @Test
    // osgi may fail in IDE, typically works on mvn CLI though
    public void testRegisterOsgiPolicyTopLevelSyntax() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = "my.catalog.policy.id";
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

        CatalogPolicySummary entityItem = Iterables.getOnlyElement( client().path("/catalog")
                .post(yaml, new GenericType<Map<String,CatalogPolicySummary>>() {}).values() );

        Assert.assertNotNull(entityItem.getPlanYaml());
        Assert.assertTrue(entityItem.getPlanYaml().contains(policyType));
        assertEquals(entityItem.getId(), ver(symbolicName));
        assertEquals(entityItem.getSymbolicName(), symbolicName);
        assertEquals(entityItem.getVersion(), TEST_VERSION);
    }

    @Test
    public void testListAllEntities() {
        List<CatalogEntitySummary> entities = client().path("/catalog/entities")
                .get(new GenericType<List<CatalogEntitySummary>>() {});
        assertTrue(entities.size() > 0);
    }

    @Test
    public void testListAllEntitiesAsItem() {
        // ensure things are happily downcasted and unknown properties ignored (e.g. sensors, effectors)
        List<CatalogItemSummary> entities = client().path("/catalog/entities")
                .get(new GenericType<List<CatalogItemSummary>>() {});
        assertTrue(entities.size() > 0);
    }

    @Test
    public void testFilterListOfEntitiesByName() {
        List<CatalogEntitySummary> entities = client().path("/catalog/entities")
                .query("fragment", "vaNIllasOFTWAREpROCESS").get(new GenericType<List<CatalogEntitySummary>>() {});
        assertEquals(entities.size(), 1);

        log.info("RedisCluster-like entities are: " + entities);

        List<CatalogEntitySummary> entities2 = client().path("/catalog/entities")
                .query("regex", "[Vv]an.[alS]+oftware\\w+").get(new GenericType<List<CatalogEntitySummary>>() {});
        assertEquals(entities2.size(), 1);

        assertEquals(entities, entities2);
    
        List<CatalogEntitySummary> entities3 = client().path("/catalog/entities")
                .query("fragment", "bweqQzZ").get(new GenericType<List<CatalogEntitySummary>>() {});
        assertEquals(entities3.size(), 0);

        List<CatalogEntitySummary> entities4 = client().path("/catalog/entities")
                .query("regex", "bweq+z+").get(new GenericType<List<CatalogEntitySummary>>() {});
        assertEquals(entities4.size(), 0);
    }

    @Test
    @Deprecated
    // If we move to using a yaml catalog item, the details will be of the wrapping app,
    // not of the entity itself, so the test won't make sense any more.
    public void testGetCatalogEntityDetails() {
        CatalogEntitySummary details = client()
                .path(URI.create("/catalog/entities/org.apache.brooklyn.rest.resources.DummyIconEntity"))
                .get(CatalogEntitySummary.class);
        assertTrue(details.toString().contains("dummy.config"), "expected more config, only got: "+details);
        // No icons in brooklyn-server entities
        String iconUrl = "/catalog/icon/" + details.getSymbolicName();
        assertTrue(details.getIconUrl().contains(iconUrl), "expected brooklyn URL for icon image, but got: " + details.getIconUrl());
    }

    @Test
    @Deprecated
    // If we move to using a yaml catalog item, the details will be of the wrapping app,
    // not of the entity itself, so the test won't make sense any more.
    public void testGetCatalogEntityPlusVersionDetails() {
        CatalogEntitySummary details = client()
                .path(URI.create("/catalog/entities/org.apache.brooklyn.rest.resources.DummyIconEntity:0.0.0.SNAPSHOT"))
                .get(CatalogEntitySummary.class);
        assertTrue(details.toString().contains("dummy.config"), "expected more config, only got: "+details);
        URI expectedIconUrl = URI.create(getEndpointAddress() + "/catalog/icon/" + details.getSymbolicName() + "/" + details.getVersion()).normalize();
        assertEquals(details.getIconUrl(), expectedIconUrl.getPath(), "expected brooklyn URL for icon image ("+expectedIconUrl+"), but got: "+details.getIconUrl());
    }

    @Test
    public void testGetCatalogEntityIconDetails() throws IOException {
        String catalogItemId = "testGetCatalogEntityIconDetails";
        addTestCatalogItemAsEntity(catalogItemId);
        Response response = client().path(URI.create("/catalog/icon/" + catalogItemId + "/" + TEST_VERSION))
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
                "  version: " + TEST_VERSION,
                "  itemType: " + checkNotNull(itemType),
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath:///bridge-small.png",
                "  version: " + version,
                "  item:",
                "    type: " + service);

        client().path("/catalog").post(yaml);
    }

    private enum DeprecateStyle {
        NEW_STYLE,
        LEGACY_STYLE
    }
    private void deprecateCatalogItem(DeprecateStyle style, String symbolicName, String version, boolean deprecated) {
        String id = String.format("%s:%s", symbolicName, version);
        Response response;
        if (style == DeprecateStyle.NEW_STYLE) {
            response = client().path(String.format("/catalog/entities/%s/deprecated", id))
                    .header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON)
                    .post(deprecated);
        } else {
            response = client().path(String.format("/catalog/entities/%s/deprecated/%s", id, deprecated))
                    .post(null);
        }
        assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
    }

    private void disableCatalogItem(String symbolicName, String version, boolean disabled) {
        String id = String.format("%s:%s", symbolicName, version);
        Response getDisableResponse = client().path(String.format("/catalog/entities/%s/disabled", id))
                .header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON)
                .post(disabled);
        assertEquals(getDisableResponse.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
    }

    @Test
    public void testListPolicies() {
        Set<CatalogPolicySummary> policies = client().path("/catalog/policies")
                .get(new GenericType<Set<CatalogPolicySummary>>() {});

        assertTrue(policies.size() > 0);
        CatalogItemSummary asp = null;
        for (CatalogItemSummary p : policies) {
            if (AutoScalerPolicy.class.getName().equals(p.getType()))
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
        Map<String, CatalogLocationSummary> items = client().path("/catalog")
                .post(yaml, new GenericType<Map<String,CatalogLocationSummary>>() {});
        CatalogLocationSummary locationItem = Iterables.getOnlyElement(items.values());

        Assert.assertNotNull(locationItem.getPlanYaml());
        Assert.assertTrue(locationItem.getPlanYaml().contains(locationType));
        assertEquals(locationItem.getId(), ver(symbolicName));
        assertEquals(locationItem.getSymbolicName(), symbolicName);
        assertEquals(locationItem.getVersion(), TEST_VERSION);

        // Retrieve location item
        CatalogLocationSummary location = client().path("/catalog/locations/"+symbolicName+"/"+TEST_VERSION)
                .get(CatalogLocationSummary.class);
        assertEquals(location.getSymbolicName(), symbolicName);

        // Retrieve all locations
        Set<CatalogLocationSummary> locations = client().path("/catalog/locations")
                .get(new GenericType<Set<CatalogLocationSummary>>() {});
        boolean found = false;
        for (CatalogLocationSummary contender : locations) {
            if (contender.getSymbolicName().equals(symbolicName)) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found, "contenders="+locations);
        
        // Delete
        Response deleteResponse = client().path("/catalog/locations/"+symbolicName+"/"+TEST_VERSION)
                .delete();
        assertEquals(deleteResponse.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        Response getPostDeleteResponse = client().path("/catalog/locations/"+symbolicName+"/"+TEST_VERSION)
                .get();
        assertEquals(getPostDeleteResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
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

        client().path("/catalog")
                .post(yaml);

        Response deleteResponse = client().path("/catalog/entities/"+symbolicName+"/"+TEST_VERSION)
                .delete();

        assertEquals(deleteResponse.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

        Response getPostDeleteResponse = client().path("/catalog/entities/"+symbolicName+"/"+TEST_VERSION)
                .get();
        assertEquals(getPostDeleteResponse.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void testSetDeprecated() {
        runSetDeprecated(DeprecateStyle.NEW_STYLE);
    }
    
    // Uses old-style "/catalog/{itemId}/deprecated/true", rather than the "true" in the request body.
    @Test
    @Deprecated
    public void testSetDeprecatedLegacy() {
        runSetDeprecated(DeprecateStyle.LEGACY_STYLE);
    }

    protected void runSetDeprecated(DeprecateStyle style) {
        String symbolicName = "my.catalog.item.id.for.deprecation";
        String serviceType = "org.apache.brooklyn.entity.stock.BasicApplication";
        addTestCatalogItem(symbolicName, "template", TEST_VERSION, serviceType);
        addTestCatalogItem(symbolicName, "template", "2.0", serviceType);
        try {
            List<CatalogEntitySummary> applications = client().path("/catalog/applications")
                    .query("fragment", symbolicName).query("allVersions", "true").get(new GenericType<List<CatalogEntitySummary>>() {});
            assertEquals(applications.size(), 2);
            CatalogItemSummary summary0 = applications.get(0);
            CatalogItemSummary summary1 = applications.get(1);
    
            // Deprecate: that app should be excluded
            deprecateCatalogItem(style, summary0.getSymbolicName(), summary0.getVersion(), true);
    
            List<CatalogEntitySummary> applicationsAfterDeprecation = client().path("/catalog/applications")
                    .query("fragment", "basicapp").query("allVersions", "true").get(new GenericType<List<CatalogEntitySummary>>() {});
    
            assertEquals(applicationsAfterDeprecation.size(), 1);
            assertTrue(applicationsAfterDeprecation.contains(summary1));
    
            // Un-deprecate: that app should be included again
            deprecateCatalogItem(style, summary0.getSymbolicName(), summary0.getVersion(), false);
    
            List<CatalogEntitySummary> applicationsAfterUnDeprecation = client().path("/catalog/applications")
                    .query("fragment", "basicapp").query("allVersions", "true").get(new GenericType<List<CatalogEntitySummary>>() {});
    
            assertEquals(applications, applicationsAfterUnDeprecation);
        } finally {
            client().path("/catalog/entities/"+symbolicName+"/"+TEST_VERSION)
                    .delete();
            client().path("/catalog/entities/"+symbolicName+"/"+"2.0")
                    .delete();
        }
    }

    @Test
    public void testSetDisabled() {
        String symbolicName = "my.catalog.item.id.for.disabling";
        String serviceType = "org.apache.brooklyn.entity.stock.BasicApplication";
        addTestCatalogItem(symbolicName, "template", TEST_VERSION, serviceType);
        addTestCatalogItem(symbolicName, "template", "2.0", serviceType);
        try {
            List<CatalogEntitySummary> applications = client().path("/catalog/applications")
                    .query("fragment", symbolicName).query("allVersions", "true").get(new GenericType<List<CatalogEntitySummary>>() {});
            assertEquals(applications.size(), 2);
            CatalogItemSummary summary0 = applications.get(0);
            CatalogItemSummary summary1 = applications.get(1);
    
            // Disable: that app should be excluded
            disableCatalogItem(summary0.getSymbolicName(), summary0.getVersion(), true);
    
            List<CatalogEntitySummary> applicationsAfterDisabled = client().path("/catalog/applications")
                    .query("fragment", "basicapp").query("allVersions", "true").get(new GenericType<List<CatalogEntitySummary>>() {});
    
            assertEquals(applicationsAfterDisabled.size(), 1);
            assertTrue(applicationsAfterDisabled.contains(summary1));
    
            // Un-disable: that app should be included again
            disableCatalogItem(summary0.getSymbolicName(), summary0.getVersion(), false);
    
            List<CatalogEntitySummary> applicationsAfterUnDisabled = client().path("/catalog/applications")
                    .query("fragment", "basicapp").query("allVersions", "true").get(new GenericType<List<CatalogEntitySummary>>() {});
    
            assertEquals(applications, applicationsAfterUnDisabled);
        } finally {
            client().path("/catalog/entities/"+symbolicName+"/"+TEST_VERSION)
                    .delete();
            client().path("/catalog/entities/"+symbolicName+"/"+"2.0")
                    .delete();
        }
    }

    @Test
    public void testAddUnreachableItem() {
        addAddCatalogItemWithInvalidBundleUrl("http://0.0.0.0/can-not-connect");
    }

    @Test
    public void testAddInvalidItem() {
        //equivalent to HTTP response 200 text/html
        addAddCatalogItemWithInvalidBundleUrl("classpath://not-a-jar-file.txt");
    }

    @Test
    public void testAddMissingItem() {
        //equivalent to HTTP response 404 text/html
        addAddCatalogItemWithInvalidBundleUrl("classpath://missing-jar-file.txt");
    }

    @Test
    public void testInvalidArchive() throws Exception {
        File f = Os.newTempFile("osgi", "zip");

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertTrue(response.readEntity(String.class).contains("Invalid ZIP/JAR archive"));
    }

    @Test
    public void testArchiveWithoutBom() throws Exception {
        File f = createZip(ImmutableMap.<String, String>of());

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertTrue(response.readEntity(String.class).contains("Archive must contain a catalog.bom file in the root"));
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

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertTrue(response.readEntity(String.class).contains("Catalog BOM must define bundle and version"));
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

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertTrue(response.readEntity(String.class).contains("Catalog BOM must define bundle"));
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

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertTrue(response.readEntity(String.class).contains("Catalog BOM must define version"));
    }

    @Test
    public void testJarWithoutMatchingBundle() throws Exception {
        String name = "My Catalog App";
        String bundle = "org.apache.brooklyn.test";
        String version = "0.1.0";
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
                        "Bundle-SymbolicName: org.apache.brooklyn.test2",
                        "Bundle-Version: " + version,
                        "Bundle-ManifestVersion: " + version)));

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertTrue(response.readEntity(String.class).contains("JAR MANIFEST symbolic-name 'org.apache.brooklyn.test2' does not match '"+bundle+"' defined in BOM"));
    }

    @Test
    public void testJarWithoutMatchingVersion() throws Exception {
        String name = "My Catalog App";
        String bundle = "org.apache.brooklyn.test";
        String version = "0.1.0";
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
                        "Bundle-Version: 0.3.0",
                        "Bundle-ManifestVersion: 0.3.0")));

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(f)));

        assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertTrue(response.readEntity(String.class).contains("JAR MANIFEST version '0.3.0' does not match '"+version+"' defined in BOM"));
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

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-jar")
                .post(Streams.readFully(new FileInputStream(f)));

        
        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        CatalogEntitySummary entityItem = client().path("/catalog/entities/"+symbolicName + "/" + version)
                .get(CatalogEntitySummary.class);

        Assert.assertNotNull(entityItem.getPlanYaml());
        Assert.assertTrue(entityItem.getPlanYaml().contains("org.apache.brooklyn.core.test.entity.TestEntity"));

        assertEquals(entityItem.getId(), CatalogUtils.getVersionedId(symbolicName, version));
        assertEquals(entityItem.getSymbolicName(), symbolicName);
        assertEquals(entityItem.getVersion(), version);

        // and internally let's check we have libraries
        RegisteredType item = getManagementContext().getTypeRegistry().get(symbolicName, version);
        Assert.assertNotNull(item);
        Collection<OsgiBundleWithUrl> libs = item.getLibraries();
        assertEquals(libs.size(), 1);
        OsgiBundleWithUrl lib = Iterables.getOnlyElement(libs);
        Assert.assertNull(lib.getUrl());

        assertEquals(lib.getSymbolicName(), "org.apache.brooklyn.test.resources.osgi.brooklyn-test-osgi-entities");
        assertEquals(lib.getVersion(), version);

        // now let's check other things on the item
        URI expectedIconUrl = URI.create(getEndpointAddress() + "/catalog/icon/" + symbolicName + "/" + entityItem.getVersion()).normalize();
        assertEquals(entityItem.getName(), "My Catalog App");
        assertEquals(entityItem.getDescription(), "My description");
        assertEquals(entityItem.getIconUrl(), expectedIconUrl.getPath());
        assertEquals(item.getIconUrl(), "classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif");

        // an InterfacesTag should be created for every catalog item
        assertEquals(entityItem.getTags().size(), 1);
        Object tag = entityItem.getTags().iterator().next();
        @SuppressWarnings("unchecked")
        List<String> actualInterfaces = ((Map<String, List<String>>) tag).get("traits");
        List<Class<?>> expectedInterfaces = Reflections.getAllInterfaces(TestEntity.class);
        assertEquals(actualInterfaces.size(), expectedInterfaces.size());
        for (Class<?> expectedInterface : expectedInterfaces) {
            assertTrue(actualInterfaces.contains(expectedInterface.getName()));
        }

        byte[] iconData = client().path("/catalog/icon/" + symbolicName + "/" + version).get(byte[].class);
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

        Response response = client().path("/catalog")
                .header(HttpHeaders.CONTENT_TYPE, "application/x-zip")
                .post(Streams.readFully(new FileInputStream(f)));


        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        CatalogEntitySummary entityItem = client().path("/catalog/entities/"+symbolicName + "/" + version)
                .get(CatalogEntitySummary.class);

        Assert.assertNotNull(entityItem.getPlanYaml());
        Assert.assertTrue(entityItem.getPlanYaml().contains(entityType));

        assertEquals(entityItem.getId(), CatalogUtils.getVersionedId(symbolicName, version));
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
        assertEquals(lib.getVersion(), version);

        // now let's check other things on the item
        URI expectedIconUrl = URI.create(getEndpointAddress() + "/catalog/icon/" + symbolicName + "/" + entityItem.getVersion()).normalize();
        assertEquals(entityItem.getName(), "My Catalog App");
        assertEquals(entityItem.getDescription(), "My description");
        assertEquals(entityItem.getIconUrl(), expectedIconUrl.getPath());
        assertEquals(item.getIconUrl(), "classpath:" + iconPath);

        // an InterfacesTag should be created for every catalog item
        assertEquals(entityItem.getTags().size(), 1);
        Object tag = entityItem.getTags().iterator().next();
        @SuppressWarnings("unchecked")
        List<String> actualInterfaces = ((Map<String, List<String>>) tag).get("traits");
        List<String> expectedInterfaces = ImmutableList.of(Entity.class.getName(), BrooklynObject.class.getName(), Identifiable.class.getName(), Configurable.class.getName());
        assertTrue(actualInterfaces.containsAll(expectedInterfaces), "actual="+actualInterfaces);

        byte[] iconData = client().path("/catalog/icon/" + symbolicName + "/" + version).get(byte[].class);
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

    private void addAddCatalogItemWithInvalidBundleUrl(String bundleUrl) {
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

        Response response = client().path("/catalog")
                .post(yaml);

        assertEquals(response.getStatus(), HttpStatus.BAD_REQUEST_400);
    }

    private static String ver(String id) {
        return CatalogUtils.getVersionedId(id, TEST_VERSION);
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
}
