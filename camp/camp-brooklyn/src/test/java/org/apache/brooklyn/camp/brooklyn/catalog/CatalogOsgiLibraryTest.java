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
package org.apache.brooklyn.camp.brooklyn.catalog;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.config.external.AbstractExternalConfigSupplier;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.internal.ExternalConfigSupplierRegistry;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.http.TestHttpRecordingRequestInterceptor;
import org.apache.brooklyn.test.http.TestHttpRequestHandler;
import org.apache.brooklyn.test.http.TestHttpServer;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;

public class CatalogOsgiLibraryTest extends AbstractYamlTest {
    
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CatalogOsgiLibraryTest.class);

    private TestHttpServer webServer;
    private TestHttpRecordingRequestInterceptor requestInterceptor;
    
    private String jarName = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SYMBOLIC_NAME_FINAL_PART + ".jar";
    private String malformedJarName = "thisIsNotAJar.jar";
    private String classpathUrl = "classpath:" + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH;
    private URL jarUrl;
    private URL malformedJarUrl;
    
    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        // Load the bytes of the jar
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), "/brooklyn/osgi/" + jarName);
        InputStream resource = getClass().getResourceAsStream("/brooklyn/osgi/" + jarName);
        final byte[] jarBytes = Streams.readFullyAndClose(resource);
        
        // Start a mock web-server that will return the jar
        requestInterceptor = new TestHttpRecordingRequestInterceptor();
        webServer = new TestHttpServer()
                .handler("/" + jarName, new TestHttpRequestHandler().code(200).response(jarBytes))
                .handler("/" + malformedJarName, new TestHttpRequestHandler().code(200).response("simulating-malformed-jar"))
                .interceptor(requestInterceptor)
                .start();
        jarUrl = new URL(Urls.mergePaths(webServer.getUrl(), jarName));
        malformedJarUrl = new URL(Urls.mergePaths(webServer.getUrl(), malformedJarName));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        try {
            if (webServer != null) webServer.stop();
        } finally {
            super.tearDown();
        }
    }
    
    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Test
    public void testLibraryStringWithClasspathUrl() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple-osgi-library",
                "  version: \"1.0\"",
                "  itemType: template",
                "  libraries:",
                "  - " + classpathUrl,
                "  item:",
                "    services:",
                "    - type: org.apache.brooklyn.test.osgi.entities.SimpleApplication");

        CatalogItem<?, ?> item = mgmt().getCatalog().getCatalogItem("simple-osgi-library", "1.0");
        assertCatalogLibraryUrl(item, classpathUrl);
    }

    @Test
    public void testLibraryMapWithClasspathUrl() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple-osgi-library",
                "  version: \"1.0\"",
                "  itemType: template",
                "  libraries:",
                "  - url: " + classpathUrl,
                "  item:",
                "    services:",
                "    - type: org.apache.brooklyn.test.osgi.entities.SimpleApplication");

        CatalogItem<?, ?> item = mgmt().getCatalog().getCatalogItem("simple-osgi-library", "1.0");
        assertCatalogLibraryUrl(item, classpathUrl);
    }

    @Test
    public void testLibraryHttpUrl() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple-osgi-library",
                "  version: \"1.0\"",
                "  itemType: template",
                "  libraries:",
                "  - " + jarUrl,
                "  item:",
                "    services:",
                "    - type: org.apache.brooklyn.test.osgi.entities.SimpleApplication");

        CatalogItem<?, ?> item = mgmt().getCatalog().getCatalogItem("simple-osgi-library", "1.0");
        assertCatalogLibraryUrl(item, jarUrl.toString());
    }

    @Test
    public void testLibraryUrlDoesNotExist() throws Exception {
        String wrongUrl = "classpath:/path/does/not/exist/aefjaifjie3kdd.jar";
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: simple-osgi-library",
                    "  version: \"1.0\"",
                    "  itemType: template",
                    "  libraries:",
                    "  - " + wrongUrl,
                    "  item:",
                    "    services:",
                    "    - type: " + BasicApplication.class.getName());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            if (!e.toString().contains("Bundle from " + wrongUrl + " failed to install")) {
                throw e;
            }
        }
    }

    @Test
    public void testLibraryMalformed() throws Exception {
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: simple-osgi-library",
                    "  version: \"1.0\"",
                    "  itemType: template",
                    "  libraries:",
                    "  - " + malformedJarUrl,
                    "  item:",
                    "    services:",
                    "    - type: " + BasicApplication.class.getName());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            if (!e.toString().contains("not a jar file")) {
                throw e;
            }
        }
    }

    @Test
    public void testLibraryUrlUsingExternalizedConfig() throws Exception {
        // Add an externalized config provider, which will return us the url
        Map<String, String> externalConfig = ImmutableMap.of("url", classpathUrl);
        ExternalConfigSupplierRegistry externalConfigProviderRegistry = ((ManagementContextInternal)mgmt()).getExternalConfigProviderRegistry();
        externalConfigProviderRegistry.addProvider("myprovider", new MyExternalConfigSupplier(mgmt(), "myprovider", externalConfig));

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple-osgi-library",
                "  version: \"1.0\"",
                "  itemType: template",
                "  libraries:",
                "  - $brooklyn:external(\"myprovider\", \"url\")",
                "  item:",
                "    services:",
                "    - type: org.apache.brooklyn.test.osgi.entities.SimpleApplication");

        CatalogItem<?, ?> item = mgmt().getCatalog().getCatalogItem("simple-osgi-library", "1.0");
        assertCatalogLibraryUrl(item, classpathUrl);
    }

    @Test
    public void testLibraryIsUsedByAppDeclaredInCatalog() throws Exception {
        // simplest case, an app in the bundle, can resolve files in the same bundle
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: item-from-library",
            "  version: \"1.0\"",
            "  itemType: template",
            "  libraries:",
            "  - " + classpathUrl,
            "  item:",
            "    services:",
            "    - type: org.apache.brooklyn.test.osgi.entities.SimpleApplication");
        Entity app = createAndStartApplication("services: [ { type: item-from-library } ]");
        assertCanFindMessages( app );
    }
    
    @Test
    public void testLibraryIsUsedByStockEntityDeclaredInCatalog() throws Exception {
        // slightly trickier case, an entity NOT in the bundle, declared as an item, can resolve files in its library bundles
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: item-from-library",
            "  version: \"1.0\"",
            "  itemType: template",
            "  libraries:",
            "  - " + classpathUrl,
            "  item:",
            "    services:",
            "    - type: "+TestEntity.class.getName());
        Entity app = createAndStartApplication("services: [ { type: item-from-library } ]");
        assertCanFindMessages( app.getChildren().iterator().next() );
    }

    protected void assertCanFindMessages(Entity entity) {
        ResourceUtils ru = ResourceUtils.create(entity);
        Iterable<URL> files = ru.getResources("org/apache/brooklyn/test/osgi/resources/message.txt");
        if (!files.iterator().hasNext()) {
            Entities.dumpInfo(entity);
            Assert.fail("Expected to find 'messages.txt'");
        }
    }
    
    @Test
    public void testLibraryIsUsedByChildInCatalogItem() throws Exception {
        // even trickier, something added as a child can resolve bundles
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: item-from-library",
            "  version: \"1.0\"",
            "  itemType: template",
            "  libraries:",
            "  - " + classpathUrl,
            "  item:",
            "    services:",
            "    - type: "+TestEntity.class.getName(),
            "      name: parent",
            "      brooklyn.children:",
            "      - type: "+TestEntity.class.getName(),
            "        name: child");
        
        RegisteredType item = mgmt().getTypeRegistry().get("item-from-library", "1.0");
        Assert.assertNotNull(item, "Should have had item-from-library in catalog");
        AbstractBrooklynObjectSpec<?, ?> spec = mgmt().getTypeRegistry().createSpec(item, null, null);
        Assert.assertNotNull(spec, "Should have had spec");
        // the spec has no catalog item ID except at the root Application
        
        Entity app = createAndStartApplication("services: [ { type: item-from-library } ]");
        Entity entity = app.getChildren().iterator().next();
        entity = entity.getChildren().iterator().next();
        Entities.dumpInfo(entity);
        
        // TODO re-enable when we've converted to a search path;
        // currently this test method passes because of CatalogUtils.setCatalogItemIdOnAddition
        // but we don't want to be doing that, we only want the search path
        //Assert.assertNull(entity.getCatalogItemId(), "Entity had a catalog item ID, even though it was stockj");
        
        assertCanFindMessages( entity );
    }
    
    @Test
    public void testLibraryIsUsedByChildInCatalogItemIfItIsFromCatalogItem() throws Exception {
        // even trickier, something added as a child can resolve bundles
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        addCatalogItems(
            "brooklyn.catalog:",
            "  version: \"1.0\"",
            "  itemType: template",
            "  libraries:",
            "  - " + classpathUrl,
            "  items:",
            "  - id: child-from-library",
            "    item:",
            "      services:",
            "      - type: "+TestEntity.class.getName(),
            "  - id: parent-with-child-from-library",
            "    item:",
            "      services:",
            "      - type: "+TestEntity.class.getName(),
            "        brooklyn.children:",
            "        - type: child-from-library");
        
        Entity app = createAndStartApplication("services: [ { type: parent-with-child-from-library } ]");
        Entity entity = app.getChildren().iterator().next();
        entity = entity.getChildren().iterator().next();
        
        assertCanFindMessages( entity );
    }

    @Test(groups="Broken")
    public void testLibraryUrlUsingExternalizedConfigForCredentials() throws Exception {
        runLibraryUrlUsingExternalizedConfigForCredentials(true);
    }
    
    @Test
    public void testLibraryUrlUsingExternalizedConfigForCredentialsLenient() throws Exception {
        runLibraryUrlUsingExternalizedConfigForCredentials(false);
    }
    
    /**
     * See https://issues.apache.org/jira/browse/BROOKLYN-421.
     * 
     * TODO java.net.URLEncoder.encode() gets it wrong for:
     * <ul>
     *   <li>" " (i.e. space - char %20). It turns that into "+" in the url.
     *   <li>"*" (i.e char %2A) is not escaped - this is wrong according to https://en.wikipedia.org/wiki/Percent-encoding (RFC 3986).
     * </ul>
     */
    protected void runLibraryUrlUsingExternalizedConfigForCredentials(boolean includeUrlEncoderBrokenChars) throws Exception {
        StringBuilder passwordBuilder = new StringBuilder();
        for (int i = 1; i < 128; i++) {
            if (!includeUrlEncoderBrokenChars && (i == 32 || i == 42)) continue;
            passwordBuilder.append((char)i);
        }
        String username = "myuser@mydomain.com";
        String password = passwordBuilder.toString();
        
        // Add an externalized config provider, which will return us a username + password
        Map<String, String> externalConfig = ImmutableMap.of("username", username, "password", password);
        ExternalConfigSupplierRegistry externalConfigProviderRegistry = ((ManagementContextInternal)mgmt()).getExternalConfigProviderRegistry();
        externalConfigProviderRegistry.addProvider("myprovider", new MyExternalConfigSupplier(mgmt(), "myprovider", externalConfig));

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple-osgi-library",
                "  version: \"1.0\"",
                "  itemType: template",
                "  libraries:",
                "  - $brooklyn:formatString:",
                "    - http://%s:%s@" + jarUrl.getHost() + ":" + jarUrl.getPort() + jarUrl.getPath(),
                "    - $brooklyn:urlEncode:",
                "      - $brooklyn:external(\"myprovider\", \"username\")",
                "    - $brooklyn:urlEncode:",
                "      - $brooklyn:external(\"myprovider\", \"password\")",
                "  item:",
                "    services:",
                "    - type: org.apache.brooklyn.test.osgi.entities.SimpleApplication");

        // Expect basic-auth used when retrieving jar
        HttpRequest req = requestInterceptor.getLastRequest();
        Header[] authHeaders = req.getHeaders("Authorization");
        assertEquals(authHeaders.length, 1, "authHeaders=" + Arrays.toString(authHeaders));
        String authHeader = authHeaders[0].getValue();
        String expectedHeader = "Basic " + BaseEncoding.base64().encode((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        assertEquals(authHeader, expectedHeader, "headers=" + Arrays.toString(req.getAllHeaders()));

        // Expect url to have been correctly escaped
        String escapedUsername = "myuser%40mydomain.com";
        String escapedPassword;
        if (includeUrlEncoderBrokenChars) {
            escapedPassword = "%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%10%11%12%13%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%20%21%22%23%24%25%26%27%28%29%2A%2B%2C-.%2F0123456789%3A%3B%3C%3D%3E%3F%40ABCDEFGHIJKLMNOPQRSTUVWXYZ%5B%5C%5D%5E_%60abcdefghijklmnopqrstuvwxyz%7B%7C%7D%7E%7F";
        } else {
            escapedPassword = "%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%10%11%12%13%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%21%22%23%24%25%26%27%28%29%2B%2C-.%2F0123456789%3A%3B%3C%3D%3E%3F%40ABCDEFGHIJKLMNOPQRSTUVWXYZ%5B%5C%5D%5E_%60abcdefghijklmnopqrstuvwxyz%7B%7C%7D%7E%7F";
        }
        String expectedUrl = "http://" + escapedUsername + ":" + escapedPassword+ "@" + jarUrl.getHost() + ":" + jarUrl.getPort() + jarUrl.getPath();
        
        CatalogItem<?, ?> item = mgmt().getCatalog().getCatalogItem("simple-osgi-library", "1.0");
        assertCatalogLibraryUrl(item, expectedUrl);
    }
    
    public static class MyExternalConfigSupplier extends AbstractExternalConfigSupplier {
        private final Map<String, String> conf;

        public MyExternalConfigSupplier(ManagementContext mgmt, String name, Map<String, String> conf) {
            super(mgmt, name);
            this.conf = conf;
        }

        @Override public String get(String key) {
            return conf.get(key);
        }
    }
    
    protected void assertCatalogLibraryUrl(CatalogItem<?,?> item, String expectedUrl) {
        CatalogBundle library = Iterables.getOnlyElement(item.getLibraries());
        assertEquals(library.getUrl(), expectedUrl);
    }
}
