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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.config.external.AbstractExternalConfigSupplier;
import org.apache.brooklyn.core.mgmt.internal.ExternalConfigSupplierRegistry;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.http.TestHttpRecordingRequestInterceptor;
import org.apache.brooklyn.test.http.TestHttpRequestHandler;
import org.apache.brooklyn.test.http.TestHttpServer;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.net.UrlEscapers;

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
    
    // TODO See https://issues.apache.org/jira/browse/BROOKLYN-421
    //      Need to somehow escape the username and password (or include them explicitly as a 
    //      "Authorization" header instead of embedding them in the URI).
    @Test(groups="Broken")
    public void testLibraryUrlUsingExternalizedConfigForCredentials() throws Exception {
        String username = "myuser@mydomain.com";
        String password = "Myp4ss@?/:!";
        
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
                "    - $brooklyn:external(\"myprovider\", \"username\")",
                "    - $brooklyn:external(\"myprovider\", \"password\")",
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
        String escapedUsername = UrlEscapers.urlFragmentEscaper().escape(username);
        String escapedPassword = UrlEscapers.urlFragmentEscaper().escape(password);
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
