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
package org.apache.brooklyn.rest.util.json;

import java.io.NotSerializableException;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.rest.BrooklynRestApiLauncher;
import org.apache.brooklyn.rest.util.json.BrooklynJacksonSerializerTest.SelfRefNonSerializableClass;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

public class BrooklynJacksonSerializerIntegrationTest extends BrooklynAppUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(BrooklynJacksonSerializerIntegrationTest.class);
    
    private Server server;
    private HttpClient client;
    URI entityUrl;
    URI configUri;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        super.setUp();
        server = BrooklynRestApiLauncher.launcher().managementContext(mgmt).start();
        client = HttpTool.httpClientBuilder().build();

        String serverAddress = "http://localhost:"+((NetworkConnector)server.getConnectors()[0]).getLocalPort();
        String appUrl = serverAddress + "/v1/applications/" + app.getId();
        entityUrl = URI.create(appUrl + "/entities/" + app.getId());
        configUri = new URIBuilder(entityUrl + "/config/" + TestEntity.CONF_OBJECT.getName())
                .addParameter("raw", "true")
                .build();

    }
    
    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        try {
            if (server != null) server.stop();
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.warn("failed to stop server: "+e);
        }
        super.tearDown();
    }
    
    // Ensure TEXT_PLAIN just returns toString for ManagementContext instance.
    // Strangely, testWithLauncherSerializingListsContainingEntitiesAndOtherComplexStuff ended up in the 
    // EntityConfigResource.getPlain code, throwing a ClassCastException.
    // 
    // TODO This tests the fix for that ClassCastException, but does not explain why 
    // testWithLauncherSerializingListsContainingEntitiesAndOtherComplexStuff was calling it.
    @Test(groups="Integration") //because of time
    public void testWithAcceptsPlainText() throws Exception {
        // assert config here is just mgmt.toString()
        setConfig(mgmt);
        String content = getConfigValueAsText();
        log.info("CONFIG MGMT is:\n"+content);
        Assert.assertEquals(content, mgmt.toString(), "content="+content);
    }

    @Test(groups="Integration") //because of time
    public void testWithMgmt() throws Exception {
        setConfig(mgmt);
        Map<?, ?> values = getConfigValueAsJson();
        Assert.assertEquals(values, ImmutableMap.of("type", LocalManagementContextForTests.class.getCanonicalName()), "values="+values);

        // assert normal API returns the same, containing links
        values = getRestValueAsJson(entityUrl);
        Assert.assertTrue(values.size()>=3, "Map is too small: "+values);
        Assert.assertTrue(values.size()<=6, "Map is too big: "+values);
        Assert.assertEquals(values.get("type"), TestApplication.class.getCanonicalName(), "values="+values);
        Assert.assertNotNull(values.get("links"), "Map should have contained links: values="+values);

    }

    @Test(groups="Integration") //because of time
    public void testWithApp() throws Exception {
        // but config etc returns our nicely json serialized
        setConfig(app);
        Map<?, ?> values = getConfigValueAsJson();
        Assert.assertEquals(values, ImmutableMap.of("type", Entity.class.getCanonicalName(), "id", app.getId()), "values="+values);
    }

    @Test(groups="Integration") //because of time
    public void testWithCyclic() throws Exception {
        // and self-ref gives error + toString
        SelfRefNonSerializableClass angry = new SelfRefNonSerializableClass();
        setConfig(angry);
        Map<?, ?> values = getConfigValueAsJson();
        assertErrorObjectMatchingToString(values, angry);
        
    }

    // Broken. Serialization is failing because of "group" -> "threads" -> "group" cycle inside server (through the thread pool).
    // It's doing best effort though - still serializing what it was able to before giving up. This results in a huge string
    // which fails the assertions.
    @Test(groups={"Integration", "Broken"}) //because of time
    public void testWithServer() throws Exception {
        // as does Server
        setConfig(server);
        Map<?, ?> values = getConfigValueAsJson();
        // NOTE, if using the default visibility / object mapper, the getters of the object are invoked
        // resulting in an object which is huge, 7+MB -- and it wreaks havoc w eclipse console regex parsing!
        // (but with our custom VisibilityChecker server just gives us the nicer error!)
        String content = getRestValue(configUri, MediaType.APPLICATION_JSON);
        log.info("CONFIG is:\n"+content);
        values = new Gson().fromJson(content, Map.class);
        assertErrorObjectMatchingToString(values, server);
        Assert.assertTrue(content.contains(NotSerializableException.class.getCanonicalName()), "server should have contained things which are not serializable");
        Assert.assertTrue(content.length() < 1024, "content should not have been very long; instead was: "+content.length());
    }

    private void assertErrorObjectMatchingToString(Map<?, ?> content, Object expected) {
        Assert.assertEquals(content.get("toString"), expected.toString());
    }

    private String get(HttpClient client, URI uri, Map<String, String> headers) {
        return HttpTool.httpGet(client, uri, headers).getContentAsString();
    }

    protected String getConfigValueAsText() {
        return getRestValueAsText(configUri);
    }

    protected String getRestValueAsText(URI url) {
        return getRestValue(url, MediaType.TEXT_PLAIN);
    }

    protected Map<?, ?> getConfigValueAsJson() {
        return getRestValueAsJson(configUri);
    }

    protected Map<?, ?> getRestValueAsJson(URI url) {
        String content = getRestValue(url, MediaType.APPLICATION_JSON);
        log.info("CONFIG is:\n"+content);
        return new Gson().fromJson(content, Map.class);
    }

    protected String getRestValue(URI url, String contentType) {
        return get(client, url, ImmutableMap.of("Accept", contentType));
    }
        
    protected void setConfig(Object value) {
        app.config().set(TestEntity.CONF_OBJECT, value);
    }

}
