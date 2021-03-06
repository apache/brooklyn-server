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

import static org.testng.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


/**
 * Tests that web app starts with given WAR(s) - legacy startup, originally written for the single non-angular brooklyn.war,
 * but tested here that it works with other WARs in case that is used anywhere.
 */
public class WebAppRunnerTest {

    public static final Logger log = LoggerFactory.getLogger(WebAppRunnerTest.class);
            
    List<LocalManagementContext> managementContexts = Lists.newCopyOnWriteArrayList();
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        for (LocalManagementContext managementContext : managementContexts) {
            Entities.destroyAll(managementContext);
        }
        managementContexts.clear();
    }
    
    LocalManagementContext newManagementContext(BrooklynProperties brooklynProperties) {
        LocalManagementContext result = new LocalManagementContext(brooklynProperties);
        managementContexts.add(result);
        return result;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    BrooklynWebServer createWebServer(Map properties) {
        Map bigProps = MutableMap.copyOf(properties);
        Map attributes = MutableMap.copyOf( (Map) bigProps.get("attributes") );
        bigProps.put("attributes", attributes);

        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.putAll(bigProps);
        brooklynProperties.put("brooklyn.webconsole.security.https.required","false");
        return new BrooklynWebServer(bigProps, newManagementContext(brooklynProperties))
                .skipSecurity();
    }
    
    @Test
    public void testStartWar1() throws Exception {
        BrooklynWebServer server = createWebServer(MutableMap.of("port", "8091+"));
        assertNotNull(server);
        
        try {
            server.start();
            assertRootPageAvailableAt("http://localhost:"+server.getActualPort()+"/");
        } finally {
            server.stop();
        }
    }

    public static void assertRootPageAvailableAt(String url) {
        HttpAsserts.assertContentEventuallyContainsText(url, "Brooklyn REST API only");
    }

    @Test
    public void testStartSecondaryWar() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), "/hello-world.war");

        BrooklynWebServer server = createWebServer(
            MutableMap.of("port", "8091+", "war", null, "wars", MutableMap.of("hello", "hello-world.war")) );
        assertNotNull(server);
        
        try {
            server.start();

            assertRootPageAvailableAt("http://localhost:"+server.getActualPort()+"/");
            HttpAsserts.assertContentEventuallyContainsText("http://localhost:"+server.getActualPort()+"/hello",
                "This is the home page for a sample application");

        } finally {
            server.stop();
        }
    }

    @Test
    public void testStartSecondaryWarAfter() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), "/hello-world.war");

        BrooklynWebServer server = createWebServer(MutableMap.of("port", "8091+", "war", null));
        assertNotNull(server);
        
        try {
            server.start();
            server.deploy("/hello", "hello-world.war");

            assertRootPageAvailableAt("http://localhost:"+server.getActualPort()+"/");
            HttpAsserts.assertContentEventuallyContainsText("http://localhost:"+server.getActualPort()+"/hello",
                "This is the home page for a sample application");

        } finally {
            server.stop();
        }
    }

    @Test
    public void testStartWithLauncher() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), "/hello-world.war");

        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .brooklynProperties(BrooklynProperties.Factory.newEmpty())
                .brooklynProperties("brooklyn.webconsole.security.provider","org.apache.brooklyn.rest.security.provider.AnyoneSecurityProvider")
                .webapp("/hello", "hello-world.war")
                .start();
        BrooklynServerDetails details = launcher.getServerDetails();
        
        try {
            details.getWebServer().deploy("/hello2", "hello-world.war");

            assertRootPageAvailableAt(details.getWebServerUrl());
            HttpAsserts.assertContentEventuallyContainsText(details.getWebServerUrl()+"hello", "This is the home page for a sample application");
            HttpAsserts.assertContentEventuallyContainsText(details.getWebServerUrl()+"hello2", "This is the home page for a sample application");
            HttpAsserts.assertHttpStatusCodeEventuallyEquals(details.getWebServerUrl()+"hello0", 404);

        } finally {
            details.getWebServer().stop();
            ((ManagementContextInternal)details.getManagementContext()).terminate();
        }
    }
    
}
