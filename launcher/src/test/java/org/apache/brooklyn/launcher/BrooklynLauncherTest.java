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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URI;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestApplicationImpl;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.launcher.common.BrooklynPropertiesFactoryHelperTest;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.test.support.FlakyRetryAnalyser;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.io.FileUtil;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Strings;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;

public class BrooklynLauncherTest {
    
    private BrooklynLauncher launcher;

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (launcher != null) launcher.terminate();
        launcher = null;
    }

    // Integration because takes a few seconds to start web-console
    @Test(groups="Integration")
    public void testStartsWebServerOnExpectectedPort() throws Exception {
        launcher = newLauncherForTests(true)
                .restServerPort("10000+")
                .installSecurityFilter(false)
                .start();
        
        String webServerUrlStr = launcher.getServerDetails().getWebServerUrl();
        URI webServerUri = new URI(webServerUrlStr);
        
        assertEquals(launcher.getApplications(), ImmutableList.of());
        assertTrue(webServerUri.getPort() >= 10000 && webServerUri.getPort() < 10100, "port="+webServerUri.getPort()+"; uri="+webServerUri);
        HttpAsserts.assertUrlReachable(webServerUrlStr);
    }
    
    // Integration because takes a few seconds to start web-console
    @Test(groups="Integration")
    public void testWebServerTempDirRespectsDataDirConfig() throws Exception {
        String dataDirName = ".brooklyn-foo"+Strings.makeRandomId(4);
        String dataDir = "~/"+dataDirName;

        launcher = newLauncherForTests(true)
                .brooklynProperties(BrooklynServerConfig.MGMT_BASE_DIR, dataDir)
                .start();
        
        ManagementContext managementContext = launcher.getServerDetails().getManagementContext();
        String expectedTempDir = Os.mergePaths(Os.home(), dataDirName, "planes", managementContext.getManagementNodeId(), "jetty");
        
        File webappTempDir = launcher.getServerDetails().getWebServer().getWebappTempDir();
        assertEquals(webappTempDir.getAbsolutePath(), expectedTempDir);
    }
    
    // Integration because takes a few seconds to start web-console
    @Test(groups="Integration")
    public void testStartsWebServerWithoutAuthentication() throws Exception {
        launcher = newLauncherForTests(true)
                .start();
        String uri = launcher.getServerDetails().getWebServerUrl();
        
        HttpToolResponse response = HttpTool.execAndConsume(HttpTool.httpClientBuilder().build(), new HttpGet(uri));
        assertEquals(response.getResponseCode(), 200);
    }
    
    // Integration because takes a few seconds to start web-console
    @Test(groups="Integration")
    public void testStartsWebServerWithCredentials() throws Exception {
        launcher = newLauncherForTests(true)
                .restServerPort("10000+")
                .brooklynProperties(BrooklynWebConfig.USERS, "myname")
                .brooklynProperties(BrooklynWebConfig.PASSWORD_FOR_USER("myname"), "mypassword")
                .start();
        String uri = launcher.getServerDetails().getWebServerUrl();
        
        HttpToolResponse response = HttpTool.execAndConsume(HttpTool.httpClientBuilder().build(), new HttpGet(uri));
        assertEquals(response.getResponseCode(), 401);
        
        HttpToolResponse response2 = HttpTool.execAndConsume(
                HttpTool.httpClientBuilder()
                        .uri(uri)
                        .credentials(new UsernamePasswordCredentials("myname", "mypassword"))
                        .build(), 
                new HttpGet(uri));
        assertEquals(response2.getResponseCode(), 200);
    }
    
    @Test
    public void testCanDisableWebServerStartup() throws Exception {
        launcher = newLauncherForTests(true)
                .restServer(false)
                .start();
        
        assertNull(launcher.getServerDetails().getWebServer());
        assertNull(launcher.getServerDetails().getWebServerUrl());
        Assert.assertTrue( ((ManagementContextInternal)launcher.getServerDetails().getManagementContext()).errors().isEmpty() );
    }
    
    @Test
    public void testStartsAppInstance() throws Exception {
        launcher = newLauncherForTests(true)
                .restServer(false)
                .application(EntitySpec.create(TestApplicationImpl.class))
                .start();
        
        assertOnlyApp(launcher, TestApplication.class);
    }
    
    @Test
    public void testStartsAppFromSpec() throws Exception {
        launcher = newLauncherForTests(true)
                .restServer(false)
                .application(EntitySpec.create(TestApplication.class))
                .start();
        
        assertOnlyApp(launcher, TestApplication.class);
    }
    
    @Test
    public void testStartsAppFromYAML() throws Exception {
        String yaml = "name: example-app\n" +
                "services:\n" +
                "- serviceType: org.apache.brooklyn.core.test.entity.TestEntity\n" +
                "  name: test-app";
        launcher = newLauncherForTests(true)
                .restServer(false)
                .application(yaml)
                .start();

        assertEquals(launcher.getApplications().size(), 1, "apps="+launcher.getApplications());
        Application app = Iterables.getOnlyElement(launcher.getApplications());
        assertEquals(app.getChildren().size(), 1, "children=" + app.getChildren());
        assertTrue(Iterables.getOnlyElement(app.getChildren()) instanceof TestEntity);
    }
    
    @Test  // may take 2s initializing location if running this test case alone, but noise if running suite 
    public void testStartsAppInSuppliedLocations() throws Exception {
        launcher = newLauncherForTests(true)
                .restServer(false)
                .location("localhost")
                .application(EntitySpec.create(TestApplication.class))
                .start();
        
        Application app = Iterables.find(launcher.getApplications(), Predicates.instanceOf(TestApplication.class));
        assertOnlyLocation(app, LocalhostMachineProvisioningLocation.class);
    }
    
    @Test
    public void testUsesSuppliedManagementContext() throws Exception {
        LocalManagementContext myManagementContext = LocalManagementContextForTests.newInstance();
        launcher = newLauncherForTests(false)
                .restServer(false)
                .managementContext(myManagementContext)
                .start();
        
        assertSame(launcher.getServerDetails().getManagementContext(), myManagementContext);
    }
    
    @Test
    public void testUsesSuppliedBrooklynProperties() throws Exception {
        BrooklynProperties props = LocalManagementContextForTests.builder(true).buildProperties();
        props.put("mykey", "myval");
        launcher = newLauncherForTests(false)
                .restServer(false)
                .brooklynProperties(props)
                .start();
        
        assertEquals(launcher.getServerDetails().getManagementContext().getConfig().getFirst("mykey"), "myval");
    }

    @Test
    public void testUsesSupplementaryBrooklynProperties() throws Exception {
        launcher = newLauncherForTests(true)
                .restServer(false)
                .brooklynProperties("mykey", "myval")
                .start();
        
        assertEquals(launcher.getServerDetails().getManagementContext().getConfig().getFirst("mykey"), "myval");
    }
    
    @Test
    public void testReloadBrooklynPropertiesRestoresProgrammaticProperties() throws Exception {
        launcher = newLauncherForTests(true)
                .restServer(false)
                .brooklynProperties("mykey", "myval")
                .start();
        LocalManagementContext managementContext = (LocalManagementContext)launcher.getServerDetails().getManagementContext();
        assertEquals(managementContext.getConfig().getFirst("mykey"), "myval");
        managementContext.getBrooklynProperties().put("mykey", "newval");
        assertEquals(managementContext.getConfig().getFirst("mykey"), "newval");
        managementContext.reloadBrooklynProperties();
        assertEquals(managementContext.getConfig().getFirst("mykey"), "myval");
    }
    
    @Test
    public void testReloadBrooklynPropertiesFromFile() throws Exception {
        File globalPropertiesFile = File.createTempFile("local-brooklyn-properties-test", ".properties");
        FileUtil.setFilePermissionsTo600(globalPropertiesFile);
        try {
            String property = "mykey=myval";
            Files.append(BrooklynPropertiesFactoryHelperTest.getMinimalLauncherPropertiesString()+property, globalPropertiesFile, Charsets.UTF_8);
            launcher = newLauncherForTests(false)
                    .restServer(false)
                    .globalBrooklynPropertiesFile(globalPropertiesFile.getAbsolutePath())
                    .start();
            LocalManagementContext managementContext = (LocalManagementContext)launcher.getServerDetails().getManagementContext();
            assertEquals(managementContext.getConfig().getFirst("mykey"), "myval");
            property = "mykey=newval";
            Files.write(BrooklynPropertiesFactoryHelperTest.getMinimalLauncherPropertiesString()+property, globalPropertiesFile, Charsets.UTF_8);
            managementContext.reloadBrooklynProperties();
            assertEquals(managementContext.getConfig().getFirst("mykey"), "newval");
        } finally {
            globalPropertiesFile.delete();
        }
    }


    private BrooklynLauncher newLauncherForTests(boolean minimal) {
        Preconditions.checkArgument(launcher == null, "can only be used if no launcher yet");
        BrooklynLauncher launcher = BrooklynLauncher.newInstance();
        if (minimal)
            launcher.brooklynProperties(LocalManagementContextForTests.builder(true).buildProperties());
        return launcher;
    }

    private void assertOnlyApp(BrooklynLauncher launcher, Class<? extends Application> expectedType) {
        assertEquals(launcher.getApplications().size(), 1, "apps="+launcher.getApplications());
        assertNotNull(Iterables.find(launcher.getApplications(), Predicates.instanceOf(TestApplication.class), null), "apps="+launcher.getApplications());
    }
    
    private void assertOnlyLocation(Application app, Class<? extends Location> expectedType) {
        assertEquals(app.getLocations().size(), 1, "locs="+app.getLocations());
        assertNotNull(Iterables.find(app.getLocations(), Predicates.instanceOf(LocalhostMachineProvisioningLocation.class), null), "locs="+app.getLocations());
    }
}
