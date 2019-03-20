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

import com.google.common.base.Preconditions;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.test.support.FlakyRetryAnalyser;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.net.Urls;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


// TODO Fix https://issues.apache.org/jira/browse/BROOKLYN-611
// until then test testErrorsCaughtByApiAndRestApiWorks is moved to its own class to avoid the initialisation
// error described in that issue
public class BrooklynLauncherErrorTest {
    
    private BrooklynLauncher launcher;

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (launcher != null) launcher.terminate();
        launcher = null;
    }

    @Test(retryAnalyzer = FlakyRetryAnalyser.class)  // takes a bit of time because starts webapp, but also tests rest api so useful
    public void testErrorsCaughtByApiAndRestApiWorks() throws Exception {
        launcher = newLauncherForTests(true)
                .catalogInitialization(new CatalogInitialization(null) {
                    @Override public void populateInitialCatalogOnly() {
                        throw new RuntimeException("deliberate-exception-for-testing");
                    }})
                .persistMode(PersistMode.DISABLED)
                .installSecurityFilter(false)
                .start();
        // 'deliberate-exception' error above should be thrown, then caught in this calling thread
        ManagementContext mgmt = launcher.getServerDetails().getManagementContext();
        Assert.assertFalse( ((ManagementContextInternal)mgmt).errors().isEmpty() );
        Assert.assertTrue( ((ManagementContextInternal)mgmt).errors().get(0).toString().contains("deliberate"), ""+((ManagementContextInternal)mgmt).errors() );
        HttpAsserts.assertContentMatches(
            Urls.mergePaths(launcher.getServerDetails().getWebServerUrl(), "v1/server/up"), 
            "true");
        HttpAsserts.assertContentMatches(
            Urls.mergePaths(launcher.getServerDetails().getWebServerUrl(), "v1/server/healthy"), 
            "false");
        // TODO test errors api?
    }

    private BrooklynLauncher newLauncherForTests(boolean minimal) {
        Preconditions.checkArgument(launcher == null, "can only be used if no launcher yet");
        BrooklynLauncher launcher = BrooklynLauncher.newInstance();
        if (minimal)
            launcher.brooklynProperties(LocalManagementContextForTests.builder(true).buildProperties());
        return launcher;
    }
}
