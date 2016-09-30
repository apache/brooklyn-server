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
package org.apache.brooklyn.camp.brooklyn;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;

import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.location.jclouds.ComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.JcloudsRebindStubTest;
import org.testng.annotations.AfterMethod;

/**
 * Implementation notes. This relies on the test {@link JcloudsRebindStubTest#testRebind()}.
 * It changes the setup for the test in the following ways:
 * <ul>
 *   <li>Location is defined in YAML, and refers to the external config for the identity/credential.
 *   <li>When creating management context, it also creates {@link BrooklynCampPlatformLauncherNoServer}.
 *   <li>It uses {@link JcloudsRebindStubYamlTest#ByonComputeServiceStaticRef} to allow
 *       the test's {@link ComputeServiceRegistry} to be wired up via YAML.
 * </ul>
 * 
 * See {@link JcloudsRebindStubTest} for explanation why this is "Live" - it will not create VMs,
 * but does retrieve list of images etc.
 */
public abstract class JcloudsRebindStubYamlTest extends JcloudsRebindStubTest {

    protected BrooklynCampPlatformLauncherNoServer origLauncher;
    protected BrooklynCampPlatformLauncherNoServer newLauncher;

    @Override
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            ByonComputeServiceStaticRef.clearInstance();
            if (origLauncher != null) origLauncher.stopServers();
            if (newLauncher != null) newLauncher.stopServers();
        }
    }
    
    @Override
    protected LocalManagementContext createOrigManagementContext() {
        origLauncher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return JcloudsRebindStubYamlTest.super.createOrigManagementContext();
            }
        };
        origLauncher.launch();
        LocalManagementContext mgmt = (LocalManagementContext) origLauncher.getBrooklynMgmt();
        return mgmt;
    }

    @Override
    protected LocalManagementContext createNewManagementContext(final File mementoDir, final HighAvailabilityMode haMode) {
        newLauncher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return JcloudsRebindStubYamlTest.super.createNewManagementContext(mementoDir, haMode);
            }
        };
        newLauncher.launch();
        return (LocalManagementContext) newLauncher.getBrooklynMgmt();
    }
    
    public static class ByonComputeServiceStaticRef {
        private static volatile ComputeServiceRegistry instance;

        public ComputeServiceRegistry asComputeServiceRegistry() {
            return checkNotNull(instance, "instance");
        }
        static void setInstance(ComputeServiceRegistry val) {
            instance = val;
        }
        static void clearInstance() {
            instance = null;
        }
    }
}
