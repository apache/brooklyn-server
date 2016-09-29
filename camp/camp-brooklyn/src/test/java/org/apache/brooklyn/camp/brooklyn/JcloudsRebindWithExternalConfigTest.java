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
import java.util.Map;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.core.config.external.InPlaceExternalConfigSupplier;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.location.jclouds.ComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsPropertiesFromBrooklynProperties;
import org.apache.brooklyn.location.jclouds.JcloudsRebindStubTest;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * Implementation notes. This relies on the test {@link JcloudsRebindStubTest#testRebind()}.
 * It changes the setup for the test in the following ways:
 * <ul>
 *   <li>Brooklyn properties defines external config
 *   <li>Location is defined in YAML, and refers to the external config for the identity/credential.
 *   <li>When creating management context, it also creates {@link BrooklynCampPlatformLauncherNoServer}.
 *   <li>It uses {@link JcloudsRebindWithExternalConfigTest#ByonComputeServiceStaticRef} to allow
 *       the test's {@link ComputeServiceRegistry} to be wired up via YAML.
 * </ul>
 * 
 * See {@link JcloudsRebindStubTest} for explanation why this is "Live" - it will not create VMs,
 * but does retrieve list of images etc.
 */
@Test(groups={"Live", "Live-sanity"})
public class JcloudsRebindWithExternalConfigTest extends JcloudsRebindStubTest {

    private BrooklynCampPlatformLauncherNoServer origLauncher;
    private BrooklynCampPlatformLauncherNoServer newLauncher;

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
    protected BrooklynProperties createBrooklynProperties() {
        BrooklynProperties result = super.createBrooklynProperties();
        
        Map<Object,Object> jcloudsProps = MutableMap.<Object,Object>copyOf(new JcloudsPropertiesFromBrooklynProperties().getJcloudsProperties(PROVIDER, null, "testname", result.asMapWithStringKeys()));
        String identity = checkNotNull((String)jcloudsProps.get("identity"), "identity");
        String credential = checkNotNull((String)jcloudsProps.get("credential"), "credential");
        
        result.put("brooklyn.external.creds", InPlaceExternalConfigSupplier.class.getName());
        result.put("brooklyn.external.creds.test-identity", identity);
        result.put("brooklyn.external.creds.test-credential", credential);
        
        return result;
    }
    
    @Override
    protected LocalManagementContext createOrigManagementContext() {
        origLauncher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return JcloudsRebindWithExternalConfigTest.super.createOrigManagementContext();
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
                return JcloudsRebindWithExternalConfigTest.super.createNewManagementContext(mementoDir, haMode);
            }
        };
        newLauncher.launch();
        return (LocalManagementContext) newLauncher.getBrooklynMgmt();
    }
    
    @Override
    protected JcloudsLocation newJcloudsLocation(ComputeServiceRegistry computeServiceRegistry) throws Exception {
        ByonComputeServiceStaticRef.setInstance(computeServiceRegistry);
        
        String yaml = Joiner.on("\n").join(
                "location:",
                "  jclouds:softlayer:",
                "    identity: $brooklyn:external(\"creds\", \"test-identity\")",
                "    credential: $brooklyn:external(\"creds\", \"test-credential\")",
                "    jclouds.computeServiceRegistry:",
                "      $brooklyn:object:",
                "        type: "+ByonComputeServiceStaticRef.class.getName(),
                "    waitForSshable: false",
                "    useJcloudsSshInit: false",
                "services:\n"+
                "- type: org.apache.brooklyn.entity.stock.BasicApplication");
        
        EntitySpec<?> spec = 
                mgmt().getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, yaml, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        final Entity app = mgmt().getEntityManager().createEntity(spec);
        app.invoke(Startable.START, ImmutableMap.<String, Object>of()).get();

        return (JcloudsLocation) Iterables.getOnlyElement(app.getLocations());
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
