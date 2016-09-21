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
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.util.Map;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.camp.brooklyn.spi.creation.CampTypePlanTransformer;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.entity.machine.MachineEntity;
import org.apache.brooklyn.location.jclouds.ComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsRebindStubTest;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * This is primarily to test https://issues.apache.org/jira/browse/BROOKLYN-349
 * 
 * As per the other {@link JcloudsRebindStubTest} tests, it will connect to SoftLayer to retrieve
 * image details (so needs real credentials), but it will then stub out the VM creation.
 * 
 * It could do with some cleanup at some point:
 * <ul>
 *   <li>There is an NPE at AddMachineMetrics$2.apply(AddMachineMetrics.java:111), despite having set
 *       "metrics.usage.retrieve: false". Perhaps even with that set it will poll once before being disabled?!
 *   <li>When "stopping" the dummy machine, it fails when executing commands against the real SoftLayer:
 *       <pre>
 *       2016-09-21 17:49:22,776 ERROR Cannot retry after server error, command has exceeded retry limit 5: [method=org.jclouds.softlayer.features.VirtualGuestApi.public abstract org.jclouds.softlayer.domain.VirtualGuest org.jclouds.softlayer.features.VirtualGuestApi.getVirtualGuest(long)[123], request=GET https://api.softlayer.com/rest/v3/SoftLayer_Virtual_Guest/123/getObject?objectMask=id%3Bhostname%3Bdomain%3BfullyQualifiedDomainName%3BpowerState%3BmaxCpu%3BmaxMemory%3BstatusId%3BoperatingSystem.passwords%3BprimaryBackendIpAddress%3BprimaryIpAddress%3BactiveTransactionCount%3BblockDevices.diskImage%3Bdatacenter%3BtagReferences%3BprivateNetworkOnlyFlag%3BsshKeys HTTP/1.1]
 *       </pre>
 *       Presumably we need to stub out that call as well somehow!
 * </ul>
 */
@Test(groups={"Live", "Live-sanity"})
public class JcloudsRebindWithYamlDslTest extends JcloudsRebindStubTest {

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
    protected LocalManagementContext createOrigManagementContext() {
        origLauncher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return JcloudsRebindWithYamlDslTest.super.createOrigManagementContext();
            }
        };
        origLauncher.launch();
        LocalManagementContext mgmt = (LocalManagementContext) origLauncher.getBrooklynMgmt();
        return mgmt;
    }

    @Override
    protected LocalManagementContext createNewManagementContext(final File mementoDir) {
        newLauncher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return JcloudsRebindWithYamlDslTest.super.createNewManagementContext(mementoDir);
            }
        };
        newLauncher.launch();
        return (LocalManagementContext) newLauncher.getBrooklynMgmt();
    }
    
    @Override
    protected JcloudsLocation newJcloudsLocation(ComputeServiceRegistry computeServiceRegistry) throws Exception {
        ByonComputeServiceStaticRef.setInstance(computeServiceRegistry);
        
        String symbolicName = "my.catalog.app.id.load";
        String catalogYaml = Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: \"0.1.2\"",
            "  itemType: entity",
            "  item:",
            "    brooklyn.parameters:",
            "    - name: password",
            "      default: myYamlPassword",
            "    type: "+ MachineEntity.class.getName());
        mgmt().getCatalog().addItems(catalogYaml, true);

        String yaml = Joiner.on("\n").join(
                "location:",
                "  jclouds:softlayer:",
                "    jclouds.computeServiceRegistry:",
                "      $brooklyn:object:",
                "        type: "+ByonComputeServiceStaticRef.class.getName(),
                "    "+SshMachineLocation.SSH_TOOL_CLASS.getName() + ": " + RecordingSshTool.class.getName(),
                "    waitForSshable: false",
                "    useJcloudsSshInit: false",
                "services:\n"+
                "- type: "+symbolicName,
                "  brooklyn.config:",
                "    onbox.base.dir.skipResolution: true",
                "    sshMonitoring.enabled: false",
                "    metrics.usage.retrieve: false",
                "    provisioning.properties:",
                "      password: $brooklyn:config(\"password\")");
        
        EntitySpec<?> spec = 
                mgmt().getTypeRegistry().createSpecFromPlan(CampTypePlanTransformer.FORMAT, yaml, RegisteredTypeLoadingContexts.spec(Application.class), EntitySpec.class);
        final Entity app = mgmt().getEntityManager().createEntity(spec);
        final MachineEntity entity = (MachineEntity) Iterables.getOnlyElement(app.getChildren());
        app.invoke(Startable.START, ImmutableMap.<String, Object>of()).get();
        
        // Execute ssh (with RecordingSshTool), and confirm was given resolved password
        entity.execCommand("mycmd");
        Map<?, ?> constructorProps = RecordingSshTool.getLastConstructorProps();
        ExecCmd execCmd = RecordingSshTool.getLastExecCmd();
        assertEquals(constructorProps.get("password"), "myYamlPassword", "constructorProps: "+constructorProps+"; execProps: "+execCmd.props);
        
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
