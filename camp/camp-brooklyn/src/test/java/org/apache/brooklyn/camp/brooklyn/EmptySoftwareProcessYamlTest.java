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

import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

@Test
public class EmptySoftwareProcessYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(EnrichersYamlTest.class);

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshTool.clear();
    }
    
    @Test
    public void testProvisioningProperties() throws Exception {
        Entity app = createAndStartApplication(
            "location:",
            "  localhost:",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "    myLocConfig: myval",
            "services:",
            "- type: "+EmptySoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    provisioning.properties:",
            "      minRam: 16384");
        waitForApplicationTasks(app);

        log.info("App started:");
        Dumper.dumpInfo(app);
        
        EmptySoftwareProcess entity = (EmptySoftwareProcess) app.getChildren().iterator().next();
        Map<String, Object> pp = entity.getConfig(EmptySoftwareProcess.PROVISIONING_PROPERTIES);
        assertEquals(pp.get("minRam"), 16384);
        
        MachineLocation machine = Locations.findUniqueMachineLocation(entity.getLocations()).get();
        assertEquals(machine.config().get(ConfigKeys.newConfigKey(Object.class, "myLocConfig")), "myval");
        assertEquals(machine.config().get(ConfigKeys.newConfigKey(Object.class, "minRam")), 16384);
    }

    // for https://github.com/brooklyncentral/brooklyn/issues/1377
    @Test
    public void testWithAppAndEntityLocations() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: "+EmptySoftwareProcess.class.getName(),
                "  location:",
                "    localhost:(name=localhost on entity):",
                "      sshToolClass: "+RecordingSshTool.class.getName(),
                "location: byon:(hosts=\"127.0.0.1\", name=loopback on app)");
        waitForApplicationTasks(app);
        Dumper.dumpInfo(app);
        
        Location appLocation = Iterables.getOnlyElement(app.getLocations());
        Assert.assertEquals(appLocation.getDisplayName(), "loopback on app");
        
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Assert.assertEquals(entity.getLocations().size(), 2);
        Location provisioningLoc = Iterables.get(entity.getLocations(), 0);
        Location machineLoc = Iterables.get(entity.getLocations(), 1);
        
        Assert.assertEquals(provisioningLoc.getDisplayName(), "localhost on entity");
        Assert.assertTrue(machineLoc instanceof SshMachineLocation, "wrong location: "+machineLoc);
        // TODO this, below, probably should be 'localhost on entity', see #1377
        Assert.assertEquals(machineLoc.getParent().getDisplayName(), "localhost on entity");
    }
    
    @Test
    public void testNoSshing() throws Exception {
        Entity app = createAndStartApplication(
                "location:",
                "  localhost:",
                "    sshToolClass: "+RecordingSshTool.class.getName(),
                "services:",
                "- type: "+EmptySoftwareProcess.class.getName(),
                "  brooklyn.config:",
                "    sshMonitoring.enabled: false",
                "    "+BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION.getName()+": true");
        waitForApplicationTasks(app);

        EmptySoftwareProcess entity = (EmptySoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEqualsContinually(entity, Attributes.SERVICE_UP, true);
        
        assertEquals(RecordingSshTool.getExecCmds(), ImmutableList.of());
    }
}
