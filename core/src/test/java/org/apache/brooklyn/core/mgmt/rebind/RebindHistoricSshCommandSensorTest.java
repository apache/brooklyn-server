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
package org.apache.brooklyn.core.mgmt.rebind;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class RebindHistoricSshCommandSensorTest extends AbstractRebindHistoricTest {
    
    private static final Logger log = LoggerFactory.getLogger(RebindHistoricSshCommandSensorTest.class);
    private static final String BLACKHOLE_IP = "240.0.0.1";
    
    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            RecordingSshTool.clear();
        }
    }
    /**
     * The persisted state was generated when SshCommandSensor used fields for 'command' etc, populating
     * them during init. Now it populates them lazily (thus handling deferred config suppliers).
     * 
     * It also used anonymous inner classes for the deferred suppliers.
     * 
     * Generated the persisted state using Brooklyn from Feb 2018, using the blueprint below:
     * <pre>
     * services:
     *   - type: org.apache.brooklyn.entity.stock.BasicApplication
     *     brooklyn.initializers:
     *       - type: org.apache.brooklyn.core.sensor.ssh.SshCommandSensor
     *          brooklyn.config:
     *            name: myconf
     *            targetType: String
     *            period: 100ms
     *            command: "echo 'myval'"
     *            shell.env:
     *              MY_ENV: myEnvVal
     *            executionDir: '/path/to/myexecutiondir'
     * <pre>
    */   
    @Test
    public void testSshFeed_2018_02() throws Exception {
        addMemento(BrooklynObjectType.ENTITY, "ssh-command-sensor-entity", "dnlz7hpbdg");
        addMemento(BrooklynObjectType.FEED, "ssh-command-sensor-feed", "a9ekg3cnu0");
        rebind();
        
        EntityInternal entity = (EntityInternal) mgmt().getEntityManager().getEntity("dnlz7hpbdg");
        entity.feeds().getFeeds();
        
        SshMachineLocation recordingLocalhostMachine = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", BLACKHOLE_IP)
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()));
        entity.addLocations(ImmutableList.of(recordingLocalhostMachine));
        // odd, if this _class_ is run it waits the full period before running the feed;
        // but if this _test_ is run on its own it runs immediately.
        // reduced period to 20ms in persisted state and now it always runs immediately.
        ExecCmd cmd = Asserts.succeedsEventually(() -> RecordingSshTool.getLastExecCmd());
        assertTrue(cmd.commands.toString().contains("echo 'myval'"), "cmds="+cmd.commands);
        assertEquals(cmd.env.get("MY_ENV"), "myEnvVal", "env="+cmd.env);
        assertTrue(cmd.commands.toString().contains("/path/to/myexecutiondir"), "cmds="+cmd.commands);
    }
    
    // This test is similar to testSshFeed_2017_01, except the persisted state file has been 
    // hand-crafted to remove the bundle prefixes for "org.apache.brooklyn.*" bundles.
    @Test
    public void testFoo_2017_01_withoutBundlePrefixes() throws Exception {
        addMemento(BrooklynObjectType.FEED, "ssh-feed-no-bundle-prefixes", "zv7t8bim62");
        rebind();
    }
}
