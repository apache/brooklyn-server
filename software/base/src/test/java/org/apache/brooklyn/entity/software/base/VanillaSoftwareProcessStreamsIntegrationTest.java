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
package org.apache.brooklyn.entity.software.base;

import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.util.text.Identifiers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class VanillaSoftwareProcessStreamsIntegrationTest extends AbstractSoftwareProcessStreamsTest {
    private Location localhost;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        localhost = app.getManagementContext().getLocationRegistry().getLocationManaged("localhost");
    }

    // TODO Tests leave a lot of garbage in /tmp - should clean up after themselves.
    @Test(groups = {"Integration"})
    @Override
    public void testGetsStreams() {
        // Needs the installUniqueLabel so that, if run multiple times on same machine, there won't
        // be a "BROOKLYN" marker already in the install dir (which would cause install to be 
        // skipped).
        Map<String, String> cmds = getCommands();
        VanillaSoftwareProcess entity = app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.INSTALL_UNIQUE_LABEL, Identifiers.makeRandomId(8))
                .configure(VanillaSoftwareProcess.PRE_INSTALL_COMMAND, "echo " + cmds.get("pre-install-command"))
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "echo " + cmds.get("ssh: installing.*"))
                .configure(VanillaSoftwareProcess.POST_INSTALL_COMMAND, "echo " + cmds.get("post-install-command"))
                .configure(VanillaSoftwareProcess.CUSTOMIZE_COMMAND, "echo " + cmds.get("ssh: customizing.*"))
                .configure(VanillaSoftwareProcess.PRE_LAUNCH_COMMAND, "echo " + cmds.get("pre-launch-command"))
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "echo " + cmds.get("ssh: launching.*"))
                .configure(VanillaSoftwareProcess.POST_LAUNCH_COMMAND, "echo " + cmds.get("post-launch-command"))
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "true"));
        app.start(ImmutableList.of(localhost));

        assertStreams(entity);
    }

    @Override
    protected Map<String, String> getCommands() {
        return ImmutableMap.<String, String>builder()
                .put("pre-install-command", "myPreInstall")
                .put("ssh: installing.*", "myInstall")
                .put("post-install-command", "myPostInstall")
                .put("ssh: customizing.*", "myCustomizing")
                .put("pre-launch-command", "myPreLaunch")
                .put("ssh: launching.*", "myLaunch")
                .put("post-launch-command", "myPostLaunch")
                .build();
    }
}
