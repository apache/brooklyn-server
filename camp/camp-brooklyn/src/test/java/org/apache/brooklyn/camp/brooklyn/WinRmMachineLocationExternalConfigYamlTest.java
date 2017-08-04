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

import com.google.common.base.Joiner;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test
public class WinRmMachineLocationExternalConfigYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(WinRmMachineLocationExternalConfigYamlTest.class);

    @Override
    protected LocalManagementContext newTestManagementContext() {
        BrooklynProperties props = BrooklynProperties.Factory.newEmpty();
        props.put("brooklyn.external.inPlaceSupplier1", "org.apache.brooklyn.core.config.external.InPlaceExternalConfigSupplier");
        props.put("brooklyn.external.inPlaceSupplier1.byonPassword", "passw0rd");
        props.put("brooklyn.external.inPlaceSupplier1.byonUser", "admin");
        props.put("brooklyn.external.inPlaceSupplier1.ip", "127.0.0.1");

        return LocalManagementContextForTests.builder(true)
                .useProperties(props)
                .enableOsgiReusable()
                .build();
    }

    @Test()
    public void testWindowsMachinesExternalProvider() throws Exception {
        RecordingWinRmTool.constructorProps.clear();
        final String yaml = Joiner.on("\n").join("location:",
                "  byon:",
                "    hosts:",
                "    - winrm: $brooklyn:external(\"inPlaceSupplier1\", \"ip\")",
                "      user: $brooklyn:external(\"inPlaceSupplier1\", \"byonUser\")",
                "      brooklyn.winrm.config.winrmToolClass: org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool",
                "      password: $brooklyn:external(\"inPlaceSupplier1\", \"byonPassword\")",
                "      osFamily: windows",
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.VanillaWindowsProcess",
                "  brooklyn.config:",
                "    launch.command: echo launch",
                "    checkRunning.command: echo running");

        BasicApplication app = (BasicApplication) createAndStartApplication(yaml);
        waitForApplicationTasks(app);
        assertEquals(RecordingWinRmTool.constructorProps.get(0).get("host"), "127.0.0.1");
        assertEquals(RecordingWinRmTool.constructorProps.get(0).get(WinRmMachineLocation.USER.getName()), "admin");
        assertEquals(RecordingWinRmTool.constructorProps.get(0).get(WinRmMachineLocation.PASSWORD.getName()), "passw0rd");
    }

    @Test()
    public void testWindowsMachinesNoExternalProvider() throws Exception {
        RecordingWinRmTool.constructorProps.clear();
        final String yaml = Joiner.on("\n").join("location:",
                "  byon:",
                "    hosts:",
                "    - winrm: 127.0.0.1",
                "      user: $brooklyn:external(\"inPlaceSupplier1\", \"byonUserEmpty\")",
                "      brooklyn.winrm.config.winrmToolClass: org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool",
                "      password: $brooklyn:external(\"inPlaceSupplier1\", \"byonPasswordddd\")",
                "      osFamily: windows",
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.VanillaWindowsProcess",
                "  brooklyn.config:",
                "    launch.command: echo launch",
                "    checkRunning.command: echo running");

        BasicApplication app = (BasicApplication) createAndStartApplication(yaml);
        waitForApplicationTasks(app);
        assertNull(RecordingWinRmTool.constructorProps.get(0).get(WinRmMachineLocation.USER.getName()));
        assertNull(RecordingWinRmTool.constructorProps.get(0).get(WinRmMachineLocation.PASSWORD.getName()));
    }

    @Test()
    public void testWindowsMachinesNoExternalIPProvider() throws Exception {
        RecordingWinRmTool.constructorProps.clear();
        final String yaml = Joiner.on("\n").join("location:",
                "  byon:",
                "    hosts:",
                "    - winrm: $brooklyn:external(\"inPlaceSupplier1\", \"ipEmpty\")",
                "      user: $brooklyn:external(\"inPlaceSupplier1\", \"byonUserEmpty\")",
                "      brooklyn.winrm.config.winrmToolClass: org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool",
                "      password: $brooklyn:external(\"inPlaceSupplier1\", \"byonPasswordddd\")",
                "      osFamily: windows",
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.VanillaWindowsProcess",
                "  brooklyn.config:",
                "    launch.command: echo launch",
                "    checkRunning.command: echo running");

        try {
            BasicApplication app = (BasicApplication) createAndStartApplication(yaml);
            waitForApplicationTasks(app);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Must specify exactly one of 'ssh' or 'winrm' for machine");
        }
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
