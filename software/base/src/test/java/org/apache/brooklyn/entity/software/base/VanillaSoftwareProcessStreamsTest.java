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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmdPredicates;
import org.apache.brooklyn.util.stream.Streams;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.brooklyn.util.core.internal.ssh.ExecCmdAsserts.*;

public class VanillaSoftwareProcessStreamsTest extends AbstractSoftwareProcessStreamsTest {

    private Location location;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        location = app.getManagementContext().getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .configure(FixedListMachineProvisioningLocation.MACHINE_SPECS, ImmutableList.<LocationSpec<? extends MachineLocation>>of(
                        LocationSpec.create(SshMachineLocation.class)
                                .configure("address", "1.2.3.4")
                                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()))));
        
        RecordingSshTool.clear();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshTool.clear();
    }

    @Test
    public void testMaskedValuesInEnvStream() {

        // Prepare expected environment variables, secret names are keys with values that should be masked in env stream
        Map<String, String> expectedEnv = new ImmutableMap.Builder<String, String>()
                .put("KEY1", "VAL1")
                .put("KEY2A", "v1=v2 secret=not_hidden_if_on_same_line\nsecret2=should_be_suppressed")
                .putAll(Sanitizer.DEFAULT_SENSITIVE_FIELDS_TOKENS.stream().collect(Collectors.toMap(item -> item, item -> item)))
                .build();

        // Create configuration
        EntitySpec<VanillaSoftwareProcess> entitySpec = EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "installCommand")
                .configure(VanillaSoftwareProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(VanillaSoftwareProcess.PRE_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(VanillaSoftwareProcess.CUSTOMIZE_COMMAND, "customizeCommand")
                .configure(VanillaSoftwareProcess.POST_CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(VanillaSoftwareProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "launchCommand")
                .configure(VanillaSoftwareProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "checkRunningCommand")
                .configure(VanillaSoftwareProcess.STOP_COMMAND, "stopCommand");

        // Add sensitive environment variables that are expected to be masked in env stream
        expectedEnv.forEach((key, value) -> entitySpec.configure(VanillaSoftwareProcess.SHELL_ENVIRONMENT.subKey(key), value));

        // Start the application and verify that environment variables unmasked are available in all steps but stopCommand
        VanillaSoftwareProcess entity = app.createAndManageChild(entitySpec);
        app.start(ImmutableList.of(location));
        assertExecsSatisfy(RecordingSshTool.getExecCmds(), ImmutableList.of(
                Predicates.and(ExecCmdPredicates.containsCmd("preInstallCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("installCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("postInstallCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("preCustomizeCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("customizeCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("postCustomizeCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("preLaunchCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("launchCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("postLaunchCommand"), ExecCmdPredicates.containsEnv(expectedEnv)),
                Predicates.and(ExecCmdPredicates.containsCmd("checkRunningCommand"), ExecCmdPredicates.containsEnv(expectedEnv))));

        // Stop the application and verify that environment variables available in stopCommand step unmasked
        app.stop();
        assertExecSatisfies(
                RecordingSshTool.getLastExecCmd(),
                Predicates.and(ExecCmdPredicates.containsCmd("stopCommand"), ExecCmdPredicates.containsEnv(expectedEnv)));

        // Calculate MD5 hash for all keys that are expected to be masked and verify them displayed masked in env stream
        Map<String, String> expectedMaskedEnv = new ImmutableMap.Builder<String, String>()
                .put("KEY1", "VAL1") // this key must appear unmasked, it is not in the list of SECRET NAMES to mask
                .put("KEY2A", "v1=v2 secret=not_hidden_if_on_same_line\nsecret2= "+Sanitizer.suppress("should_be_suppressed"))
                .putAll(Sanitizer.DEFAULT_SENSITIVE_FIELDS_TOKENS.stream().collect(Collectors.toMap(
                        item -> item, // key and expected masked (suppressed) value for a SECRET NAME with MD5 hash
                        Sanitizer::suppress)))
                .build();
        Asserts.assertStringDoesNotContain(getAnyTaskEnvStream(entity), "should_be_suppressed");
        assertEnvStream(entity, expectedMaskedEnv);
    }

    @Override
    public void testGetsStreams() {
        // NOOP
    }

    @Override
    protected Map<String, String> getCommands() {
        return ImmutableMap.<String, String>builder()
                .put("pre-install-command", "myPreInstall")
                .put("installing.*", "myInstall")
                .put("post-install-command", "myPostInstall")
                .put("customizing.*", "myCustomizing")
                .put("pre-launch-command", "myPreLaunch")
                .put("launching.*", "myLaunch")
                .put("post-launch-command", "myPostLaunch")
                .build();
    }
}
