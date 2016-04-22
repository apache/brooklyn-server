/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.rest.test.entity;

import static org.apache.brooklyn.test.Asserts.assertNotNull;
import static org.apache.brooklyn.test.Asserts.assertTrue;
import static org.testng.Assert.fail;

import java.net.URI;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.entity.brooklynnode.BrooklynNode;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.rest.BrooklynRestApiLauncherTestFixture;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.task.ssh.SshTasks;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.os.Os;
import org.eclipse.jetty.server.Server;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class VanillaSoftwareProcessTest extends BrooklynRestApiLauncherTestFixture {

    Server server;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        server = newServer();
        useServerForTest(server);
    }

    @Test(groups = "Integration")
    public void testVanillaSoftwareProcessCanUseResourcesInBundles() throws Exception {
        try {
            new ResourceUtils(this).getResourceAsString("classpath://org/apache/brooklyn/test/osgi/resources/message.txt");
            fail("classpath://org/apache/brooklyn/test/osgi/resources/message.txt should not be on classpath");
        } catch (Exception e) {/* expected */}
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), "/brooklyn/osgi/brooklyn-test-osgi-entities.jar");

        final String catalogItemUrl = "classpath://vanilla-software-process-with-resource.yaml";
        final String catalogYaml = ResourceUtils.create(this)
                .getResourceAsString(catalogItemUrl);

        final URI webConsoleUri = URI.create(getBaseUriRest(server));

        // Test setup
        final EntitySpec<BrooklynNode> spec = EntitySpec.create(BrooklynNode.class);
        final ManagementContext mgmt = getManagementContextFromJettyServerAttributes(server);
        final BrooklynNode node = mgmt.getEntityManager().createEntity(spec);
        node.sensors().set(BrooklynNode.WEB_CONSOLE_URI, webConsoleUri);

        // Add catalogue item.
        HttpToolResponse response = node.http().post(
                "/catalog",
                ImmutableMap.<String, String>of(),
                catalogYaml.getBytes());
        HttpAsserts.assertHealthyStatusCode(response.getResponseCode());

        // Deploy it.
        final String blueprint = "location: localhost\n" +
                "services:\n" +
                "- type: vanilla-software-resource-test:1.0";
        response = node.http().post(
                "/applications",
                ImmutableMap.of("Content-Type", "text/yaml"),
                blueprint.getBytes());
        HttpAsserts.assertHealthyStatusCode(response.getResponseCode());

        // Assert application is eventually running and not on fire.
        final Entity vanilla = mgmt.getApplications().iterator().next().getChildren().iterator().next();
        assertTrue(vanilla instanceof VanillaSoftwareProcess,
                "expected " + VanillaSoftwareProcess.class.getName() + ", found: " + vanilla);
        EntityAsserts.assertAttributeEqualsEventually(vanilla, SoftwareProcess.SERVICE_UP, true);

        // And check that the message was copied to rundir.
        SshMachineLocation machine = Machines.findUniqueMachineLocation(vanilla.getLocations(), SshMachineLocation.class).get();
        String file = Os.mergePaths(vanilla.sensors().get(SoftwareProcess.RUN_DIR), "message.txt");
        String message = Entities.submit(vanilla, SshTasks.newSshFetchTaskFactory(machine, file).newTask()).get();
        assertNotNull(message);
        assertTrue(message.startsWith("Licensed to the Apache Software Foundation"),
                "expected ASF license header, found: " + message);

    }

}
