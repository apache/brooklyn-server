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
package org.apache.brooklyn.location.ssh;

import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.util.core.internal.ssh.sshj.SshjTool;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.brooklyn.util.core.internal.ssh.ShellTool.PROP_EXEC_ASYNC;
import static org.apache.brooklyn.util.core.internal.ssh.SshTool.BROOKLYN_CONFIG_KEY_PREFIX;

@Test(groups = "WIP")
public class SshjExecAsyncInSshMachineLocationIntegrationTest extends SshMachineLocationIntegrationTest {
    public static class RecordingSshjTool extends SshjTool {
        public static final AtomicInteger execCount = new AtomicInteger(0);

        public RecordingSshjTool(Map<String, ?> map) {
            super(map);
        }

        @Override
        protected int execScriptAsyncAndPoll(final Map<String,?> props, final List<String> commands, final Map<String,?> env) {
            try {
                return super.execScriptAsyncAndPoll(props, commands, env);
            } finally {
                execCount.incrementAndGet();
            }
        }
    }
    @Override
    protected SshMachineLocation newHost() {
        LocalhostMachineProvisioningLocation localhostMachineProvisioningLocation = (LocalhostMachineProvisioningLocation) mgmt.getLocationRegistry().getLocationManaged("localhost");
        localhostMachineProvisioningLocation.config().putAll(ImmutableMap.of(BROOKLYN_CONFIG_KEY_PREFIX + PROP_EXEC_ASYNC.getName(), true));
        localhostMachineProvisioningLocation.config().set(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshjTool.class.getName());
        try {
            return localhostMachineProvisioningLocation.obtain();
        } catch (NoMachinesAvailableException e) {
            throw Exceptions.propagate(e);
        }
    }

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
    }

    // TODO fix failing test
    @Test(groups = "Integration")
    @Override
    public void testSshExecScript() throws Exception {
        super.testSshExecScript();
        Assert.assertEquals(RecordingSshjTool.execCount.get(), 1);
    }

    @Test(groups = "Integration")
    @Override
    public void testGetMachineDetails() throws Exception {
        super.testGetMachineDetails();
        Assert.assertEquals(RecordingSshjTool.execCount.get(), 1);
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshjTool.execCount.set(0);
    }
}
