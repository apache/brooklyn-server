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

package org.apache.brooklyn.location.jclouds;

import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.location.MachineLifecycleUtils;
import org.apache.brooklyn.core.location.MachineLifecycleUtils.MachineStatus;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JcloudsLocationSuspendResumeMachineLiveTest extends AbstractJcloudsLiveTest {

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsLocationSuspendResumeMachineLiveTest.class);

    private static final String EUWEST_IMAGE_ID = AWS_EC2_EUWEST_REGION_NAME + "/" + "ami-ce7b6fba";

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        jcloudsLocation = (JcloudsLocation) managementContext.getLocationRegistry()
                .getLocationManaged(AWS_EC2_PROVIDER + ":" + AWS_EC2_EUWEST_REGION_NAME);
    }

    @Test(groups = "Live")
    public void testObtainThenSuspendThenResumeMachine() throws Exception {
        MachineLocation machine = obtainMachine(ConfigBag.newInstance()
                .configure(JcloudsLocationConfig.IMAGE_ID, EUWEST_IMAGE_ID)
                .configure(JcloudsLocationConfig.OPEN_IPTABLES, false)  // optimization
                .getAllConfig());
        JcloudsSshMachineLocation sshMachine = (JcloudsSshMachineLocation) machine;
        assertTrue(sshMachine.isSshable(), "Cannot SSH to " + sshMachine);

        suspendMachine(machine);
        ((SshMachineLocation)machine).setSshCheckTimeout(Duration.FIVE_SECONDS);
        assertFalse(sshMachine.isSshable(), "Should not be able to SSH to suspended machine");

        ((SshMachineLocation)machine).setSshCheckTimeout(null);
        MachineLocation machine2 = resumeMachine(ImmutableMap.of("id", sshMachine.getJcloudsId()));
        assertTrue(machine2 instanceof JcloudsSshMachineLocation);
        assertTrue(((JcloudsSshMachineLocation) machine2).isSshable(), "Cannot SSH to " + machine2);
    }

    @Test(groups = "Live")
    public void testObtainThenShutdownThenRestart() throws Exception {
        MachineLocation machine = obtainMachine(ConfigBag.newInstance()
                .configure(JcloudsLocationConfig.IMAGE_ID, EUWEST_IMAGE_ID)
                .configure(JcloudsLocationConfig.OPEN_IPTABLES, false)  // optimization
                .getAllConfig());
        JcloudsSshMachineLocation sshMachine = (JcloudsSshMachineLocation) machine;
        Assert.assertEquals(new MachineLifecycleUtils(sshMachine).getStatus(), MachineStatus.RUNNING);
        assertTrue(sshMachine.isSshable(), "Cannot SSH to " + sshMachine);

        jcloudsLocation.shutdownMachine(sshMachine);
        sshMachine.setSshCheckTimeout(Duration.FIVE_SECONDS);
        assertFalse(sshMachine.isSshable(), "Should not be able to SSH to suspended machine");

        Assert.assertEquals(new MachineLifecycleUtils(sshMachine).exists(), Boolean.TRUE);
        Assert.assertEquals(new MachineLifecycleUtils(sshMachine).getStatus(), MachineStatus.SUSPENDED);  // shutdown suspends in AWS
        sshMachine.setSshCheckTimeout(null);

        MachineLocation machine2 = new MachineLifecycleUtils(sshMachine).makeRunning().get();
        assertTrue(machine2 instanceof JcloudsSshMachineLocation);
        assertTrue(((JcloudsSshMachineLocation) machine2).isSshable(), "Cannot SSH to " + machine2);
        Assert.assertEquals(new MachineLifecycleUtils(machine2).getStatus(), MachineStatus.RUNNING);
    }

}
