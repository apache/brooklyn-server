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
package org.apache.brooklyn.policy.jclouds.os;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class CreateUserPolicyRebindTest extends RebindTestFixtureWithApp {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CreateUserPolicyRebindTest.class);

    @BeforeMethod(alwaysRun=true)
    @Override
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
    
    @Override
    protected Boolean shouldSkipOnBoxBaseDirResolution() {
        return true;
    }

    protected boolean isUseraddExecuted() {
        for (ExecCmd cmds : RecordingSshTool.getExecCmds()) {
            if (cmds.commands.toString().contains("useradd")) {
                return true;
            }
        }
        return false;
    }

    // See BROOKLYN-386
    @Test
    public void testNotCallCreateUserOnRebind() throws Exception {
        SshMachineLocation machine = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                .configure("address", "1.2.3.4"));
    
        String newUsername = "mynewuser";
        
        app().createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(CreateUserPolicy.class)
                        .configure(CreateUserPolicy.GRANT_SUDO, true)
                        .configure(CreateUserPolicy.RESET_LOGIN_USER, false)
                        .configure(CreateUserPolicy.VM_USERNAME, newUsername)));
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app().getChildren());
        app().start(ImmutableList.of(machine));
        
        String creds = EntityAsserts.assertAttributeEventuallyNonNull(entity, CreateUserPolicy.VM_USER_CREDENTIALS);
        if (!isUseraddExecuted()) {
            fail("useradd not found in: "+RecordingSshTool.getExecCmds());
        }
        RecordingSshTool.clear();
        
        rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(app().getChildren());
        
        Asserts.succeedsContinually(ImmutableMap.of("timeout", Duration.millis(250)), new Runnable() {
            @Override public void run() {
                if (isUseraddExecuted()) {
                    fail("useradd found in: "+RecordingSshTool.getExecCmds());
                }
            }});
        assertEquals(newEntity.sensors().get(CreateUserPolicy.VM_USER_CREDENTIALS), creds);
    }
}
