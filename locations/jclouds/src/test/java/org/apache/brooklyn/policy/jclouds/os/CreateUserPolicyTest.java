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
import static org.testng.Assert.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class CreateUserPolicyTest extends BrooklynAppUnitTestSupport {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(CreateUserPolicyTest.class);

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
    
    @Test
    public void testCallsCreateUser() throws Exception {
        SshMachineLocation machine = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                .configure(SshTool.PROP_USER, "myuser")
                .configure(SshTool.PROP_PASSWORD, "mypassword")
                .configure("address", "1.2.3.4")
                .configure(SshTool.PROP_PORT, 1234));
    
        String newUsername = "mynewuser";
        
        app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(CreateUserPolicy.class)
                        .configure(CreateUserPolicy.GRANT_SUDO, true)
                        .configure(CreateUserPolicy.RESET_LOGIN_USER, false)
                        .configure(CreateUserPolicy.VM_USERNAME, newUsername)));
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        app.start(ImmutableList.of(machine));
        
        String creds = EntityAsserts.assertAttributeEventuallyNonNull(entity, CreateUserPolicy.VM_USER_CREDENTIALS);
        Pattern pattern = Pattern.compile("(.*) : (.*) @ (.*):(.*)");
        Matcher matcher = pattern.matcher(creds);
        assertTrue(matcher.matches());
        String username2 = matcher.group(1).trim();
        String password = matcher.group(2).trim();
        String hostname = matcher.group(3).trim();
        String port = matcher.group(4).trim();
        
        assertEquals(newUsername, username2);
        assertEquals(hostname, "1.2.3.4");
        assertEquals(password.length(), 12);
        assertEquals(port, "1234");

        boolean found = false;
        for (ExecCmd cmds : RecordingSshTool.getExecCmds()) {
            if (cmds.commands.toString().contains("useradd")) {
                found = true;
                break;
            }
        }
        assertTrue(found, "useradd not found in: "+RecordingSshTool.getExecCmds());
    }
}
