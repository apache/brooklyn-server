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
import static org.testng.Assert.fail;

import java.util.List;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class SshCommandEffectorYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(SshCommandEffectorYamlTest.class);

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
    }
    
    @Test
    public void testSshCommandEffectorWithParameter() throws Exception {
        RecordingSshTool.setCustomResponse(".*myCommand.*", new RecordingSshTool.CustomResponse(0, "myResponse", null));
        
        Entity app = createAndStartApplication(
            "location:",
            "  localhost:",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "services:",
            "- type: " + VanillaSoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "    softwareProcess.serviceProcessIsRunningPollPeriod: forever",
            "  brooklyn.initializers:",
            "  - type: org.apache.brooklyn.core.effector.ssh.SshCommandEffector",
            "    brooklyn.config:",
            "      name: myEffector",
            "      description: myDescription",
            "      command: myCommand",
            "      parameters:",
            "        param1:",
            "          description: effector param, to be set as env",
            "          defaultValue: defaultValForParam1");
        waitForApplicationTasks(app);

        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myEffector").get();

        // Invoke with parameter
        {
            Object result = entity.invoke(effector, ImmutableMap.of("param1", "myCustomParam1Val")).get();
            assertEquals(((String)result).trim(), "myResponse");
    
            ExecCmd lastExecCmd = RecordingSshTool.getLastExecCmd();
            assertIncludesCmd(lastExecCmd.commands, "myCommand");
            assertEquals(lastExecCmd.env, ImmutableMap.of("param1", "myCustomParam1Val"));
        }
        
        // Invoke with default parameter
        {
            Object result = entity.invoke(effector, ImmutableMap.<String, Object>of()).get();
            assertEquals(((String)result).trim(), "myResponse");
    
            ExecCmd lastExecCmd = RecordingSshTool.getLastExecCmd();
            assertIncludesCmd(lastExecCmd.commands, "myCommand");
            assertEquals(lastExecCmd.env, ImmutableMap.of("param1", "defaultValForParam1"));
        }
    }
    
    @Test
    public void testSshCommandEffectorWithShellEnv() throws Exception {
        RecordingSshTool.setCustomResponse(".*myCommand.*", new RecordingSshTool.CustomResponse(0, "myResponse", null));
        
        Entity app = createAndStartApplication(
            "location:",
            "  localhost:",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "services:",
            "- type: " + VanillaSoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "    softwareProcess.serviceProcessIsRunningPollPeriod: forever",
            "  brooklyn.initializers:",
            "  - type: org.apache.brooklyn.core.effector.ssh.SshCommandEffector",
            "    brooklyn.config:",
            "      name: myEffector",
            "      description: myDescription",
            "      command: myCommand",
            "      shell.env:",
            "        env1: myEnv1Val");
        waitForApplicationTasks(app);

        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myEffector").get();

        Object result = entity.invoke(effector, ImmutableMap.<String, Object>of()).get();
        assertEquals(((String)result).trim(), "myResponse");

        ExecCmd lastExecCmd = RecordingSshTool.getLastExecCmd();
        assertIncludesCmd(lastExecCmd.commands, "myCommand");
        assertEquals(lastExecCmd.env, ImmutableMap.of("env1", "myEnv1Val"));
    }
    
    @Test
    public void testSshCommandEffectorWithExecutionDir() throws Exception {
        RecordingSshTool.setCustomResponse(".*myCommand.*", new RecordingSshTool.CustomResponse(0, "myResponse", null));
        
        Entity app = createAndStartApplication(
            "location:",
            "  localhost:",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "services:",
            "- type: " + VanillaSoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "    softwareProcess.serviceProcessIsRunningPollPeriod: forever",
            "  brooklyn.initializers:",
            "  - type: org.apache.brooklyn.core.effector.ssh.SshCommandEffector",
            "    brooklyn.config:",
            "      name: myEffector",
            "      description: myDescription",
            "      command: myCommand",
            "      executionDir: /my/path/execDir");
        waitForApplicationTasks(app);

        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myEffector").get();

        Object result = entity.invoke(effector, ImmutableMap.<String, Object>of()).get();
        assertEquals(((String)result).trim(), "myResponse");

        ExecCmd lastExecCmd = RecordingSshTool.getLastExecCmd();
        assertIncludesCmd(lastExecCmd.commands, "mkdir -p '/my/path/execDir'");
        assertIncludesCmd(lastExecCmd.commands, "cd '/my/path/execDir'");
    }
    
    protected void assertIncludesCmd(List<String> cmds, String expected) {
        for (String cmd : cmds) {
            if (cmd.contains(expected)) {
                return;
            }
        }
        fail("Commands do not contain '"+expected+"': " + cmds);
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
