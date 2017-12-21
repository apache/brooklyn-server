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

import static org.apache.brooklyn.util.core.internal.ssh.ExecCmdAsserts.assertExecContainsLiteral;
import static org.testng.Assert.assertFalse;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.entity.machine.MachineEntity;
import org.apache.brooklyn.entity.machine.SetLimitsCustomizer;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class SetLimitsCustomizerTest extends AbstractYamlRebindTest {
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshTool.clear();
    }

    @Test
    public void testWritesLimits() throws Exception {
        runAppendsToLimitsFile(false);
    }
    
    @Test
    public void testRebindBeforeStarts() throws Exception {
        runAppendsToLimitsFile(true);
    }
    
    protected void runAppendsToLimitsFile(boolean rebindBeforeStart) throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: 0.0.0-SNAPSHOT",
                "  id: machine-with-ulimits",
                "  itemType: entity",
                "  item:",
                "    type: " + MachineEntity.class.getName(),
                "    brooklyn.parameters:",
                "    - name: ulimits",
                "      type: java.util.List",
                "      default:",
                "      - \"* soft nofile 16384\"",
                "      - \"* hard nofile 16384\"",
                "      - \"* soft nproc 16384\"",
                "      - \"* hard nproc 16384\"",
                "    brooklyn.config:",
                "      provisioning.properties:",
                "        machineCustomizers:",
                "          - $brooklyn:object:",
                "              type: "+SetLimitsCustomizer.class.getName(),
                "              brooklyn.config:",
                "                contents: $brooklyn:config(\"ulimits\")");

        Entity app = createApplicationUnstarted(
            "location:",
            "  byon:",
            "    hosts:",
            "    - 240.0.0.1:1234",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "    detectMachineDetails: false",
            "services:",
            "- type: " + MachineEntity.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "    sshMonitoring.enabled: false",
            "    provisioning.properties:",
            "      machineCustomizers:",
            "        - $brooklyn:object:",
            "            type: "+SetLimitsCustomizer.class.getName(),
            "            brooklyn.config:",
            "              contents:",
            "                - \"* soft nofile 1024\"",
            "                - \"* hard nofile 2048\"");
        
        if (rebindBeforeStart) {
            app = rebind();
        }
        app.invoke(Startable.START, ImmutableMap.of()).get();
        waitForApplicationTasks(app);
        
        RecordingSshTool tool = Iterables.getFirst(RecordingSshTool.getTools(), null);
        ExecCmd execCmd = Iterables.getOnlyElement(RecordingSshTool.getExecCmds());
        assertExecContainsLiteral(execCmd, "echo \"* soft nofile 1024\" | tee -a /etc/security/limits.d/50-brooklyn.conf");
        assertExecContainsLiteral(execCmd, "echo \"* hard nofile 2048\" | tee -a /etc/security/limits.d/50-brooklyn.conf");
        
        // should be disconnected, so that subsequent connections will pick up the ulimit changes
        assertFalse(tool.isConnected());
    }
    
    @Test
    public void runFromCatalog() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: 0.0.0-SNAPSHOT",
                "  id: machine-with-ulimits",
                "  itemType: entity",
                "  item:",
                "    type: " + MachineEntity.class.getName(),
                "    brooklyn.parameters:",
                "    - name: ulimits",
                "      type: java.util.List",
                "      default:",
                "      - \"* soft nofile 1024\"",
                "      - \"* hard nofile 2048\"",
                "    brooklyn.config:",
                "      provisioning.properties:",
                "        machineCustomizers:",
                "          - $brooklyn:object:",
                "              type: "+SetLimitsCustomizer.class.getName(),
                "              brooklyn.config:",
                "                contents: $brooklyn:config(\"ulimits\")");

        Entity app = createAndStartApplication(
            "location:",
            "  byon:",
            "    hosts:",
            "    - 240.0.0.1:1234",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "    detectMachineDetails: false",
            "services:",
            "- type: machine-with-ulimits",
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true");
        waitForApplicationTasks(app);
        
        RecordingSshTool tool = Iterables.getFirst(RecordingSshTool.getTools(), null);
        ExecCmd execCmd = Iterables.getOnlyElement(RecordingSshTool.getExecCmds());
        assertExecContainsLiteral(execCmd, "echo \"* soft nofile 1024\" | tee -a /etc/security/limits.d/50-brooklyn.conf");
        assertExecContainsLiteral(execCmd, "echo \"* hard nofile 2048\" | tee -a /etc/security/limits.d/50-brooklyn.conf");
        
        // should be disconnected, so that subsequent connections will pick up the ulimit changes
        assertFalse(tool.isConnected());
    }

}
