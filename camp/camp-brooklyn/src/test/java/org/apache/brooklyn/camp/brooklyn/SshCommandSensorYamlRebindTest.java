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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.feed.ssh.SshFeed;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

@Test
public class SshCommandSensorYamlRebindTest extends AbstractYamlRebindTest {

   @Test
   public void testSshCommandSensorWithEffectorInEnv() throws Exception {
       RecordingSshTool.setCustomResponse(".*myCommand.*", new RecordingSshTool.CustomResponse(0, "myResponse", null));
       
       createStartWaitAndLogApplication(
           "location:",
           "  localhost:",
           "    sshToolClass: "+RecordingSshTool.class.getName(),
           "services:",
           "- type: " + VanillaSoftwareProcess.class.getName(),
           "  brooklyn.config:",
           "    onbox.base.dir.skipResolution: true",
           "  brooklyn.initializers:",
           "  - type: org.apache.brooklyn.core.sensor.ssh.SshCommandSensor",
           "    brooklyn.config:",
           "      name: mySensor",
           "      command: myCommand",
           "      executionDir: '/path/to/myexecutiondir'",
           "      shell.env:",
           "        MY_ENV: myEnvVal",
           "      period: 10ms",
           "      onlyIfServiceUp: false");

       StartableApplication newApp = rebind();
       VanillaSoftwareProcess newEntity = (VanillaSoftwareProcess) Iterables.getOnlyElement(newApp.getChildren());
       SshFeed newFeed = (SshFeed) Iterables.find(((EntityInternal)newEntity).feeds().getFeeds(), Predicates.instanceOf(SshFeed.class));
       
       // Clear history of commands, and the sensor, so can confirm it gets re-set by the ssh feed
       RecordingSshTool.clearCmdHistory();
       newEntity.sensors().set(Sensors.newStringSensor("mySensor"), null);
       
       // Assert sensor is set, and command is executed as expected
       EntityAsserts.assertAttributeEqualsEventually(newEntity, Sensors.newStringSensor("mySensor"), "myResponse");
       ExecCmd cmd = Asserts.succeedsEventually(() -> RecordingSshTool.getLastExecCmd());
       
       assertTrue(cmd.commands.toString().contains("myCommand"), "cmds="+cmd.commands);
       assertEquals(cmd.env.get("MY_ENV"), "myEnvVal", "env="+cmd.env);
       assertTrue(cmd.commands.toString().contains("/path/to/myexecutiondir"), "cmds="+cmd.commands);
       
       // Confirm feed's memento is 'clean' - no anonymous inner classes
       BrooklynMementoRawData rawMemento = loadMementoRawData();
       String rawFeedMemento = rawMemento.getFeeds().get(newFeed.getId());
       assertFalse(rawFeedMemento.contains("$1"), rawFeedMemento);
       assertFalse(rawFeedMemento.contains("$2"), rawFeedMemento);
       assertFalse(rawFeedMemento.contains("$3"), rawFeedMemento);
   }
}
