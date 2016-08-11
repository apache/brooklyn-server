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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class SshCommandSensorYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(SshCommandSensorYamlTest.class);

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
    }
    
    @Test
    public void testSshCommandSensorWithEffectorInEnv() throws Exception {
        RecordingSshTool.setCustomResponse(".*myCommand.*", new RecordingSshTool.CustomResponse(0, "myResponse", null));
        
        Entity app = createAndStartApplication(
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
            "      period: 10ms",
            "      onlyIfServiceUp: false");
        waitForApplicationTasks(app);

        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor("mySensor"), "myResponse");
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
    
}
