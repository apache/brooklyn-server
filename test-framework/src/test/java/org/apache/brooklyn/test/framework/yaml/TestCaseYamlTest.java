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
package org.apache.brooklyn.test.framework.yaml;

import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.entity.machine.MachineEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.framework.TestCase;
import org.apache.brooklyn.test.framework.TestEffector;
import org.apache.brooklyn.test.framework.TestSensor;
import org.apache.brooklyn.test.framework.TestSshCommand;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

//Checks that the test cases work in YAML
@Test
public class TestCaseYamlTest extends AbstractYamlRebindTest {
    
    // TODO Would like this test to be in brooklyn-camp, but that would create a circular dependency.
    // brooklyn-test-framework depends on brooklyn-camp.
    // To remove that dependency, we could move the DslComponent etc into brooklyn-core.

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TestCaseYamlTest.class);

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
    public void testSimpleTestCases() throws Exception {
        RecordingSshTool.setCustomResponse(".*myCommand.*", new RecordingSshTool.CustomResponse(0, "myResponse", null));
        
        origApp = (BasicApplication) createStartWaitAndLogApplication(
                "location:",
                "  localhost:",
                "    sshToolClass: "+RecordingSshTool.class.getName(),
                "services:",
                "- type: " + MachineEntity.class.getName(),
                "  id: target-app",
                "  brooklyn.config:",
                "    sshMonitoring.enabled: false",
                "    "+BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION.getName()+": true",
                "- type: " + TestCase.class.getName(),
                "  brooklyn.config:",
                "    targetId: target-app",
                "    timeout: " + Asserts.DEFAULT_LONG_TIMEOUT,
                "  brooklyn.children:",
                "  - type: " + TestSensor.class.getName(),
                "    brooklyn.config:",
                "      sensor: service.isUp",
                "      assert:",
                "      - equals: true",
                "  - type: " + TestEffector.class.getName(),
                "    brooklyn.config:",
                "      effector: execCommand",
                "      params:",
                "        command: myCommand",
                "      assert:",
                "      - contains: myResponse",
                "  - type: " + TestSshCommand.class.getName(),
                "    brooklyn.config:",
                "      command: myCommand",
                "      assertStatus:",
                "      - equals: 0",
                "      assertOut:",
                "      - contains: myResponse"
                );

        rebind();
    }
}
