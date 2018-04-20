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

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecParams;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
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
    
    // "Integration" because takes a second 
    @Test(groups="Integration")
    public void testSupressingDuplicates() throws Exception {
        AttributeSensor<String> mySensor = Sensors.newStringSensor("mySensor");
        
        RecordingSensorEventListener<String> listener = new RecordingSensorEventListener<>();
        Application tmpApp = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        tmpApp.subscriptions().subscribe(null, mySensor, listener);
        
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
            "      suppressDuplicates: true",
            "      period: 10ms",
            "      onlyIfServiceUp: false");
        waitForApplicationTasks(app);

        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertAttributeEqualsEventually(entity, mySensor, "myResponse");
        listener.assertHasEventEventually(Predicates.alwaysTrue());
        
        Asserts.succeedsContinually(new Runnable() {
            @Override public void run() {
                listener.assertEventCount(1);
            }});
    }

    @Test
    public void testDslWithSshSensor() throws Exception {
        // Set up a custom response for the echo command.
        // Simulate the execution of the echo, substituting the environment variables as well.
        RecordingSshTool.setCustomResponse(".*echo \"mytest .*", new RecordingSshTool.CustomResponseGenerator() {
            @Override public CustomResponse generate(ExecParams execParams) throws Exception {
                Optional<String> echoBody = extractEchoBody(execParams.commands);
                if (echoBody.isPresent()) {
                    String stdout = evaluateEchoBody(execParams, echoBody.get());
                    String stderr = "";
                    return new CustomResponse(0, stdout, stderr);
                } else {
                    return new CustomResponse(0, "", "");
                }
            }
            private Optional<String> extractEchoBody(List<String> cmds) {
                for (String cmd : cmds) {
                    if (cmd.contains("echo \"mytest ")) {
                        String echoBody = cmd.substring(cmd.indexOf("echo \"mytest ") + 6);
                        echoBody = Strings.removeFromEnd(echoBody.trim(), "\"");
                        return Optional.of(echoBody);
                    }
                }
                return Optional.absent();
            }
            private String evaluateEchoBody(ExecParams execParams, String echoBody) {
                String result = echoBody;
                for (Map.Entry<String, ?> entry : execParams.env.entrySet()) {
                    String envName = entry.getKey();
                    String envVal = entry.getValue() != null ? entry.getValue().toString() : "";
                    result = result.replaceAll("\\$"+envName, envVal);
                    result = result.replaceAll("\\$\\{"+envName+"\\}", envVal);
                }
                return result;
            }
        });

        AttributeSensor<String> mySensor = Sensors.newStringSensor("mySensor");
        AttributeSensor<String> sourceSensor = Sensors.newStringSensor("sourceSensor");

        Entity app = createAndStartApplication(
                "location:",
                "  localhost:",
                "    sshToolClass: "+RecordingSshTool.class.getName(),
                "services:",
                "- type: " + VanillaSoftwareProcess.class.getName(),
                "  brooklyn.config:",
                "    myconf: myconfval",
                "    onbox.base.dir.skipResolution: true",
                "    checkRunning.command: true",
                "  brooklyn.initializers:",
                "  - type: org.apache.brooklyn.core.sensor.StaticSensor",
                "    brooklyn.config:",
                "      name: " + sourceSensor.getName(),
                "      sensorType: string",
                "      static.value: someValue",
                "  - type: org.apache.brooklyn.core.sensor.ssh.SshCommandSensor",
                "    brooklyn.config:",
                "      name: " + mySensor.getName(),
                "      shell.env:",
                "        MY_ENV: $brooklyn:config(\"myconf\")",
                "      command:",
                "        $brooklyn:formatString:",
                "        - echo \"mytest %s ${MY_ENV}\"",
                "        - $brooklyn:attributeWhenReady(\"sourceSensor\")",
                "      suppressDuplicates: true",
                "      period: 10ms",
                "      onlyIfServiceUp: false");
        waitForApplicationTasks(app);

        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertAttributeEqualsEventually(entity, mySensor, "mytest someValue myconfval");

        entity.sensors().set(sourceSensor, "newValue");
        EntityAsserts.assertAttributeEqualsEventually(entity, mySensor, "mytest newValue myconfval");
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
    
}
