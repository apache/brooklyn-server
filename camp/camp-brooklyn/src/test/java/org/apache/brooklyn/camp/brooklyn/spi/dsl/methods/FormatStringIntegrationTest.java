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
package org.apache.brooklyn.camp.brooklyn.spi.dsl.methods;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.entity.software.base.SameServerEntity;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.testng.Assert.assertEquals;

public class FormatStringIntegrationTest extends AbstractYamlTest {

    private static final Logger LOG = LoggerFactory.getLogger(FormatStringIntegrationTest.class);

    @Test(groups = "Integration")
    public void testFormatString() throws Exception {

        final Path tempFile = Files.createTempFile("testFormatString", ".txt");
        LOG.info("Temp file is {}", tempFile.toAbsolutePath());

        try {
            Entity app = createAndStartApplication(
                "location: localhost:(name=localhost)",
                "services:",
                "- type: " + SameServerEntity.class.getName(),
                "  brooklyn.children:",
                "  - type: " + VanillaSoftwareProcess.class.getName(),
                "    id: sensorEntity",
                "    launch.command: while true; do sleep 3600 ; done & echo $! > ${PID_FILE}",
                "    brooklyn.initializers:",
                "    - type: org.apache.brooklyn.core.sensor.ssh.SshCommandSensor",
                "      brooklyn.config:",
                "        name: greeting",
                "        period: 2s",
                "        command: echo world",
                "  - type: " + VanillaSoftwareProcess.class.getName(),
                "    id: consumerEntity",
                "    launch.command: while true; do sleep 3600 ; done & echo $! > ${PID_FILE}",
                "    shell.env:",
                "      RESPONSE: $brooklyn:formatString(\"hello %s\", $brooklyn:entity(\"sensorEntity\").attributeWhenReady(\"greeting\"))",
                "    post.launch.command: echo ${RESPONSE} > " + tempFile.toAbsolutePath()
            );
            waitForApplicationTasks(app);

            final String contents = new String(Files.readAllBytes(tempFile)).trim();
            assertEquals(contents, "hello world", "file contents: " + contents);

        } finally {
            Files.delete(tempFile);
        }
    }
}
