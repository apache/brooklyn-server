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
package org.apache.brooklyn.test.framework;

import static org.apache.brooklyn.core.entity.trait.Startable.SERVICE_UP;
import static org.apache.brooklyn.test.framework.TargetableTestComponent.TARGET_ENTITY;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.CONTAINS;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.EQUALS;
import static org.apache.brooklyn.test.framework.TestSshCommand.ASSERT_ERR;
import static org.apache.brooklyn.test.framework.TestSshCommand.ASSERT_OUT;
import static org.apache.brooklyn.test.framework.TestSshCommand.ASSERT_STATUS;
import static org.apache.brooklyn.test.framework.TestSshCommand.COMMAND;
import static org.apache.brooklyn.test.framework.TestSshCommand.DOWNLOAD_URL;
import static org.apache.brooklyn.test.framework.TestSshCommand.RUN_DIR;
import static org.apache.brooklyn.test.framework.TestSshCommand.TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestSshCommandIntegrationTest extends BrooklynAppUnitTestSupport {

    private TestEntity testEntity;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .location(TestApplication.LOCALHOST_MACHINE_SPEC));
    }

    @Test(groups = "Integration")
    public void shouldRetryCommandWithinTimeout() throws Exception {
        TestSshCommand testWithCmd = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
                .configure(TARGET_ENTITY, testEntity)
                .configure(COMMAND, "_djaf-_f£39r24")
                .configure(RUN_DIR, "/tmp")
                .configure(TIMEOUT, Duration.TEN_SECONDS)
                .configure(ASSERT_STATUS, makeAssertions(ImmutableMap.of(EQUALS, 0))));

        // Run the test's assertions in parallel with the app.
        Thread t = new Thread() {
            @Override
            public void run() {
                app.start(ImmutableList.<Location>of());
            }
        };
        t.start();

        // Times out in one second.
        EntityAsserts.assertAttributeContinuallyNotEqualTo(testWithCmd, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);

        // Replace command with a valid one and the test should start to pass.
        testWithCmd.config().set(COMMAND, "true");
        EntityAsserts.assertAttributeEqualsEventually(testWithCmd, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);

        t.join();
    }

    @Test(groups = "Integration")
    public void shouldExecuteInTheRunDir() throws Exception {
        Path pwdPath = createTempScript("pwd", "pwd");

        try {
            Map<String, ?> equalsZero = ImmutableMap.of(EQUALS, 0);
            Map<String, ?> containsTmp = ImmutableMap.of(CONTAINS, "/tmp");

            TestSshCommand testWithScript = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
                .configure(TARGET_ENTITY, testEntity)
                .configure(DOWNLOAD_URL, "file:" + pwdPath)
                .configure(RUN_DIR, "/tmp")
                .configure(ASSERT_STATUS, makeAssertions(equalsZero))
                .configure(ASSERT_OUT, makeAssertions(containsTmp)));


            TestSshCommand testWithCmd = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
                .configure(TARGET_ENTITY, testEntity)
                .configure(COMMAND, "pwd")
                .configure(RUN_DIR, "/tmp")
                .configure(ASSERT_STATUS, makeAssertions(equalsZero))
                .configure(ASSERT_OUT, makeAssertions(containsTmp)));

            app.start(ImmutableList.<Location>of());

            assertThat(testWithScript.sensors().get(SERVICE_UP)).isTrue().withFailMessage("Service should be up");
            assertThat(ServiceStateLogic.getExpectedState(testWithScript)).isEqualTo(Lifecycle.RUNNING)
                .withFailMessage("Service should be marked running");

            assertThat(testWithCmd.sensors().get(SERVICE_UP)).isTrue().withFailMessage("Service should be up");
            assertThat(ServiceStateLogic.getExpectedState(testWithCmd)).isEqualTo(Lifecycle.RUNNING)
                .withFailMessage("Service should be marked running");

        } finally {
            Files.delete(pwdPath);
        }
    }

    @Test(groups = "Integration")
    public void shouldCaptureStdoutAndStderrOfCommands() {
        TestSshCommand uptime = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
            .configure(TARGET_ENTITY, testEntity)
            .configure(COMMAND, "echo 'a' 'b' && CMDSUFFIX=Suffix && doesNotExist${CMDSUFFIX}")
            .configure(ASSERT_OUT, makeAssertions(ImmutableMap.of(CONTAINS, "a b")))
            .configure(ASSERT_ERR, makeAssertions(ImmutableMap.of(CONTAINS, "doesNotExistSuffix"))));

        app.start(ImmutableList.<Location>of());

        assertThat(uptime.sensors().get(SERVICE_UP)).isTrue()
            .withFailMessage("Service should be up");
        assertThat(ServiceStateLogic.getExpectedState(uptime)).isEqualTo(Lifecycle.RUNNING)
            .withFailMessage("Service should be marked running");
    }

    @Test(groups = "Integration")
    public void shouldCaptureStdoutAndStderrOfScript() throws Exception {
        String text = "echo 'a' 'b' && CMDSUFFIX=Suffix && doesNotExist${CMDSUFFIX}";
        Path testScript = createTempScript("script", "echo " + text);

        try {
            TestSshCommand uptime = app.createAndManageChild(EntitySpec.create(TestSshCommand.class)
                    .configure(TARGET_ENTITY, testEntity)
                    .configure(DOWNLOAD_URL, "file:" + testScript)
                    .configure(ASSERT_OUT, makeAssertions(ImmutableMap.of(CONTAINS, "a b")))
                    .configure(ASSERT_ERR, makeAssertions(ImmutableMap.of(CONTAINS, "doesNotExistSuffix"))));

                app.start(ImmutableList.<Location>of());

                assertThat(uptime.sensors().get(SERVICE_UP)).isTrue()
                    .withFailMessage("Service should be up");
                assertThat(ServiceStateLogic.getExpectedState(uptime)).isEqualTo(Lifecycle.RUNNING)
                    .withFailMessage("Service should be marked running");

        } finally {
            Files.delete(testScript);
        }
    }
    
    private Path createTempScript(String filename, String contents) {
        try {
            Path tempFile = Files.createTempFile("SimpleShellCommandIntegrationTest-" + filename, ".sh");
            Files.write(tempFile, contents.getBytes());
            return tempFile;
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    private List<Map<String, ?>> makeAssertions(Map<String, ?> map) {
        return ImmutableList.<Map<String, ?>>of(map);
    }
}
