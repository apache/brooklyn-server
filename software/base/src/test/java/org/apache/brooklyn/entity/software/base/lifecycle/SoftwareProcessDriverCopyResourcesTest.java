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
package org.apache.brooklyn.entity.software.base.lifecycle;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.util.os.Os;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

@Test(groups="Integration")
public class SoftwareProcessDriverCopyResourcesTest extends BrooklynAppUnitTestSupport {

    File installDir;
    File runDir;
    File sourceFileDir;
    File sourceTemplateDir;

    private Location location;

    private static final String TEST_CONTENT_FILE = "testing123";
    private static final String TEST_CONTENT_TEMPLATE = "id=${entity.id}";

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        sourceFileDir = Os.newTempDir(getClass().getSimpleName());
        sourceTemplateDir = Os.newTempDir(getClass().getSimpleName());

        installDir = Os.newTempDir(getClass().getSimpleName());
        runDir = Os.newTempDir(getClass().getSimpleName());

        location = app.newLocalhostProvisioningLocation();
    }

    @Test
    public void testPreInstallPhase() throws Exception {
        testPhase(SoftwareProcess.PRE_INSTALL_FILES, SoftwareProcess.PRE_INSTALL_TEMPLATES, SoftwareProcess.INSTALL_DIR);
    }

    @Test
    public void testInstallPhase() throws Exception {
        testPhase(SoftwareProcess.INSTALL_FILES, SoftwareProcess.INSTALL_TEMPLATES, SoftwareProcess.INSTALL_DIR);
    }

    @Test
    public void testCustomisePhase() throws Exception {
        testPhase(SoftwareProcess.CUSTOMIZE_FILES, SoftwareProcess.CUSTOMIZE_TEMPLATES, SoftwareProcess.INSTALL_DIR);
    }

    @Test
    public void testRuntimePhase() throws Exception {
        testPhase(SoftwareProcess.RUNTIME_FILES, SoftwareProcess.RUNTIME_TEMPLATES, SoftwareProcess.RUN_DIR);
    }

    private void testPhase(MapConfigKey<String> filePhase, MapConfigKey<String> templatePhase,
                           AttributeSensor<String> directory) throws IOException {

        File file1 = new File(sourceFileDir, "file1");
        Files.write(TEST_CONTENT_FILE, file1, Charset.defaultCharset());
        File template1 = new File(sourceTemplateDir, "template1");
        Files.write(TEST_CONTENT_TEMPLATE, template1, Charset.defaultCharset());
        final EmptySoftwareProcess testEntity =
            app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class)
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "true")
                .configure(filePhase.getName(),
                    ImmutableMap.of(file1.getAbsolutePath(), "file1"))
                .configure(templatePhase.getName(),
                    ImmutableMap.of(template1.getAbsolutePath(), "template1")));

        app.start(ImmutableList.of(location));
        final String installDirName = testEntity.sensors().get(directory);
        assertNotNull(installDirName);
        final File installDir = new File(installDirName);

        final File file1Installed = new File(installDir, "file1");
        final String firstLine = Files.readFirstLine(file1Installed, Charset.defaultCharset());
        assertEquals(TEST_CONTENT_FILE, firstLine);

        final File template1Installed = new File(installDir, "template1");
        Properties props = new Properties();
        final FileInputStream templateStream = new FileInputStream(template1Installed);
        props.load(templateStream);
        assertEquals(props.getProperty("id"), testEntity.getId());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            Os.deleteRecursively(sourceFileDir);
            Os.deleteRecursively(sourceTemplateDir);
            Os.deleteRecursively(installDir);
            Os.deleteRecursively(runDir);
        }
    }
}
