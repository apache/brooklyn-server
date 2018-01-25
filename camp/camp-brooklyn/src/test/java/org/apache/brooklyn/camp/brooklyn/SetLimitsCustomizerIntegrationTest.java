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

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.entity.machine.SetLimitsCustomizer;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class SetLimitsCustomizerIntegrationTest extends AbstractYamlRebindTest {

    private File file;
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        if (file != null) file.delete();
        super.tearDown();
    }
    
    
    // Not using the default /etc/security/limits.d file, because don't want to risk destroying the dev machine's config!
    @Test(groups="Integration")
    public void testAppendsToGivenFile() throws Exception {
        file = File.createTempFile("testAppendsToGivenFile", ".conf");
        
        Entity app = createAndStartApplication(
            "location: localhost",
            "services:",
            "- type: " + EmptySoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    provisioning.properties:",
            "      machineCustomizers:",
            "        - $brooklyn:object:",
            "            type: "+SetLimitsCustomizer.class.getName(),
            "            brooklyn.config:",
            "              file: " + file.getAbsolutePath(),
            "              contents:",
            "                - my line 1",
            "                - my line 2");
        waitForApplicationTasks(app);
        
        List<String> actual = Files.readAllLines(file.toPath());
        assertEquals(actual, ImmutableList.of("my line 1", "my line 2"), "actual="+actual);
    }
}
