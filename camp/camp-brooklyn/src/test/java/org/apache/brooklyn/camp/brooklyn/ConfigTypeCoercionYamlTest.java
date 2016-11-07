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

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfigTypeCoercionYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(ConfigTypeCoercionYamlTest.class);

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
    }
    
    @Test
    public void testSshConfigFromDefault() throws Exception {
        RecordingSshTool.setCustomResponse(".*myCommand.*", new RecordingSshTool.CustomResponse(0, "myResponse", null));
        
        String bp = loadYaml("config-type-coercion-test.yaml",
            "location:",
            "  localhost:",
            "    sshToolClass: "+RecordingSshTool.class.getName());
        // remove all lines referring to "exact" -- that's useful for expository and running in UI
        // but it will fail (timeout) if the port isn't available so not good in tests
        bp = Strings.removeLines(bp, StringPredicates.containsLiteralIgnoreCase("exact"));
        
        Entity app = createAndStartApplication(bp);
        waitForApplicationTasks(app);

        Map<?, ?> props = RecordingSshTool.getLastExecCmd().env;
        
        Assert.assertEquals(props.get("RANGE_PORT_SENSOR"), "20003");
        Asserts.assertStringContains((String)props.get("RANGE_PORT_CONFIG"), "{\"start\"", "20003");
        
        Assert.assertEquals(props.get("INT_PORT_CONFIG"), "20001");
        Assert.assertEquals(props.get("INT_PORT_DEFAULT_CONFIG"), "30001");
        
        Assert.assertEquals(props.get("RANGE_PORT_DEFAULT_SENSOR"), "30003");
        // NB: change in Oct 2016, default values are now coerced just like explicit value
        // (previous to Oct 2016 this would have returned just "30003+", no json)
        Asserts.assertStringContains((String)props.get("RANGE_PORT_DEFAULT_CONFIG"), "{\"start\"", "30003");
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
