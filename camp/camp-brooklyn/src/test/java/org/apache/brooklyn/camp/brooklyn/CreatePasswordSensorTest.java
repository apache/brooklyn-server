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
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.test.Asserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class CreatePasswordSensorTest extends AbstractYamlTest {

    private static final Logger LOG = LoggerFactory.getLogger(CreatePasswordSensorTest.class);
    
    AttributeSensor<String> PASSWORD_1 = Sensors.newStringSensor("test.password.1");
    AttributeSensor<String> PASSWORD_2 = Sensors.newStringSensor("test.password.2");

    @Test
    public void testProvisioningProperties() throws Exception {
        final Entity app = createAndStartApplication(loadYaml("example-with-CreatePasswordSensor.yaml"));

        waitForApplicationTasks(app);
        Entity entity = Iterables.getOnlyElement(app.getChildren());

        LOG.debug("Passwords are: "+entity.sensors().get(PASSWORD_1)+" / "+entity.sensors().get(PASSWORD_2));
        
        assertPasswordLength(entity, PASSWORD_1, 15);
        assertPasswordOnlyContains(entity, PASSWORD_2, "abc");
    }

    private void assertPasswordOnlyContains(Entity entity, AttributeSensor<String> password, String acceptableChars) {
        String attribute_2 = entity.getAttribute(password);
        for (char c : attribute_2.toCharArray()) {
            Asserts.assertTrue(acceptableChars.indexOf(c) != -1);
        }
    }

    private void assertPasswordLength(Entity entity, AttributeSensor<String> password, int expectedLength) {
        String attribute_1 = entity.getAttribute(password);
        Asserts.assertEquals(attribute_1.length(), expectedLength);
    }

}
