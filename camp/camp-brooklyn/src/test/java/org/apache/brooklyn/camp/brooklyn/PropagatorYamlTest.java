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
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.enricher.stock.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

@Test
public class PropagatorYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(PropagatorYamlTest.class);

    AttributeSensor<String> mySourceSensor = Sensors.newStringSensor("mySource");
    AttributeSensor<String> myTargetSensor = Sensors.newStringSensor("myTarget");

    @Test
    public void testPropagateSensorMapping() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Propagator.class.getName(),
                "      brooklyn.config:",
                "        "+Propagator.PRODUCER.getName()+": $brooklyn:parent()",
                "        "+Propagator.SENSOR_MAPPING.getName()+": ",
                "          mySource: myTarget");
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
        
        app.sensors().set(mySourceSensor, "myval");
        EntityAsserts.assertAttributeEqualsEventually(entity, myTargetSensor, "myval");
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
