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
import org.apache.brooklyn.enricher.stock.Aggregator;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Test
public class AggregatorYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(AggregatorYamlTest.class);

    AttributeSensor<Object> myVal = Sensors.newSensor(Object.class, "myVal");
    AttributeSensor<Object> myResult = Sensors.newSensor(Object.class, "myResult");

    @Test
    public void testSum() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "    - type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Aggregator.class.getName(),
                "      brooklyn.config:",
                "        "+Aggregator.SOURCE_SENSOR.getName()+": myVal",
                "        "+Aggregator.TARGET_SENSOR.getName()+": myResult",
                "        "+Aggregator.TRANSFORMATION_UNTYPED.getName()+": sum");
        Entity child1 = Iterables.get(app.getChildren(), 0);
        Entity child2 = Iterables.get(app.getChildren(), 1);
        
        child1.sensors().set(myVal, 1d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 1d);
        
        child2.sensors().set(myVal, 2d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 3d);
        
        child1.sensors().set(myVal, 3d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 5d);
        
        child2.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 3d);
    }
    
    @Test
    public void testAverage() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "    - type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Aggregator.class.getName(),
                "      brooklyn.config:",
                "        "+Aggregator.SOURCE_SENSOR.getName()+": myVal",
                "        "+Aggregator.TARGET_SENSOR.getName()+": myResult",
                "        "+Aggregator.TRANSFORMATION_UNTYPED.getName()+": average");
        Entity child1 = Iterables.get(app.getChildren(), 0);
        Entity child2 = Iterables.get(app.getChildren(), 1);
        
        child1.sensors().set(myVal, 1d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 1d);
        
        child2.sensors().set(myVal, 3d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 2d);
        
        child1.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 3d);
    }
    
    @Test
    public void testMin() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "    - type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Aggregator.class.getName(),
                "      brooklyn.config:",
                "        "+Aggregator.SOURCE_SENSOR.getName()+": myVal",
                "        "+Aggregator.TARGET_SENSOR.getName()+": myResult",
                "        "+Aggregator.TRANSFORMATION_UNTYPED.getName()+": min");
        Entity child1 = Iterables.get(app.getChildren(), 0);
        Entity child2 = Iterables.get(app.getChildren(), 1);
        
        child1.sensors().set(myVal, 3d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 3d);
        
        child2.sensors().set(myVal, 1d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 1d);
        
        child2.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 3d);
    }
    
    @Test
    public void testMax() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "    - type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Aggregator.class.getName(),
                "      brooklyn.config:",
                "        "+Aggregator.SOURCE_SENSOR.getName()+": myVal",
                "        "+Aggregator.TARGET_SENSOR.getName()+": myResult",
                "        "+Aggregator.TRANSFORMATION_UNTYPED.getName()+": max");
        Entity child1 = Iterables.get(app.getChildren(), 0);
        Entity child2 = Iterables.get(app.getChildren(), 1);
        
        child1.sensors().set(myVal, 1d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 1d);
        
        child2.sensors().set(myVal, 3d);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 3d);
        
        child2.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, 1d);
    }
    
    @Test
    public void testList() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "    - type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Aggregator.class.getName(),
                "      brooklyn.config:",
                "        "+Aggregator.SOURCE_SENSOR.getName()+": myVal",
                "        "+Aggregator.TARGET_SENSOR.getName()+": myResult",
                "        "+Aggregator.TRANSFORMATION_UNTYPED.getName()+": list");
        Entity child1 = Iterables.get(app.getChildren(), 0);
        Entity child2 = Iterables.get(app.getChildren(), 1);
        
        child1.sensors().set(myVal, "val1");
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, MutableList.of("val1", null));
        
        child2.sensors().set(myVal, "val2");
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, ImmutableList.of("val1", "val2"));
        
        child1.sensors().set(myVal, "val1b");
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, ImmutableList.of("val1b", "val2"));
        
        child1.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, MutableList.of(null, "val2"));
    }
    
    @Test
    public void testListExcludesBlank() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "    - type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Aggregator.class.getName(),
                "      brooklyn.config:",
                "        "+Aggregator.SOURCE_SENSOR.getName()+": myVal",
                "        "+Aggregator.TARGET_SENSOR.getName()+": myResult",
                "        "+Aggregator.EXCLUDE_BLANK.getName()+": true",
                "        "+Aggregator.TRANSFORMATION_UNTYPED.getName()+": list");
        Entity child1 = Iterables.get(app.getChildren(), 0);
        Entity child2 = Iterables.get(app.getChildren(), 1);

        EntityAsserts.assertAttributeEqualsEventually(app, myResult, ImmutableList.of());

        child1.sensors().set(myVal, "val1");
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, ImmutableList.of("val1"));
        
        child2.sensors().set(myVal, "val2");
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, ImmutableList.of("val1", "val2"));
        
        child1.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, MutableList.of("val2"));
    }
    
    @Test
    public void testFirst() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "    - type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Aggregator.class.getName(),
                "      brooklyn.config:",
                "        "+Aggregator.SOURCE_SENSOR.getName()+": myVal",
                "        "+Aggregator.TARGET_SENSOR.getName()+": myResult",
                "        "+Aggregator.TRANSFORMATION_UNTYPED.getName()+": first",
                "        "+Aggregator.VALUE_FILTER.getName()+": notNull");
        Entity child1 = Iterables.get(app.getChildren(), 0);
        Entity child2 = Iterables.get(app.getChildren(), 1);
        
        child1.sensors().set(myVal, "val1");
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, "val1");
        
        child2.sensors().set(myVal, "val2");
        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", Duration.millis(50)), app, myResult, "val1");
        
        child1.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, "val2");
        
        child2.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, null);
        
        child1.sensors().set(myVal, "val3");
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, "val3");
    }

    @Test
    public void testQuorum() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "    - type: " + TestEntity.class.getName(),
                "  brooklyn.enrichers:",
                "    - type: " + Aggregator.class.getName(),
                "      brooklyn.config:",
                "        "+Aggregator.SOURCE_SENSOR.getName()+": myVal",
                "        "+Aggregator.TARGET_SENSOR.getName()+": myResult",
                "        "+Aggregator.TRANSFORMATION_UNTYPED.getName()+": isQuorate",
                "        "+Aggregator.QUORUM_CHECK_TYPE.getName()+": all",
                "        "+Aggregator.QUORUM_TOTAL_SIZE.getName()+": 2");
        Entity child1 = Iterables.get(app.getChildren(), 0);
        Entity child2 = Iterables.get(app.getChildren(), 1);
        
        child1.sensors().set(myVal, true);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, false);
        
        child2.sensors().set(myVal, true);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, true);
        
        child2.sensors().set(myVal, null);
        EntityAsserts.assertAttributeEqualsEventually(app, myResult, false);
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
