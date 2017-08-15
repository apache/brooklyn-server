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
package org.apache.brooklyn.entity.java;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.http.HttpRequestSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.software.base.test.jmx.GeneralisedDynamicMBean;
import org.apache.brooklyn.entity.software.base.test.jmx.JmxService;
import org.apache.brooklyn.feed.jmx.JmxFeedTest.TestEntityWithJmx;
import org.apache.brooklyn.feed.jmx.JmxHelper;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JmxAttributeSensorTest extends BrooklynAppUnitTestSupport {
    final static AttributeSensor<String> SENSOR_STRING = Sensors.newStringSensor("aString");
    final static AttributeSensor<Object> SENSOR_OBJ = Sensors.newSensor(Object.class, "anObj");
    final static String STRING_TARGET_TYPE = "java.lang.String";
    final static String OBJECT_TARGET_TYPE = "java.lang.Object";

    private TestEntity entity;

    private JmxService jmxService;
    private JmxHelper jmxHelper;
    
    private String objectName = "Brooklyn:type=MyTestMBean,name=myname";
    private String attributeName = "myattrib";

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        // Create an entity and configure it with the above JMX service
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class).impl(TestEntityWithJmx.class).additionalInterfaces(UsesJmx.class));
        app.start(ImmutableList.of(new SimulatedLocation()));

        jmxHelper = new JmxHelper(entity);
        jmxService = new JmxService(entity);
    }
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        try {
            if (jmxHelper != null) jmxHelper.disconnect();
            if (jmxService != null) jmxService.shutdown();
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testSensor() throws Exception {
        GeneralisedDynamicMBean mbean = jmxService.registerMBean(ImmutableMap.of(attributeName, 42), objectName);

        JmxAttributeSensor<Integer> sensor = new JmxAttributeSensor<Integer>(ConfigBag.newInstance()
                .configure(JmxAttributeSensor.SENSOR_PERIOD, Duration.millis(10))
                .configure(JmxAttributeSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(JmxAttributeSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(JmxAttributeSensor.OBJECT_NAME, objectName)
                .configure(JmxAttributeSensor.ATTRIBUTE, attributeName));
        sensor.apply(entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "42");
        
        // Change the value and check it updates
        mbean.updateAttributeValue(attributeName, 64);
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "64");
    }

    // "Integration" because takes a second 
    @Test(groups="Integration")
    public void testSensorSupressingDuplicates() throws Exception {
        GeneralisedDynamicMBean mbean = jmxService.registerMBean(ImmutableMap.of(attributeName, 42), objectName);

        RecordingSensorEventListener<String> listener = new RecordingSensorEventListener<>();
        entity.subscriptions().subscribe(entity, SENSOR_STRING, listener);
        
        JmxAttributeSensor<Integer> sensor = new JmxAttributeSensor<Integer>(ConfigBag.newInstance()
                .configure(HttpRequestSensor.SUPPRESS_DUPLICATES, true)
                .configure(JmxAttributeSensor.SENSOR_PERIOD, Duration.millis(10))
                .configure(JmxAttributeSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(JmxAttributeSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(JmxAttributeSensor.OBJECT_NAME, objectName)
                .configure(JmxAttributeSensor.ATTRIBUTE, attributeName));
        sensor.apply(entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "42");
        listener.assertHasEventEventually(Predicates.alwaysTrue());
        
        Asserts.succeedsContinually(new Runnable() {
            @Override public void run() {
                listener.assertEventCount(1);
            }});
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "42");
        
        // Change the value and check it updates
        mbean.updateAttributeValue(attributeName, 64);
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "64");
    }
}
