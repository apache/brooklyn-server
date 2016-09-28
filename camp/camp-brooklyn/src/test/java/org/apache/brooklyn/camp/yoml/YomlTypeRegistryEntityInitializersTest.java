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
package org.apache.brooklyn.camp.yoml;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.yoml.types.YomlInitializers;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.brooklyn.util.yoml.tests.YomlTestFixture;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class YomlTypeRegistryEntityInitializersTest extends AbstractYamlTest {

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        // TODO logically how should we populate the catalog? see notes on the method called below
        YomlInitializers.install(mgmt());
    }
    
    StaticSensor<?> SS_42 = new StaticSensor<Object>(ConfigBag.newInstance()
        .configure(StaticSensor.SENSOR_NAME, "the-answer")
        .configure(StaticSensor.SENSOR_TYPE, "int")
        .configure(StaticSensor.STATIC_VALUE, 42) );

    // permitted anytime
    final static String SS_42_YAML_SIMPLE = Joiner.on("\n").join(
        "name: the-answer",
        "type: static-sensor",
        "sensor-type: int",
        "value: 42");

    // permitted if we know we are reading EntityInitializer instances
    final String SS_42_YAML_SINGLETON_MAP = Joiner.on("\n").join(
        "the-answer:",
        "  type: static-sensor",
        "  sensor-type: int",
        "  value: 42");

    @Test
    public void testYomlReadSensor() throws Exception {
        String yaml = Joiner.on("\n").join(
                "name: the-answer",
                "type: static-sensor",
                "sensor-type: int",
                "value: 42");

        Object ss = mgmt().getTypeRegistry().createBeanFromPlan("yoml", Yamls.parseAll(yaml).iterator().next(), null, null);
        Asserts.assertInstanceOf(ss, StaticSensor.class);
        // Assert.assertEquals(ss, SS_42);  // class does not support equals
    }

    @Test
    public void testYomlReadSensorWithExpectedSuperType() throws Exception {
        Object ss = mgmt().getTypeRegistry().createBeanFromPlan("yoml", Yamls.parseAll(SS_42_YAML_SIMPLE).iterator().next(), null, EntityInitializer.class);
        Asserts.assertInstanceOf(ss, StaticSensor.class);
        // Assert.assertEquals(ss, SS_42);  // class does not support equals
    }
    
    @Test
    public void testReadSensorAsMapWithName() throws Exception {
        Object ss = mgmt().getTypeRegistry().createBeanFromPlan("yoml", Yamls.parseAll(SS_42_YAML_SINGLETON_MAP).iterator().next(), null, EntityInitializer.class);
        Asserts.assertInstanceOf(ss, StaticSensor.class);
    }

    @Test
    public void testYomlReadSensorSingletonMapWithFixture() throws Exception {
        YomlTestFixture y = BrooklynYomlTestFixture.newInstance(mgmt());
        y.read(SS_42_YAML_SINGLETON_MAP, "entity-initializer");
        Asserts.assertInstanceOf(y.getLastReadResult(), StaticSensor.class);
    }
    
    @Test
    public void testYomlWriteSensorWithFixture() throws Exception {
        YomlTestFixture y = BrooklynYomlTestFixture.newInstance(mgmt());
        y.write(SS_42, "entity-initializer").assertLastWriteIgnoringQuotes(
            "{the-answer: { type: static-sensor:0.10.0-SNAPSHOT, period: 5m, targetType: int, timeout: a very long time, value: 42}}"
            // ideally it would be simple like below but it sets all the values so it ends up looking like the above
//            Jsonya.newInstance().add( Yamls.parseAll(SS_42_YAML_SINGLETON_MAP).iterator().next() ).toString() 
            );
        
        // and another read/write cycle gives the same thing
        Object fullOutput = y.getLastWriteResult();
        y.readLastWrite().writeLastRead();
        Assert.assertEquals(fullOutput, y.getLastWriteResult());
    }

    // and test in context
    
    @Test(enabled=false) // this format (list) still runs old camp parse, does not attempt yaml, included for comparison
    public void testStaticSensorWorksAsList() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.initializers:",
                "    - name: the-answer",
                "      type: static-sensor",
                "      sensor-type: int",
                "      value: 42");

        checkStaticSensorInApp(yaml);
    }

    @Test
    public void testStaticSensorWorksAsSingletonMap() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.initializers:",
                "    the-answer:",
                "      type: static-sensor",
                "      sensor-type: int",
                "      value: 42");

        checkStaticSensorInApp(yaml);
    }

    protected void checkStaticSensorInApp(String yaml)
            throws Exception, InterruptedException, ExecutionException, TimeoutException {
        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        Assert.assertEquals(
            entity.getExecutionContext().submit( 
                DependentConfiguration.attributeWhenReady(entity, Sensors.newIntegerSensor("the-answer")) )
                .get( Duration.FIVE_SECONDS ), (Integer) 42);
    }

}
