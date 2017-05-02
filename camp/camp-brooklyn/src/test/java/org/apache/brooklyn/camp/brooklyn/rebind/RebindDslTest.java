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
package org.apache.brooklyn.camp.brooklyn.rebind;

import static org.testng.Assert.assertEquals;

import java.io.File;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;

public class RebindDslTest extends AbstractYamlRebindTest {

    // TODO What about testing DslBrooklynObjectConfigSupplier?
    
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(RebindDslTest.class);

    @SuppressWarnings("serial")
    ConfigKey<Sensor<?>> sensorSupplier1 = ConfigKeys.newConfigKey(new TypeToken<Sensor<?>>() {}, "sensorSupplier1");
    
    @SuppressWarnings("serial")
    ConfigKey<String> configSupplier1 = ConfigKeys.newConfigKey(new TypeToken<String>() {}, "configSupplier1");
    
    @SuppressWarnings("serial")
    ConfigKey<String> attributeWhenReady1 = ConfigKeys.newConfigKey(new TypeToken<String>() {}, "attributeWhenReady1");
    
    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
    }
    
    // Difference is that now inside DslSensorSupplier's persisted state we have:
    //   <sensorName class="string">mysensor</sensorName>
    // Whereas previously we just had:
    //   <sensorName>mysensor</sensorName>
    // The code was changed such that the field is now `Object` rather than `String`!
    @Test
    public void testDslSensorSupplier_2016_07() throws Exception {
        String entityId = "klcueb1ide";
        doAddEntityMemento("2016-07", entityId);
        
        rebind();
        Entity newEntity = mgmt().getEntityManager().getEntity(entityId);
        
        Sensor<?> sensor = newEntity.config().get(sensorSupplier1);
        assertEquals(sensor.getName(), "mySensorName");
    }
    
    @Test
    public void testDslConfigSupplier_2016_07() throws Exception {
        String entityId = "klcueb1ide";
        doAddEntityMemento("2016-07", entityId);
        
        rebind();
        Entity newEntity = mgmt().getEntityManager().getEntity(entityId);
        
        String val = newEntity.config().get(configSupplier1);
        assertEquals(val, "myval");
    }
    
    @Test
    public void testDslAttributeWhenReady_2016_07() throws Exception {
        String entityId = "klcueb1ide";
        doAddEntityMemento("2016-07", entityId);
        
        rebind();
        Entity newEntity = mgmt().getEntityManager().getEntity(entityId);
        
        String val = newEntity.config().get(attributeWhenReady1);
        assertEquals(val, "myval");
    }
    
    @Test
    public void testDslSensorSupplier() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    " + sensorSupplier1.getName() + ": $brooklyn:sensor(\"mySensorName\")");
        
        rebind();
        Entity newEntity = mgmt().getEntityManager().getEntity(app.getId());
        
        Sensor<?> sensor = newEntity.config().get(sensorSupplier1);
        assertEquals(sensor.getName(), "mySensorName");
    }

    @Test
    public void testDslConfigSupplier() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    myConfigKeyName: myval",
                "    " + configSupplier1.getName() + ": $brooklyn:config(\"myConfigKeyName\")");
        
        rebind();
        Entity newEntity = mgmt().getEntityManager().getEntity(app.getId());
        
        String val = newEntity.config().get(configSupplier1);
        assertEquals(val, "myval");
    }

    @Test
    public void testDslConfigSupplierNested() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    keyName: myConfigKeyName",
                "    myConfigKeyName: myval",
                "    " + configSupplier1.getName() + ": $brooklyn:config($brooklyn:config(\"keyName\"))");
        
        rebind();
        Entity newEntity = mgmt().getEntityManager().getEntity(app.getId());
        
        String val = newEntity.config().get(configSupplier1);
        assertEquals(val, "myval");
    }

    @Test
    public void testDslAttributeSupplier() throws Exception {
        final Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.initializers:",
                "  - type: " + StaticSensor.class.getName(),
                "    brooklyn.config:",
                "      name: mySensorName",
                "      static.value: myval",
                "  brooklyn.config:",
                "    " + attributeWhenReady1.getName() + ": $brooklyn:attributeWhenReady(\"mySensorName\")");
        
        rebind();
        Entity newEntity = mgmt().getEntityManager().getEntity(app.getId());
        
        String val = newEntity.config().get(attributeWhenReady1);
        assertEquals(val, "myval");
    }

    protected void doAddEntityMemento(String label, String entityId) throws Exception {
        String mementoResourceName = "dsl-" + label + "-entity-" + entityId;
        String memento = Streams.readFullyString(getClass().getResourceAsStream(mementoResourceName));
        
        File persistedEntityFile = new File(mementoDir, Os.mergePaths("entities", entityId));
        Files.write(memento.getBytes(), persistedEntityFile);
    }
}
