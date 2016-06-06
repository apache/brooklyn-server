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
package org.apache.brooklyn.core.location.access;

import java.net.URI;
import java.util.List;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

public class PublicNetworkFaceEnricherTest extends BrooklynAppUnitTestSupport {

    private static final Duration VERY_SHORT_WAIT = Duration.millis(100);
    
    private TestEntity entity;
    private SshMachineLocation machine;
    private PortForwardManager portForwardManager;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        machine = app.newLocalhostProvisioningLocation().obtain();
        portForwardManager = (PortForwardManager) mgmt.getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
    }
    
    @DataProvider(name = "variants")
    public static Object[][] provideVariants() {
        AttributeSensor<HostAndPort> hostAndPortSensor = Sensors.newSensor(HostAndPort.class, "test.hostAndPort");
        List<Object[]> result = Lists.newArrayList();
        for (Timing setSensor : Timing.values()) {
            for (Timing createAssociation : Timing.values()) {
                for (Timing addLocation : Timing.values()) {
                    result.add(new Object[] {setSensor, createAssociation, addLocation, Attributes.MAIN_URI, URI.create("http://127.0.0.1:1234/my/path"), "http://mypublichost:5678/my/path"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, TestEntity.NAME, "http://127.0.0.1:1234/my/path", "http://mypublichost:5678/my/path"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, Attributes.HTTP_PORT, 1234, "mypublichost:5678"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, TestEntity.NAME, "127.0.0.1:1234", "mypublichost:5678"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, hostAndPortSensor, HostAndPort.fromString("127.0.0.1:1234"), "mypublichost:5678"});
                }
            }
        }
        return result.toArray(new Object[result.size()][]);
    }
    enum Timing {
        BEFORE,
        AFTER;
    }

    /**
     * The sensorVal must include port 1234, so that it will be converted to mypublichost:5678
     */
    @Test(dataProvider = "variants")
    public <T> void testSensorTransformed(Timing setUri, Timing createAssociation, Timing addLocation, AttributeSensor<T> sensor, T sensorVal, String expectedVal) throws Exception {
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        if (setUri == Timing.BEFORE) {
            entity.sensors().set(sensor, sensorVal);
        }
        if (createAssociation == Timing.BEFORE) {
            portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        }
        if (addLocation == Timing.BEFORE) {
            entity.addLocations(ImmutableList.of(machine));
        }
        
        entity.enrichers().add(EnricherSpec.create(PublicNetworkFaceEnricher.class)
                .configure(PublicNetworkFaceEnricher.SENSOR, sensor));

        if (setUri == Timing.AFTER) {
            entity.sensors().set(sensor, sensorVal);
        }
        if (createAssociation == Timing.AFTER) {
            portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        }
        if (addLocation == Timing.AFTER) {
            entity.addLocations(ImmutableList.of(machine));
        }
        
        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor(sensor.getName()+".mapped.public"), expectedVal);
        EntityAsserts.assertAttributeEquals(entity, sensor, sensorVal);
    }
    
    @Test
    public <T> void testTransformsDefaultHttp80() throws Exception {
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(Attributes.MAIN_URI, URI.create("http://127.0.0.1/my/path"));
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 80);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(PublicNetworkFaceEnricher.class)
                .configure(PublicNetworkFaceEnricher.SENSOR, Attributes.MAIN_URI));

        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor(Attributes.MAIN_URI.getName()+".mapped.public"), "http://mypublichost:5678/my/path");
    }

    @Test
    public <T> void testTransformsDefaultHttps443() throws Exception {
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(Attributes.MAIN_URI, URI.create("https://127.0.0.1/my/path"));
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 443);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(PublicNetworkFaceEnricher.class)
                .configure(PublicNetworkFaceEnricher.SENSOR, Attributes.MAIN_URI));

        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor(Attributes.MAIN_URI.getName()+".mapped.public"), "https://mypublichost:5678/my/path");
    }

    @Test
    public void testIgnoresWhenNoMapping() throws Exception {
        // Creates associate for a non-matching port
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(Attributes.HTTP_PORT, 1234);
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 4321);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(PublicNetworkFaceEnricher.class)
                .configure(PublicNetworkFaceEnricher.SENSOR, Attributes.HTTP_PORT));

        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), entity, Sensors.newStringSensor(Attributes.HTTP_PORT.getName()+".mapped.public"), null);
    }
    
    @Test
    public void testIgnoresWhenNoMachine() throws Exception {
        // Does not call entity.addLocation
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(Attributes.HTTP_PORT, 1234);
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        
        entity.enrichers().add(EnricherSpec.create(PublicNetworkFaceEnricher.class)
                .configure(PublicNetworkFaceEnricher.SENSOR, Attributes.HTTP_PORT));

        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), entity, Sensors.newStringSensor(Attributes.HTTP_PORT.getName()+".mapped.public"), null);
    }
    
    @Test
    public void testIgnoresWithNoPort() throws Exception {
        // Sensor value does not have any port number
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(TestEntity.NAME, "myval");
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        
        entity.enrichers().add(EnricherSpec.create(PublicNetworkFaceEnricher.class)
                .configure(PublicNetworkFaceEnricher.SENSOR, TestEntity.NAME));

        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), entity, Sensors.newStringSensor(TestEntity.NAME.getName()+".mapped.public"), null);
    }
}
