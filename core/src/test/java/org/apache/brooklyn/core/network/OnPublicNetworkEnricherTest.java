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
package org.apache.brooklyn.core.network;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

public class OnPublicNetworkEnricherTest extends BrooklynAppUnitTestSupport {

    private static final Duration VERY_SHORT_WAIT = Duration.millis(100);
    
    private TestEntity entity;
    private SshMachineLocation machine;
    private PortForwardManager portForwardManager;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        machine = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", "127.0.0.1"));
        portForwardManager = (PortForwardManager) mgmt.getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
    }
    
    @DataProvider(name = "variants")
    public Object[][] provideVariants() {
        AttributeSensor<HostAndPort> hostAndPortSensor = Sensors.newSensor(HostAndPort.class, "test.endpoint");
        List<Object[]> result = Lists.newArrayList();
        for (Timing setSensor : Timing.values()) {
            for (Timing createAssociation : Timing.values()) {
                for (Timing addLocation : Timing.values()) {
                    result.add(new Object[] {setSensor, createAssociation, addLocation, Attributes.MAIN_URI, 
                            URI.create("http://127.0.0.1:1234/my/path"), "main.uri.mapped.public", "http://mypublichost:5678/my/path"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, TestEntity.NAME, 
                            "http://127.0.0.1:1234/my/path", "test.name.mapped.public", "http://mypublichost:5678/my/path"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, Attributes.HTTP_PORT, 
                            1234, "http.endpoint.mapped.public", "mypublichost:5678"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, TestEntity.NAME, 
                            "1234", "test.name.mapped.public", "mypublichost:5678"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, TestEntity.NAME, 
                            "127.0.0.1:1234", "test.name.mapped.public", "mypublichost:5678"});
                    result.add(new Object[] {setSensor, createAssociation, addLocation, hostAndPortSensor, 
                            HostAndPort.fromString("127.0.0.1:1234"), "test.endpoint.mapped.public", "mypublichost:5678"});
                }
            }
        }
        return result.toArray(new Object[result.size()][]);
    }
    
    @DataProvider(name = "invalidVariants")
    public Object[][] provideInvalidVariants() {
        AttributeSensor<HostAndPort> hostAndPortSensor = Sensors.newSensor(HostAndPort.class, "test.hostAndPort");
        List<Object[]> result = Lists.newArrayList();
        result.add(new Object[] {Attributes.MAIN_URI, (URI)null});
        result.add(new Object[] {TestEntity.NAME, "127.0.0.1:1234/my/path"}); // must have scheme
        result.add(new Object[] {Attributes.HTTP_PORT, null});
        result.add(new Object[] {Attributes.HTTP_PORT, 1234567});
        result.add(new Object[] {TestEntity.NAME, null});
        result.add(new Object[] {TestEntity.NAME, "1234567"});
        result.add(new Object[] {TestEntity.NAME, "thisHasNoPort"});
        result.add(new Object[] {TestEntity.NAME, "portIsTooBig:1234567"});
        result.add(new Object[] {hostAndPortSensor, (HostAndPort)null});
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
    public <T> void testSensorTransformed(Timing setUri, Timing createAssociation, Timing addLocation, 
            AttributeSensor<T> sensor, T sensorVal, String targetSensorName, String expectedVal) throws Exception {
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
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(sensor)));

        if (setUri == Timing.AFTER) {
            entity.sensors().set(sensor, sensorVal);
        }
        if (createAssociation == Timing.AFTER) {
            portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        }
        if (addLocation == Timing.AFTER) {
            entity.addLocations(ImmutableList.of(machine));
        }
        
        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor(targetSensorName), expectedVal);
        EntityAsserts.assertAttributeEquals(entity, sensor, sensorVal);
    }
    
    
    @Test(dataProvider = "invalidVariants")
    public <T> void testIgnoresWhenInvalidAttribute(AttributeSensor<T> sensor, T sensorVal) throws Exception {
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(sensor, sensorVal);
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(sensor)));

        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), entity, Sensors.newStringSensor(sensor.getName()+".mapped.public"), null);
    }

    @Test
    public <T> void testTransformsDefaultHttp80() throws Exception {
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(Attributes.MAIN_URI, URI.create("http://127.0.0.1/my/path"));
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 80);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(Attributes.MAIN_URI)));

        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor(Attributes.MAIN_URI.getName()+".mapped.public"), "http://mypublichost:5678/my/path");
    }

    @Test
    public <T> void testTransformsDefaultHttps443() throws Exception {
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(Attributes.MAIN_URI, URI.create("https://127.0.0.1/my/path"));
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 443);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(Attributes.MAIN_URI)));

        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor(Attributes.MAIN_URI.getName()+".mapped.public"), "https://mypublichost:5678/my/path");
    }

    @Test
    public void testIgnoresWhenNoMapping() throws Exception {
        // Creates associate for a non-matching port
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(Attributes.HTTP_PORT, 1234);
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 4321);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(Attributes.HTTP_PORT)));

        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), entity, Sensors.newStringSensor(Attributes.HTTP_PORT.getName()+".mapped.public"), null);
    }
    
    @Test
    public void testIgnoresWhenNoMachine() throws Exception {
        // Does not call entity.addLocation
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(Attributes.HTTP_PORT, 1234);
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(Attributes.HTTP_PORT)));

        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), entity, Sensors.newStringSensor(Attributes.HTTP_PORT.getName()+".mapped.public"), null);
    }
    
    @Test
    public void testIgnoresWithNoPort() throws Exception {
        // Sensor value does not have any port number
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(TestEntity.NAME, "myval");
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(TestEntity.NAME)));

        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), entity, Sensors.newStringSensor(TestEntity.NAME.getName()+".mapped.public"), null);
    }
    
    @Test
    public <T> void testTransformsAllMatchingSensors() throws Exception {
        AttributeSensor<URI> stronglyTypedUri = Sensors.newSensor(URI.class, "strongly.typed.uri");
        AttributeSensor<String> stringUri = Sensors.newStringSensor("string.uri");
        AttributeSensor<URL> stronglyTypedUrl = Sensors.newSensor(URL.class, "strongly.typed.url");
        AttributeSensor<String> stringUrl = Sensors.newStringSensor("string.url");
        AttributeSensor<Integer> intPort = Sensors.newIntegerSensor("int.port");
        AttributeSensor<String> stringPort = Sensors.newStringSensor("string.port");
        AttributeSensor<Integer> portNoPrefix = Sensors.newIntegerSensor("port");
        AttributeSensor<HostAndPort> endpoint = Sensors.newSensor(HostAndPort.class, "hostAndPort.endpoint");
        AttributeSensor<String> stringEndpoint = Sensors.newStringSensor("string.hostAndPort.endpoint");
        AttributeSensor<HostAndPort> hostAndPort = Sensors.newSensor(HostAndPort.class, "example.hostAndPort");
        AttributeSensor<String> stringHostAndPort = Sensors.newStringSensor("string.hostAndPort");

        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(stronglyTypedUri, URI.create("http://127.0.0.1:1234/my/path"));
        entity.sensors().set(stringUri, "http://127.0.0.1:1234/my/path");
        entity.sensors().set(stronglyTypedUrl, new URL("http://127.0.0.1:1234/my/path"));
        entity.sensors().set(stringUrl, "http://127.0.0.1:1234/my/path");
        entity.sensors().set(intPort, 1234);
        entity.sensors().set(stringPort, "1234");
        entity.sensors().set(portNoPrefix, 1234);
        entity.sensors().set(endpoint, HostAndPort.fromParts("127.0.0.1", 1234));
        entity.sensors().set(stringEndpoint, "127.0.0.1:1234");
        entity.sensors().set(hostAndPort, HostAndPort.fromParts("127.0.0.1", 1234));
        entity.sensors().set(stringHostAndPort, "127.0.0.1:1234");
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        entity.addLocations(ImmutableList.of(machine));

        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class));

        assertAttributeEqualsEventually("strongly.typed.uri.mapped.public", "http://mypublichost:5678/my/path");
        assertAttributeEqualsEventually("string.uri.mapped.public", "http://mypublichost:5678/my/path");
        assertAttributeEqualsEventually("strongly.typed.url.mapped.public", "http://mypublichost:5678/my/path");
        assertAttributeEqualsEventually("string.url.mapped.public", "http://mypublichost:5678/my/path");
        assertAttributeEqualsEventually("int.endpoint.mapped.public", "mypublichost:5678");
        assertAttributeEqualsEventually("string.endpoint.mapped.public", "mypublichost:5678");
        assertAttributeEqualsEventually("endpoint.mapped.public", "mypublichost:5678");
        assertAttributeEqualsEventually("hostAndPort.endpoint.mapped.public", "mypublichost:5678");
        assertAttributeEqualsEventually("string.hostAndPort.endpoint.mapped.public", "mypublichost:5678");
        assertAttributeEqualsEventually("example.hostAndPort.mapped.public", "mypublichost:5678");
        assertAttributeEqualsEventually("string.hostAndPort.mapped.public", "mypublichost:5678");
    }
    
    @Test
    public <T> void testIgnoresNonMatchingSensors() throws Exception {
        AttributeSensor<URI> sensor1 = Sensors.newSensor(URI.class, "my.different");
        AttributeSensor<URL> sensor2 = Sensors.newSensor(URL.class, "my.different2");
        AttributeSensor<String> sensor3 = Sensors.newStringSensor("my.different3");
        AttributeSensor<Integer> sensor4 = Sensors.newIntegerSensor("my.different4");
        AttributeSensor<HostAndPort> sensor5 = Sensors.newSensor(HostAndPort.class, "my.different5");

        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(sensor1, URI.create("http://127.0.0.1:1234/my/path"));
        entity.sensors().set(sensor2, new URL("http://127.0.0.1:1234/my/path"));
        entity.sensors().set(sensor3, "http://127.0.0.1:1234/my/path");
        entity.sensors().set(sensor4, 1234);
        entity.sensors().set(sensor5, HostAndPort.fromParts("127.0.0.1", 1234));
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class));

        Asserts.succeedsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), new Runnable() {
            @Override public void run() {
                Map<AttributeSensor<?>, Object> allSensors = entity.sensors().getAll();
                String errMsg = "sensors="+allSensors;
                for (AttributeSensor<?> sensor : allSensors.keySet()) {
                    String name = sensor.getName();
                    assertFalse(name.startsWith("my.different") && sensor.getName().contains("public"), errMsg);
                }
            }});
    }

    @Test
    public <T> void testDoesNotDoRegexMatchingWhenSensorsSpecified() throws Exception {
        AttributeSensor<String> sensor = Sensors.newStringSensor("mysensor");
        AttributeSensor<Integer> intPort = Sensors.newIntegerSensor("int.port");

        entity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        entity.sensors().set(intPort, 1234);
        entity.sensors().set(sensor, "127.0.0.1:1234");
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        entity.addLocations(ImmutableList.of(machine));
        
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(sensor)));

        assertAttributeEqualsEventually("mysensor.mapped.public", "mypublichost:5678");
        assertAttributeEqualsContinually("int.endpoint.mapped.public", null, VERY_SHORT_WAIT);
    }
    
    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> void testCoercesSensorName() throws Exception {
        AttributeSensor<String> sensor = Sensors.newStringSensor("mysensor");

        entity.sensors().set(sensor, "127.0.0.1:1234");
        portForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), machine, 1234);
        entity.addLocations(ImmutableList.of(machine));
        
        // Ugly casting in java, but easy to get passed this when constructed from YAML
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ((List<AttributeSensor<?>>)(List)ImmutableList.of("mysensor"))));

        assertAttributeEqualsEventually("mysensor.mapped.public", "mypublichost:5678");
    }

    @Test
    public void testDefaultMapMatching() throws Exception {
        OnPublicNetworkEnricher enricher = entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class));
        String regex = enricher.getConfig(OnPublicNetworkEnricher.MAP_MATCHING);
        
        Map<String, Boolean> testCases = ImmutableMap.<String, Boolean>builder()
                .put("my.uri", true)
                .put("my.UrI", true)
                .put("my.url", true)
                .put("my.endpoint", true)
                .put("my.port", true)
                .put("port", true)
                .put("uri", true)
                .put("PREFIX_NO_DOTuri", false)
                .put("PREFIX_NO_DOTendpoint", false)
                .put("PREFIX_NO_DOTport", false)
                .put("portSUFFIX", false)
                .build();
        
        for (Map.Entry<String, Boolean> entry : testCases.entrySet()) {
            assertEquals(Boolean.valueOf(entry.getKey().matches(regex)), entry.getValue(), "input="+entry.getKey());
        }
    }
    
    @Test
    public void testSensorNameConverter() throws Exception {
        OnPublicNetworkEnricher enricher = entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class));
        Function<? super String, String> converter = enricher.getConfig(OnPublicNetworkEnricher.SENSOR_NAME_CONVERTER);
        
        Map<String, String> testCases = ImmutableMap.<String, String>builder()
                .put("my.uri", "my.uri.mapped.public")
                .put("myuri", "myuri.mapped.public")
                .put("my.UrI", "my.UrI.mapped.public")
                .put("my.url", "my.url.mapped.public")
                .put("myurl", "myurl.mapped.public")
                .put("my.endpoint", "my.endpoint.mapped.public")
                .put("myendpoint", "myendpoint.mapped.public")
                .put("my.port", "my.endpoint.mapped.public")
                .put("myport", "my.endpoint.mapped.public")
                .put("port", "endpoint.mapped.public")
                .put("uri", "uri.mapped.public")
                .build();
        
        for (Map.Entry<String, String> entry : testCases.entrySet()) {
            assertEquals(converter.apply(entry.getKey()), entry.getValue(), "input="+entry.getKey());
        }
    }
    
    @Test(expectedExceptions=IllegalStateException.class, expectedExceptionsMessageRegExp=".*must not have explicit 'mapMatching' and 'sensors'.*")
    public void testFailsIfSensorsAndMapMatchingConfigured() throws Exception {
        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(Attributes.HTTPS_PORT))
                .configure(OnPublicNetworkEnricher.MAP_MATCHING, ".*uri"));
    }
    
    protected void assertAttributeEqualsEventually(String sensorName, String expectedVal) throws Exception {
        try {
            EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor(sensorName), expectedVal);
        } catch (Exception e) {
            throw new Exception("Failed assertion for sensor '"+sensorName+"'; attributes are "+entity.sensors().getAll(), e);
        }
    }
    
    protected void assertAttributeEqualsContinually(String sensorName, String expectedVal, Duration duration) throws Exception {
        try {
            EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", duration), entity, Sensors.newStringSensor(sensorName), expectedVal);
        } catch (Exception e) {
            throw new Exception("Failed assertion for sensor '"+sensorName+"'; attributes are "+entity.sensors().getAll(), e);
        }
    }
}

