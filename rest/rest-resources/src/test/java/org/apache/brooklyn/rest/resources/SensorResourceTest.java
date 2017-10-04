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
package org.apache.brooklyn.rest.resources;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.core.config.render.RendererHints;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.policy.TestEnricher;
import org.apache.brooklyn.core.test.policy.TestPolicy;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.rest.api.SensorApi;
import org.apache.brooklyn.rest.domain.ApplicationSpec;
import org.apache.brooklyn.rest.domain.EntitySpec;
import org.apache.brooklyn.rest.test.config.render.TestRendererHints;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.rest.testing.mocks.RestMockSimpleEntity;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.StringFunctions;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.cxf.jaxrs.client.WebClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Callables;

/**
 * Test the {@link SensorApi} implementation.
 * <p>
 * Check that {@link SensorResource} correctly renders {@link AttributeSensor}
 * values, including {@link RendererHints.DisplayValue} hints.
 */
@Test(singleThreaded = true,
        // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "SensorResourceTest")
public class SensorResourceTest extends BrooklynRestResourceTest {

    final static ApplicationSpec SIMPLE_SPEC = ApplicationSpec.builder()
            .name("simple-app")
            .entities(ImmutableSet.of(new EntitySpec("simple-ent", RestMockSimpleEntity.class.getName())))
            .locations(ImmutableSet.of("localhost"))
            .build();

    static final String SENSORS_ENDPOINT = "/applications/simple-app/entities/simple-ent/sensors";
    static final String SENSOR_NAME = "amphibian.count";
    static final AttributeSensor<Integer> SENSOR = Sensors.newIntegerSensor(SENSOR_NAME);

    EntityInternal entity;

    /**
     * Sets up the application and entity.
     * <p>
     * Adds a sensor and sets its value to {@code 12345}. Configures a display value
     * hint that appends {@code frogs} to the value of the sensor.
     */
    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        // Deploy application
        startServer();
        Response deploy = clientDeploy(SIMPLE_SPEC);
        waitForApplicationToBeRunning(deploy.getLocation());

        entity = (EntityInternal) Iterables.find(getManagementContext().getEntityManager().getEntities(), EntityPredicates.displayNameEqualTo("simple-ent"));
        addAmphibianSensor(entity);
    }

    static void addAmphibianSensor(EntityInternal entity) {
        // Add new sensor
        entity.getMutableEntityType().addSensor(SENSOR);
        entity.sensors().set(SENSOR, 12345);

        // Register display value hint
        RendererHints.register(SENSOR, RendererHints.displayValue(Functions.compose(StringFunctions.append(" frogs"), Functions.toStringFunction())));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        TestRendererHints.clearRegistry();
    }

    /** Check default is to use display value hint. */
    @Test
    public void testBatchSensorRead() throws Exception {
        Response response = client().path(SENSORS_ENDPOINT + "/current-state")
                .accept(MediaType.APPLICATION_JSON)
                .get();
        Map<String, ?> currentState = response.readEntity(new GenericType<Map<String,?>>(Map.class) {});

        for (String sensor : currentState.keySet()) {
            if (sensor.equals(SENSOR_NAME)) {
                assertEquals(currentState.get(sensor), "12345 frogs");
            }
        }
    }

    /** Check setting {@code raw} to {@code true} ignores display value hint. */
    @Test(dependsOnMethods = "testBatchSensorRead")
    public void testBatchSensorReadRaw() throws Exception {
        Response response = client().path(SENSORS_ENDPOINT + "/current-state")
                .query("raw", "true")
                .accept(MediaType.APPLICATION_JSON)
                .get();
        Map<String, ?> currentState = response.readEntity(new GenericType<Map<String,?>>(Map.class) {});

        for (String sensor : currentState.keySet()) {
            if (sensor.equals(SENSOR_NAME)) {
                assertEquals(currentState.get(sensor), Integer.valueOf(12345));
            }
        }
    }

    protected Response doSensorTest(Boolean raw, MediaType acceptsType, Object expectedValue) {
        return doSensorTestUntyped(
            raw==null ? null : (""+raw).toLowerCase(), 
            acceptsType==null ? null : new String[] { acceptsType.toString() },
            expectedValue);
    }
    protected Response doSensorTestUntyped(String raw, String[] acceptsTypes, Object expectedValue) {
        WebClient req = client().path(SENSORS_ENDPOINT + "/" + SENSOR_NAME);
        if (raw!=null) req = req.query("raw", raw);
        Response response;
        if (acceptsTypes!=null) {
            response = req.accept(acceptsTypes).get();
        } else {
            response = req.get();
        }
        if (expectedValue!=null) {
            HttpAsserts.assertHealthyStatusCode(response.getStatus());
            Object value = response.readEntity(expectedValue.getClass());
            assertEquals(value, expectedValue);
        }
        return response;
    }
    
    /**
     * Check we can get a sensor, explicitly requesting json; gives a string picking up the rendering hint.
     * 
     * If no "Accepts" header is given, then we don't control whether json or plain text comes back.
     * It is dependent on the method order, which is compiler-specific.
     */
    @Test
    public void testGetJson() throws Exception {
        doSensorTest(null, MediaType.APPLICATION_JSON_TYPE, "\"12345 frogs\"");
    }
    
    @Test
    public void testGetJsonBytes() throws Exception {
        Response response = doSensorTest(null, MediaType.APPLICATION_JSON_TYPE, null);
        byte[] bytes = Streams.readFullyAndClose(response.readEntity(InputStream.class));
        // assert we have one set of surrounding quotes
        assertEquals(bytes.length, 13);
    }

    /** Check that plain returns a string without quotes, with the rendering hint */
    @Test
    public void testGetPlain() throws Exception {
        doSensorTest(null, MediaType.TEXT_PLAIN_TYPE, "12345 frogs");
    }

    /** 
     * Check that when we set {@code raw = true}, the result ignores the display value hint.
     *
     * If no "Accepts" header is given, then we don't control whether json or plain text comes back.
     * It is dependent on the method order, which is compiler-specific.
     */
    @Test
    public void testGetRawJson() throws Exception {
        doSensorTest(true, MediaType.APPLICATION_JSON_TYPE, 12345);
    }
    
    /** As {@link #testGetRaw()} but with plain set, returns the number */
    @Test
    public void testGetPlainRaw() throws Exception {
        // have to pass a string because that's how PLAIN is deserialized
        doSensorTest(true, MediaType.TEXT_PLAIN_TYPE, "12345");
    }

    /** Check explicitly setting {@code raw} to {@code false} is as before */
    @Test
    public void testGetPlainRawFalse() throws Exception {
        doSensorTest(false, MediaType.TEXT_PLAIN_TYPE, "12345 frogs");
    }

    /** Check empty vaue for {@code raw} will revert to using default. */
    @Test
    public void testGetPlainRawEmpty() throws Exception {
        doSensorTestUntyped("", new String[] { MediaType.TEXT_PLAIN }, "12345 frogs");
    }

    /** Check unparseable vaue for {@code raw} will revert to using default. */
    @Test
    public void testGetPlainRawError() throws Exception {
        doSensorTestUntyped("biscuits", new String[] { MediaType.TEXT_PLAIN }, "12345 frogs");
    }
    
    /** Check we can set a value */
    @Test
    public void testSet() throws Exception {
        try {
            Response response = client().path(SENSORS_ENDPOINT + "/" + SENSOR_NAME)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post(67890);
            assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

            assertEquals(entity.getAttribute(SENSOR), (Integer)67890);
            
            String value = client().path(SENSORS_ENDPOINT + "/" + SENSOR_NAME).accept(MediaType.TEXT_PLAIN_TYPE).get(String.class);
            assertEquals(value, "67890 frogs");

        } finally { addAmphibianSensor(entity); }
    }

    @Test
    public void testSetFromMap() throws Exception {
        try {
            Response response = client().path(SENSORS_ENDPOINT)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post(MutableMap.of(SENSOR_NAME, 67890));
            assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
            
            assertEquals(entity.getAttribute(SENSOR), (Integer)67890);

        } finally { addAmphibianSensor(entity); }
    }
    
    /** Check we can delete a value */
    @Test
    public void testDelete() throws Exception {
        try {
            Response response = client().path(SENSORS_ENDPOINT + "/" + SENSOR_NAME)
                .delete();
            assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());

            String value = client().path(SENSORS_ENDPOINT + "/" + SENSOR_NAME).accept(MediaType.TEXT_PLAIN_TYPE).get(String.class);
            assertEquals(value, "");

        } finally { addAmphibianSensor(entity); }
    }
    
    @Test
    public void testGetSensorValueOfTypeManagementContext() throws Exception {
        entity.sensors().set(Sensors.newSensor(ManagementContext.class, "myMgmt"), getManagementContext());
        doGetSensorTest("myMgmt", Map.class, ImmutableMap.of("type", ManagementContext.class.getName()));
    }
    
    @Test
    public void testGetSensorValueOfTypeEntity() throws Exception {
        // Note that non-raw will render it with just the entity's displayName
        entity.sensors().set(Sensors.newSensor(Entity.class, "myEntity"), entity);
        doGetSensorTest("myEntity", Map.class, ImmutableMap.of("type", Entity.class.getName(), "id", entity.getId()), true);
        doGetSensorTest("myEntity", String.class, "\"simple-ent\"", false);
    }
    
    @Test
    public void testGetSensorValueOfTypeLocation() throws Exception {
        Location loc = getManagementContext().getLocationRegistry().getLocationManaged("localhost");
        entity.sensors().set(Sensors.newSensor(Location.class, "myLocation"), loc);
        doGetSensorTest("myLocation", Map.class, ImmutableMap.of("type", Location.class.getName(), "id", loc.getId()));
    }
    
    @Test
    public void testGetSensorValueOfTypePolicy() throws Exception {
        Policy policy = entity.policies().add(org.apache.brooklyn.api.policy.PolicySpec.create(TestPolicy.class));
        entity.sensors().set(Sensors.newSensor(Policy.class, "myPolicy"), policy);
        doGetSensorTest("myPolicy", Map.class, ImmutableMap.of("type", Policy.class.getName(), "id", policy.getId(), "entityId", entity.getId()));
    }
    
    @Test
    public void testGetSensorValueOfTypeEnricher() throws Exception {
        Enricher enricher = entity.enrichers().add(org.apache.brooklyn.api.sensor.EnricherSpec.create(TestEnricher.class));
        entity.sensors().set(Sensors.newSensor(Enricher.class, "myEnricher"), enricher);
        doGetSensorTest("myEnricher", Map.class, ImmutableMap.of("type", Enricher.class.getName(), "id", enricher.getId(), "entityId", entity.getId()));
    }
    
    @Test
    public void testGetSensorValueOfTypeFeed() throws Exception {
        Feed feed = entity.feeds().add(FunctionFeed.builder().entity(entity).build());
        entity.sensors().set(Sensors.newSensor(Feed.class, "myFeed"), feed);
        doGetSensorTest("myFeed", Map.class, ImmutableMap.of("type", Feed.class.getName(), "id", feed.getId(), "entityId", entity.getId()));
    }
    
    @Test
    public void testGetSensorValueOfTypeCompletedTask() throws Exception {
        Task<String> task = entity.getExecutionContext().submit("returning myval", Callables.returning("myval"));
        task.get();
        entity.sensors().set(Sensors.newSensor(Task.class, "myTask"), task);
        doGetSensorTest("myTask", String.class, "\"myval\"");
    }
    
    @Test
    public void testGetSensorValueOfTypeInprogressTask() throws Exception {
        Task<Void> task = Tasks.<Void>builder().displayName("my-task-name").body(new SleepingCallable(Duration.THIRTY_SECONDS)).build();
        entity.getExecutionContext().submit(task);
        entity.sensors().set(Sensors.newSensor(Task.class, "myTask"), task);
        doGetSensorTest("myTask", Map.class, ImmutableMap.of("type", Task.class.getName(), "id", task.getId(), "displayName", "my-task-name"));
    }        
    
    @Test
    public void testGetSensorValueOfTypeEffectorTask() throws Exception {
        TestEntity testEntity = entity.addChild(org.apache.brooklyn.api.entity.EntitySpec.create(TestEntity.class));
        Task<Void> task = testEntity.invoke(TestEntity.SLEEP_EFFECTOR, ImmutableMap.of("duration", Duration.THIRTY_SECONDS));
        entity.sensors().set(Sensors.newSensor(Task.class, "myTask"), task);
        doGetSensorTest("myTask", Map.class, ImmutableMap.of("type", Task.class.getName(), "id", task.getId(), "displayName", "sleepEffector"));
    }
    
    protected <T> void doGetSensorTest(String sensorName, Class<T> expectedType, T expectedVal) throws Exception {
        doGetSensorTest(sensorName, expectedType, expectedVal, true);
        doGetSensorTest(sensorName, expectedType, expectedVal, false);
    }

    protected <T> void doGetSensorTest(String sensorName, Class<T> expectedType, T expectedVal, boolean raw) throws Exception {
        Response response = client().path(SENSORS_ENDPOINT + "/" + sensorName)
                .query("raw", raw)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .get();
        
        HttpAsserts.assertHealthyStatusCode(response.getStatus());
        T value = response.readEntity(expectedType);
        assertEquals(value, expectedVal);
    }

    public static class SleepingCallable implements Callable<Void> {
        private final Duration duration;
        
        SleepingCallable(Duration duration) {
            this.duration = duration;
        }
        @Override
        public Void call() throws Exception {
            Time.sleep(duration);
            return null;
        }
    }
}
