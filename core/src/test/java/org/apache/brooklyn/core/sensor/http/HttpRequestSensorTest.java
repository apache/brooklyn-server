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
package org.apache.brooklyn.core.sensor.http;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.feed.http.HttpFeedTest;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.http.RecordingHttpRequestHandler;
import org.apache.brooklyn.test.http.TestHttpRequestHandler;
import org.apache.brooklyn.test.http.TestHttpServer;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.apache.http.HttpRequest;
import org.apache.http.protocol.HttpRequestHandler;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class HttpRequestSensorTest {
    final static AttributeSensor<String> SENSOR_STRING = Sensors.newStringSensor("aString");
    final static AttributeSensor<Object> SENSOR_OBJ = Sensors.newSensor(Object.class, "anObj");
    final static String STRING_TARGET_TYPE = "java.lang.String";
    final static String OBJECT_TARGET_TYPE = "java.lang.Object";

    private TestApplication app;
    private Entity entity;

    private TestHttpServer server;
    private String serverUrl;
    private RecordingHttpRequestHandler recordingHandler;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        HttpRequestHandler handler = new TestHttpRequestHandler().header("Content-Type", "application/json").response("{\"myKey\":\"myValue\"}");
        recordingHandler = new RecordingHttpRequestHandler(handler);
        server = new TestHttpServer()
            .handler("/nonjson", new TestHttpRequestHandler().header("Content-Type", "text/plain").response("myresponse"))
            .handler("/jsonstring", new TestHttpRequestHandler().header("Content-Type", "application/json").response("\"myValue\""))
            .handler("/myKey/myValue", recordingHandler)
            .start();
        serverUrl = server.getUrl();

        app = TestApplication.Factory.newManagedInstanceForTests();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .location(TestApplication.LOCALHOST_MACHINE_SPEC));
        app.start(ImmutableList.<Location>of());
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (app != null) Entities.destroyAll(app.getManagementContext());
        server.stop();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testHttpSensor() throws Exception {
        HttpRequestSensor<Integer> sensor = new HttpRequestSensor<Integer>(ConfigBag.newInstance()
                .configure(HttpRequestSensor.SENSOR_PERIOD, Duration.millis(100))
                .configure(HttpRequestSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(HttpRequestSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(HttpRequestSensor.JSON_PATH, "$.myKey")
                .configure(HttpRequestSensor.SENSOR_URI, serverUrl + "/myKey/myValue"));
        sensor.apply((org.apache.brooklyn.api.entity.EntityLocal)entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "myValue");
    }

    // "Integration" because takes a second
    @Test(groups="Integration")
    @SuppressWarnings("deprecation")
    public void testHttpSensorSuppressingDuplicates() throws Exception {
        RecordingSensorEventListener<String> listener = new RecordingSensorEventListener<>();
        entity.subscriptions().subscribe(entity, SENSOR_STRING, listener);
        
        HttpRequestSensor<Integer> sensor = new HttpRequestSensor<Integer>(ConfigBag.newInstance()
                .configure(HttpRequestSensor.SUPPRESS_DUPLICATES, true)
                .configure(HttpRequestSensor.SENSOR_PERIOD, Duration.millis(1))
                .configure(HttpRequestSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(HttpRequestSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(HttpRequestSensor.JSON_PATH, "$.myKey")
                .configure(HttpRequestSensor.SENSOR_URI, serverUrl + "/myKey/myValue"));
        sensor.apply((org.apache.brooklyn.api.entity.EntityLocal)entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "myValue");
        listener.assertHasEventEventually(Predicates.alwaysTrue());
        
        Asserts.succeedsContinually(new Runnable() {
            @Override public void run() {
                listener.assertEventCount(1);
            }});
    }

    // TODO Fails because doesn't pick up default value of `JSON_PATH`, which is `$`
    @Test(groups="Broken")
    @SuppressWarnings("deprecation")
    public void testDefaultJsonPath() throws Exception {
        HttpRequestSensor<Integer> sensor = new HttpRequestSensor<Integer>(ConfigBag.newInstance()
                .configure(HttpRequestSensor.SENSOR_PERIOD, Duration.millis(100))
                .configure(HttpRequestSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(HttpRequestSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(HttpRequestSensor.SENSOR_URI, serverUrl + "/jsonstring"));
        sensor.apply((org.apache.brooklyn.api.entity.EntityLocal)entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "myValue");
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testPreemptiveBasicAuth() throws Exception {
        HttpRequestSensor<Integer> sensor = new HttpRequestSensor<Integer>(ConfigBag.newInstance()
                .configure(HttpRequestSensor.PREEMPTIVE_BASIC_AUTH, true)
                .configure(HttpRequestSensor.USERNAME, "myuser")
                .configure(HttpRequestSensor.PASSWORD, "mypass")
                .configure(HttpRequestSensor.SENSOR_PERIOD, Duration.minutes(1))
                .configure(HttpRequestSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(HttpRequestSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(HttpRequestSensor.JSON_PATH, "$.myKey")
                .configure(HttpRequestSensor.SENSOR_URI, serverUrl + "/myKey/myValue"));
        sensor.apply((org.apache.brooklyn.api.entity.EntityLocal)entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);
        
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "myValue");
        
        HttpRequest req = Iterables.getFirst(recordingHandler.getRequests(), null);
        String headerVal = req.getFirstHeader("Authorization").getValue();
        String expectedVal = HttpFeedTest.getBasicAuthHeaderVal("myuser", "mypass");
        assertEquals(headerVal, expectedVal);
    }
    
    @Test
    @SuppressWarnings("deprecation")
    public void testDoNotParseJson() throws Exception {
        HttpRequestSensor<Integer> sensor = new HttpRequestSensor<Integer>(ConfigBag.newInstance()
                .configure(HttpRequestSensor.SENSOR_PERIOD, Duration.minutes(1))
                .configure(HttpRequestSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(HttpRequestSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(HttpRequestSensor.JSON_PATH, "")
                .configure(HttpRequestSensor.SENSOR_URI, serverUrl + "/myKey/myValue"));
        sensor.apply((org.apache.brooklyn.api.entity.EntityLocal)entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);
        
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "{\"myKey\":\"myValue\"}");
    }
    
    @Test
    @SuppressWarnings("deprecation")
    public void testNonJson() throws Exception {
        HttpRequestSensor<Integer> sensor = new HttpRequestSensor<Integer>(ConfigBag.newInstance()
                .configure(HttpRequestSensor.SENSOR_PERIOD, Duration.minutes(1))
                .configure(HttpRequestSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(HttpRequestSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(HttpRequestSensor.JSON_PATH, "")
                .configure(HttpRequestSensor.SENSOR_URI, serverUrl + "/nonjson"));
        sensor.apply((org.apache.brooklyn.api.entity.EntityLocal)entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);
        
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "myresponse");
    }
}
