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

import java.net.URI;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.http.HttpRequestSensor;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.http.TestHttpRequestHandler;
import org.apache.brooklyn.test.http.TestHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class HttpRequestSensorYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(HttpRequestSensorYamlTest.class);

    final static AttributeSensor<String> SENSOR_STRING = Sensors.newStringSensor("aString");
    final static String TARGET_TYPE = String.class.getName();

    private TestHttpServer server;
    private String serverUrl;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        server = new TestHttpServer()
            .handler("/myKey/myValue", new TestHttpRequestHandler().header("Content-Type", "application/json").response("{\"myKey\":\"myValue\"}"))
            .start();
        serverUrl = server.getUrl();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            if (server != null) server.stop();
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testHttpSensor() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "  brooklyn.initializers:",
            "  - type: "+HttpRequestSensor.class.getName(),
            "    brooklyn.config:",
            "      "+HttpRequestSensor.SENSOR_PERIOD.getName()+": 100ms",
            "      "+HttpRequestSensor.SENSOR_NAME.getName()+": " + SENSOR_STRING.getName(),
            "      "+HttpRequestSensor.SENSOR_TYPE.getName()+": " + TARGET_TYPE,
            "      "+HttpRequestSensor.JSON_PATH.getName()+": " + "$.myKey",
            "      "+HttpRequestSensor.SENSOR_URI.getName()+": " + serverUrl + "/myKey/myValue");
        waitForApplicationTasks(app);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        entity.sensors().set(Attributes.SERVICE_UP, true);

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "myValue");
    }

    // TODO Fails trying to coerce DslConfigSupplier to String
    // See https://issues.apache.org/jira/browse/BROOKLYN-330
    @Test(groups="Broken")
    public void testHttpSensorWithDeferredSuppliers() throws Exception {
        // TODO If we fix the coercion problem, will it still fail because passing username/password?
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  brooklyn.config:",
            "    server.username: myusername",
            "    server.password: mypassword",
            "  brooklyn.initializers:",
            "  - type: "+HttpRequestSensor.class.getName(),
            "    brooklyn.config:",
            "      "+HttpRequestSensor.SENSOR_PERIOD.getName()+": 100ms",
            "      "+HttpRequestSensor.SENSOR_NAME.getName()+": " + SENSOR_STRING.getName(),
            "      "+HttpRequestSensor.SENSOR_TYPE.getName()+": " + TARGET_TYPE,
            "      "+HttpRequestSensor.JSON_PATH.getName()+": " + "$.myKey",
            "      "+HttpRequestSensor.USERNAME.getName()+": $brooklyn:config(\"server.username\")",
            "      "+HttpRequestSensor.PASSWORD.getName()+": $brooklyn:config(\"server.password\")",
            "      "+HttpRequestSensor.SENSOR_URI.getName()+":",
            "          $brooklyn:formatString:",
            "            - \"%s/myKey/myValue\"",
            "            - $brooklyn:attributeWhenReady(\"main.uri\")");

        waitForApplicationTasks(app);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        entity.sensors().set(Attributes.MAIN_URI, URI.create(serverUrl));
        entity.sensors().set(Attributes.SERVICE_UP, true);

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "myValue");
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
