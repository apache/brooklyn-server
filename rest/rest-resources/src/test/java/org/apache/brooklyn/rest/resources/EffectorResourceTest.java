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
import static org.testng.Assert.assertTrue;

import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;

/**
 * Test the {@link EffectorApi} implementation.
 */
@Test(singleThreaded = true,
        // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "EffectorResourceTest")
public class EffectorResourceTest extends BrooklynRestResourceTest {

    BasicApplication app;
    TestEntity entity;

    @BeforeMethod(alwaysRun = true)
    public void setUpMethod() throws Exception {
        app = getManagementContext().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class)
                .child(EntitySpec.create(TestEntity.class)));
        entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
    }

    @Test
    public void testInvokeEffectorNoArgs() throws Exception {
        String path = "/applications/"+app.getId()+"/entities/"+entity.getId()+"/effectors/"+"myEffector";

        Response response = client().path(path)
                .accept(MediaType.APPLICATION_JSON)
                .post(null);
        assertEquals(response.getStatus(), 202);
        Asserts.succeedsEventually(() -> assertTrue(entity.getCallHistory().contains("myEffector")));
    }
    
    @Test
    public void testInvokeEffectorNoArgsBlocking() throws Exception {
        String path = "/applications/"+app.getId()+"/entities/"+entity.getId()+"/effectors/"+"myEffector";

        Response response = client().path(path)
                .query("timeout", "0")
                .accept(MediaType.APPLICATION_JSON)
                .post(null);
        assertEquals(response.getStatus(), 202);
        assertTrue(entity.getCallHistory().contains("myEffector"));
    }
    
    @Test
    public void testInvokeEffectorWithArg() throws Exception {
        String path = "/applications/"+app.getId()+"/entities/"+entity.getId()+"/effectors/"+"identityEffector";

        Response response = client().path(path)
                .accept(MediaType.APPLICATION_JSON)
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .post("{\"arg\": \"myval\"}");
        assertEquals(response.getStatus(), 202);
        
        String responseBody = response.readEntity(String.class);
        assertTrue(entity.getCallHistory().contains("identityEffector"));
        assertEquals(responseBody, "myval");
    }
    
    @Test
    public void testInvokeEffectorWithTimeoutWaits() throws Exception {
        String path = "/applications/"+app.getId()+"/entities/"+entity.getId()+"/effectors/"+"sleepEffector";

        Stopwatch stopwatch = Stopwatch.createStarted();
        Response response = client().path(path)
                .query("timeout", "1m")
                .accept(MediaType.APPLICATION_JSON)
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .post("{\"duration\": \"50ms\"}");
        Duration runDuration = Duration.of(stopwatch);
        assertEquals(response.getStatus(), 202);
        
        assertTrue(entity.getCallHistory().contains("sleepEffector"));
        assertTrue(runDuration.isLongerThan(Duration.millis(40)), "runDuration="+runDuration);
    }
    
    @Test
    public void testInvokeEffectorWithTimeoutTimesOut() throws Exception {
        String path = "/applications/"+app.getId()+"/entities/"+entity.getId()+"/effectors/"+"sleepEffector";

        Stopwatch stopwatch = Stopwatch.createStarted();
        Response response = client().path(path)
                .query("timeout", "1ms")
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .post("{\"duration\": \"5m\"}");
        Duration runDuration = Duration.of(stopwatch);
        assertEquals(response.getStatus(), 202);
        
        String responseBody = response.readEntity(String.class);
        assertTrue(entity.getCallHistory().contains("sleepEffector"));
        assertTrue(runDuration.isShorterThan(Asserts.DEFAULT_LONG_TIMEOUT), "runDuration="+runDuration);
        
        // Expect to get a task back, representing the currently executing effector
        Map<?,?> responseMap = new Gson().fromJson(responseBody, Map.class);
        assertTrue((""+responseMap.get("displayName")).contains("sleepEffector"), "responseMap="+responseMap);
        assertTrue((""+responseMap.get("detailedStatus")).contains("In progress, thread waiting"), "responseMap="+responseMap);
    }
}
