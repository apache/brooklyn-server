/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.rest.client;

import static org.apache.brooklyn.test.Asserts.assertEquals;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.core.http.BetterMockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.mockwebserver.MockResponse;
import com.google.mockwebserver.RecordedRequest;

public class BrooklynApiUtilTest {

    private static final String APP_ID = "fedcba";

    private static final String YAML = Joiner.on("\n").join(
            "name: test-blueprint",
            "location: localhost",
            "services:",
            "- type: brooklyn.entity.basic.EmptySoftwareProcess");

    private BetterMockWebServer server;

    @BeforeMethod(alwaysRun = true)
    public void newMockWebServer() {
        server = BetterMockWebServer.newInstanceLocalhost();
    }

    @AfterMethod(alwaysRun = true)
    public void shutDownServer() throws Exception {
        if (server != null) server.shutdown();
    }

    @Test
    public void testDeployBlueprint() throws Exception {
        server.enqueue(taskSummaryResponse());
        server.play();

        BrooklynApi api = BrooklynApi.newInstance(server.getUrl("/").toString());
        BrooklynApiUtil.deployBlueprint(api, YAML);

        RecordedRequest request = server.takeRequest();
        assertEquals("/applications", request.getPath());
        assertEquals("POST", request.getMethod());
        assertEquals(YAML, new String(request.getBody()));
    }

    @Test
    public void testWaitForRunningExitsCleanlyWhenAppRunning() throws Exception {
        server.enqueue(applicationStatusResponse("RUNNING"));
        server.play();

        BrooklynApi api = BrooklynApi.newInstance(server.getUrl("/").toString());
        BrooklynApiUtil.waitForRunningAndThrowOtherwise(api, "appId", "taskId");
        // i.e. no exception
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testWaitForRunningFailsWhenAppStatusError() throws Exception {
        server.enqueue(applicationStatusResponse("ERROR"));
        // Method checks for status of task.
        server.enqueue(taskSummaryResponse());
        server.play();

        BrooklynApi api = BrooklynApi.newInstance(server.getUrl("/").toString());
        BrooklynApiUtil.waitForRunningAndThrowOtherwise(api, "appId", "taskId");
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testWaitForRunningFailsWhenAppStatusUnknown() throws Exception {
        server.enqueue(applicationStatusResponse("UNKNOWN"));
        // Method checks for status of task.
        server.enqueue(taskSummaryResponse());
        server.play();

        BrooklynApi api = BrooklynApi.newInstance(server.getUrl("/").toString());
        BrooklynApiUtil.waitForRunningAndThrowOtherwise(api, "appId", "taskId");
    }

    /** @return a response whose Content-Type header is application/json. */
    private MockResponse newJsonResponse() {
        return new MockResponse()
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    }

    private MockResponse taskSummaryResponse() {
        String body = Jsonya.newInstance()
                .put("id", "taskid")
                .put("entityId", APP_ID)
                .toString();
        return newJsonResponse().setBody(body);
    }

    private MockResponse applicationStatusResponse(String status) {
        String body = Jsonya.newInstance()
                .put("status", status)
                .at("spec", "locations").list().add("localhost")
                .root()
                .toString();
        return newJsonResponse()
                .setBody(body);
    }

}
