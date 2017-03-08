/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.http.TestHttpRequestHandler;
import org.apache.brooklyn.test.http.TestHttpServer;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.protocol.HttpContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestHttpCallTest extends BrooklynAppUnitTestSupport {

    private TestHttpServer server;
    private LocalhostMachineProvisioningLocation loc;
    private String testId;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testId = Identifiers.makeRandomId(8);
        server = new TestHttpServer()
                .handler("/201", new TestHttpRequestHandler()
                        .response("Created - " + testId)
                        .code(201))
                .handler("/204", new TestHttpRequestHandler().code(204))
                .handler("/index.html", new TestHttpRequestHandler()
                        .response("<html><body><h1>Im a H1 tag!</h1></body></html>")
                        .code(200))
                .handler("/body.json", new TestHttpRequestHandler()
                        .response("{\"a\":\"b\",\"c\":\"d\",\"e\":123,\"g\":false}")
                        .code(200 + Identifiers.randomInt(99)))
                .handler("/foo/bar", new TestHttpTestRequestHandler()
                        .method("POST")
                        .response("hello world")
                        .code(201))
                .start();
        loc = mgmt.getLocationManager().createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class)
                .configure("name", testId));
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (server != null) server.stop();
        }
    }

    @Test(groups = "Integration")
    public void testHttpBodyAssertions() {
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/201")
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", "Created - " + testId)));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/204")
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", "")));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/index.html")
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestSensor.ASSERTIONS, newAssertion("contains", "Im a H1 tag!")));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/body.json")
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestSensor.ASSERTIONS, newAssertion("matches", ".*123.*")));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/foo/bar")
                .configure(TestHttpCall.TARGET_METHOD, TestHttpCall.HttpMethod.POST)
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", "hello world")));
        app.start(ImmutableList.of(loc));
    }

    @Test(groups = "Integration")
    public void testHttpStatusAssertions() {
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/201")
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("notNull", Boolean.TRUE)));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/204")
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", HttpStatus.SC_NO_CONTENT)));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/body.json")
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("matches", "2[0-9][0-9]")));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/foo/bar")
                .configure(TestHttpCall.TARGET_METHOD, TestHttpCall.HttpMethod.POST)
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", HttpStatus.SC_CREATED)));
        app.start(ImmutableList.of(loc));
    }

    @Test(groups = "Integration")
    public void testHttpMethodAssertions() {
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/foo/bar")
                .configure(TestHttpCall.TARGET_METHOD, TestHttpCall.HttpMethod.GET)
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", HttpStatus.SC_METHOD_NOT_ALLOWED)));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/foo/bar")
                .configure(TestHttpCall.TARGET_METHOD, TestHttpCall.HttpMethod.POST)
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", HttpStatus.SC_CREATED)));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/foo/bar")
                .configure(TestHttpCall.TARGET_METHOD, TestHttpCall.HttpMethod.PUT)
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", HttpStatus.SC_METHOD_NOT_ALLOWED)));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/foo/bar")
                .configure(TestHttpCall.TARGET_METHOD, TestHttpCall.HttpMethod.DELETE)
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", HttpStatus.SC_METHOD_NOT_ALLOWED)));
        app.createAndManageChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/foo/bar")
                .configure(TestHttpCall.TARGET_METHOD, TestHttpCall.HttpMethod.HEAD)
                .configure(TestHttpCall.TIMEOUT, new Duration(10L, TimeUnit.SECONDS))
                .configure(TestHttpCall.ASSERTION_TARGET, TestHttpCall.HttpAssertionTarget.status)
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", HttpStatus.SC_METHOD_NOT_ALLOWED)));
        app.start(ImmutableList.of(loc));
    }

    @Test(groups = "Integration")
    public void testMaxAttempts() {
        app.addChild(EntitySpec.create(TestHttpCall.class)
                .configure(TestHttpCall.TARGET_URL, server.getUrl() + "/201")
                .configure(TestHttpCall.TIMEOUT, Duration.minutes(1))
                .configure(TestHttpCall.MAX_ATTEMPTS, 1)
                .configure(TestSensor.ASSERTIONS, newAssertion("isEqualTo", "Wrong")));
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            app.start(ImmutableList.of(loc));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            AssertionError ae = Exceptions.getFirstThrowableOfType(e, AssertionError.class);
            if (ae == null || !ae.toString().contains("body expected isEqualTo Wrong")) {
                throw e;
            }
        }
        Duration duration = Duration.of(stopwatch);
        assertTrue(duration.isShorterThan(Asserts.DEFAULT_LONG_TIMEOUT), "duration="+duration);
    }

    private List<Map<String, Object>> newAssertion(final String assertionKey, final Object assertionValue) {
        final List<Map<String, Object>> result = new ArrayList<>();
        result.add(ImmutableMap.of(assertionKey, assertionValue));
        return result;
    }

    private class TestHttpTestRequestHandler extends TestHttpRequestHandler {
        private String method = "GET";

        public TestHttpRequestHandler method(String method) {
            this.method = method;
            return this;
        }

        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            super.handle(request, response, context);

            if (!isValidRequest(request)) {
                response.setStatusCode(405);
                response.setEntity(null);
            }
        }

        private boolean isValidRequest(HttpRequest request) {
            return request.getRequestLine().getMethod().equals(this.method.toUpperCase());
        }
    }
}
