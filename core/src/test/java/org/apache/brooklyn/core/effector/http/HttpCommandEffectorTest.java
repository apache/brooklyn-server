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
package org.apache.brooklyn.core.effector.http;

import static org.apache.brooklyn.test.Asserts.assertNotNull;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.http.BetterMockWebServer;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.net.HttpHeaders;
import com.google.mockwebserver.MockResponse;
import com.google.mockwebserver.RecordedRequest;

public class HttpCommandEffectorTest extends BrooklynAppUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(HttpCommandEffectorTest.class);
    private static final String DEFAULT_ENDPOINT = "/";

    public final static Effector<String> EFFECTOR_HTTP_COMMAND = Effectors.effector(String.class, "http-command-effector").buildAbstract();

    protected BetterMockWebServer server;
    protected URL baseUrl;

    protected Location loc;
    protected HttpCommandEffector httpCommandEffector;

   @BeforeMethod
   public void start() throws IOException {
      server = BetterMockWebServer.newInstanceLocalhost();
      server.play();
   }

   @AfterMethod(alwaysRun = true)
   public void stop() throws IOException {
      server.shutdown();
   }

   protected String url(String path) {
      return server.getUrl(path).toString();
   }

   protected MockResponse jsonResponse(String resource) {
      return new MockResponse().addHeader(HttpHeaders.CONTENT_TYPE, "application/json").setBody(stringFromResource(resource));
   }

   protected MockResponse response404() {
      return new MockResponse().setStatus("HTTP/1.1 404 Not Found");
   }

   protected String stringFromResource(String resourceName) {
      return stringFromResource("/org/apache/brooklyn/core/effector/http", resourceName);
   }

   private String stringFromResource(String prefix, String resourceName) {
      try {
         return Resources.toString(getClass().getResource(String.format("%s/%s", prefix, resourceName)), Charsets.UTF_8)
                 .replace(DEFAULT_ENDPOINT, url(""));
      } catch (IOException e) {
         throw Throwables.propagate(e);
      }
   }

   protected RecordedRequest assertSent(BetterMockWebServer server, String method, String path) throws InterruptedException {
      RecordedRequest request = server.takeRequest();
      assertEquals(request.getMethod(), method);
      assertEquals(request.getPath(), path);
      return request;
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testMissingURI() {
      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
      );
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testMissingVerb() {
      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url(""))
      );
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testInvalidURI() throws ExecutionException, InterruptedException {
      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.EFFECTOR_URI, "invalid-uri")
      );
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector));
      testEntity.invoke(EFFECTOR_HTTP_COMMAND, MutableMap.<String,String>of()).get();
   }

   @Test(expectedExceptions = ExecutionException.class)
   public void testInvalidVerb() throws ExecutionException, InterruptedException {
      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "CHANGE")
              .configure(HttpCommandEffector.EFFECTOR_URI, url(""))
      );
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector));
      testEntity.invoke(EFFECTOR_HTTP_COMMAND, MutableMap.<String,String>of()).get();
   }

   @Test
   public void testPayloadWithContentTypeHeaderJson() throws InterruptedException {
      server.enqueue(jsonResponse("map-response.json"));

      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/post"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "POST")
              .configure(HttpCommandEffector.EFFECTOR_HTTP_PAYLOAD, ImmutableMap.of("key", "value"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_HEADERS, ImmutableMap.of(HttpHeaders.CONTENT_TYPE, "application/json"))
              .configure(HttpCommandEffector.JSON_PATH, "$.data")
      );
      assertNotNull(httpCommandEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector));
      Object output = testEntity.invoke(EFFECTOR_HTTP_COMMAND, ImmutableMap.<String, Object>of()).getUnchecked(Duration.minutes(1));
      assertEquals(output, "{\"key\", \"value\"}");

      assertEquals(server.getRequestCount(), 1);
      assertSent(server, "POST", "/post");
   }

   @Test
   public void testPayloadWithoutContentTypeHeader() throws InterruptedException {
      server.enqueue(jsonResponse("map-response.json"));

      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/post"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "POST")
              .configure(HttpCommandEffector.EFFECTOR_HTTP_PAYLOAD, ImmutableMap.of("key", "value"))
              .configure(HttpCommandEffector.JSON_PATH, "$.data")
      );
      assertNotNull(httpCommandEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector));
      Object output = testEntity.invoke(EFFECTOR_HTTP_COMMAND, ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      assertEquals(output, "{\"key\", \"value\"}");

      assertEquals(server.getRequestCount(), 1);
      assertSent(server, "POST", "/post");
   }

   @Test
   public void testListPayloadWithoutContentTypeHeader() throws InterruptedException {
      server.enqueue(jsonResponse("list-response.json"));

      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/post"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "POST")
              .configure(HttpCommandEffector.EFFECTOR_HTTP_PAYLOAD, ImmutableList.of("key", "value"))
              .configure(HttpCommandEffector.JSON_PATH, "$.data")
      );
      assertNotNull(httpCommandEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector));
      Object output = testEntity.invoke(EFFECTOR_HTTP_COMMAND, ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      assertEquals(output, "[\"key\", \"value\"]");

      assertEquals(server.getRequestCount(), 1);
      assertSent(server, "POST", "/post");
   }

   @Test
   public void testPayloadWithContentTypeHeaderXml() throws InterruptedException {
      server.enqueue(jsonResponse("int-response.json"));

      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/post"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "POST")
              .configure(HttpCommandEffector.EFFECTOR_HTTP_PAYLOAD, 1)
              .configure(HttpCommandEffector.EFFECTOR_HTTP_HEADERS, ImmutableMap.of(HttpHeaders.CONTENT_TYPE, "application/xml"))
              .configure(HttpCommandEffector.JSON_PATH, "$.data")
      );
      assertNotNull(httpCommandEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector));
      Object output = testEntity.invoke(EFFECTOR_HTTP_COMMAND, ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      assertEquals(output, "1");

      assertEquals(server.getRequestCount(), 1);
      assertSent(server, "POST", "/post");
   }

   @Test
   public void testHappyPath() throws InterruptedException {
      server.enqueue(jsonResponse("login.json"));

      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/get?login=myLogin"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.JSON_PATH, "$.args.login")
      );
      assertNotNull(httpCommandEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector));
      Object output = testEntity.invoke(EFFECTOR_HTTP_COMMAND, ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      assertEquals(output, "myLogin");

      assertEquals(server.getRequestCount(), 1);
      assertSent(server, "GET", "/get?login=myLogin");
   }

   @Test(expectedExceptions = PropagatedRuntimeException.class)
   public void testWhen404() throws InterruptedException {
      server.enqueue(response404());

      httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/get?login=myLogin"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.JSON_PATH, "$.args.login")
      );
      assertNotNull(httpCommandEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector));
      Object output = testEntity.invoke(EFFECTOR_HTTP_COMMAND, ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      assertEquals(output, "myLogin");

      assertEquals(server.getRequestCount(), 1);
      assertSent(server, "GET", "/get?login=myLogin");
   }

   private EntitySpec<TestEntity> buildEntitySpec(HttpCommandEffector httpCommandEffector) {
      return EntitySpec.create(TestEntity.class).addInitializer(httpCommandEffector);
   }

}
