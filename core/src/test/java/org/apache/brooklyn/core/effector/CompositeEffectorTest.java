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
package org.apache.brooklyn.core.effector;

import static org.apache.brooklyn.core.effector.http.HttpCommandEffectorTest.EFFECTOR_HTTP_COMMAND;
import static org.apache.brooklyn.test.Asserts.assertNotNull;
import static org.apache.brooklyn.test.Asserts.assertNull;
import static org.apache.brooklyn.test.Asserts.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.effector.http.HttpCommandEffector;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
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
import com.google.mockwebserver.MockResponse;

public class CompositeEffectorTest extends BrooklynAppUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(CompositeEffectorTest.class);
    private static final String DEFAULT_ENDPOINT = "/";

    final static Effector<List> EFFECTOR_COMPOSITE = Effectors.effector(List.class, "CompositeEffector").buildAbstract();

    protected BetterMockWebServer server;
    protected URL baseUrl;

    protected Location loc;
    protected CompositeEffector compositeEffector;

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
      return new MockResponse().addHeader("Content-Type", "application/json").setBody(stringFromResource(resource));
   }

   protected MockResponse response404() {
      return new MockResponse().setStatus("HTTP/1.1 404 Not Found");
   }

   protected MockResponse response204() {
      return new MockResponse().setStatus("HTTP/1.1 204 No Content");
   }

   protected String stringFromResource(String resourceName) {
      return stringFromResource("/org/apache/brooklyn/core/effector", resourceName);
   }

   private String stringFromResource(String prefix, String resourceName) {
      try {
         return Resources.toString(getClass().getResource(String.format("%s/%s", prefix, resourceName)), Charsets.UTF_8)
                 .replace(DEFAULT_ENDPOINT, url(""));
      } catch (IOException e) {
         throw Throwables.propagate(e);
      }
   }

   @Test
   public void testCompositeEffectorWithNonExistingName() throws InterruptedException {
      server.enqueue(jsonResponse("test.json"));

      HttpCommandEffector httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/get?login=myLogin"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.JSON_PATH, "$.args.login")
      );
      assertNotNull(httpCommandEffector);
      compositeEffector = new CompositeEffector(ConfigBag.newInstance()
              .configure(CompositeEffector.EFFECTOR_NAME, EFFECTOR_COMPOSITE.getName())
              .configure(CompositeEffector.EFFECTORS, ImmutableList.of(EFFECTOR_HTTP_COMMAND.getName()))
      );
      assertNotNull(compositeEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector, compositeEffector));
      List<Object> results = testEntity.invoke(EFFECTOR_COMPOSITE, ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      Asserts.assertEquals(results.size(), 1);

      assertTrue(results.get(0) instanceof String);
      Asserts.assertEquals(results.get(0), "myLogin");
   }

   @Test
   public void testCompositeEffectorWithStartName() throws InterruptedException {
      server.enqueue(jsonResponse("test.json"));

      HttpCommandEffector httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/get?login=myLogin"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.JSON_PATH, "$.args.login")
      );
      assertNotNull(httpCommandEffector);
      compositeEffector = new CompositeEffector(ConfigBag.newInstance()
              .configure(CompositeEffector.EFFECTOR_NAME, "start")
              .configure(CompositeEffector.EFFECTORS, ImmutableList.of(EFFECTOR_HTTP_COMMAND.getName()))
      );
      assertNotNull(compositeEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector, compositeEffector));
      List<Object> results = testEntity.invoke(Effectors.effector(List.class, "start").buildAbstract(), ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      Asserts.assertEquals(results.size(), 2);
      assertNull(results.get(0));
      assertTrue(results.get(1) instanceof String);
      Asserts.assertEquals(results.get(1), "myLogin");
   }

   @Test
   public void testCompositeEffectorWithStartNameAndOverriding() throws InterruptedException {
      server.enqueue(jsonResponse("test.json"));

      HttpCommandEffector httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/get?login=myLogin"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.JSON_PATH, "$.args.login")
      );
      assertNotNull(httpCommandEffector);
      compositeEffector = new CompositeEffector(ConfigBag.newInstance()
              .configure(CompositeEffector.EFFECTOR_NAME, "start")
              .configure(CompositeEffector.OVERRIDE, true)
              .configure(CompositeEffector.EFFECTORS, ImmutableList.of(EFFECTOR_HTTP_COMMAND.getName()))
      );
      assertNotNull(compositeEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector, compositeEffector));
      List<Object> results = testEntity.invoke(Effectors.effector(List.class, "start").buildAbstract(), ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      Asserts.assertEquals(results.size(), 1);
      assertTrue(results.get(0) instanceof String);
      Asserts.assertEquals(results.get(0), "myLogin");
   }

   @Test
   public void testCompositeEffectorWithStopName() throws InterruptedException {
      server.enqueue(jsonResponse("test.json"));

      HttpCommandEffector httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/get?login=myLogin"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.JSON_PATH, "$.args.login")
      );
      assertNotNull(httpCommandEffector);
      compositeEffector = new CompositeEffector(ConfigBag.newInstance()
              .configure(CompositeEffector.EFFECTOR_NAME, "stop")
              .configure(CompositeEffector.EFFECTORS, ImmutableList.of(EFFECTOR_HTTP_COMMAND.getName()))
      );
      assertNotNull(compositeEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector, compositeEffector));
      List<Object> results = testEntity.invoke(Effectors.effector(List.class, "stop").buildAbstract(), ImmutableMap.<String, Object>of()).getUnchecked(Duration.minutes(1));
      Asserts.assertEquals(results.size(), 2);
      assertTrue(results.get(0) instanceof String);
      Asserts.assertEquals(results.get(0), "myLogin");
      assertNull(results.get(1));
   }

   @Test
   public void testCompositeEffectorWithStopNameAndOverriding() throws InterruptedException {
      server.enqueue(jsonResponse("test.json"));

      HttpCommandEffector httpCommandEffector = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, EFFECTOR_HTTP_COMMAND.getName())
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/get?login=myLogin"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.JSON_PATH, "$.args.login")
      );
      assertNotNull(httpCommandEffector);
      compositeEffector = new CompositeEffector(ConfigBag.newInstance()
              .configure(CompositeEffector.EFFECTOR_NAME, "stop")
              .configure(CompositeEffector.OVERRIDE, true)
              .configure(CompositeEffector.EFFECTORS, ImmutableList.of(EFFECTOR_HTTP_COMMAND.getName()))
      );
      assertNotNull(compositeEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(httpCommandEffector, compositeEffector));
      List<Object> results = testEntity.invoke(Effectors.effector(List.class, "stop").buildAbstract(), ImmutableMap.<String, Object>of()).getUnchecked(Duration.minutes(1));
      Asserts.assertEquals(results.size(), 1);
      assertTrue(results.get(0) instanceof String);
      Asserts.assertEquals(results.get(0), "myLogin");
   }

   @Test(expectedExceptions = NullPointerException.class)
   public void testMissingEffectors() {
      compositeEffector = new CompositeEffector(ConfigBag.newInstance()
              .configure(CompositeEffector.EFFECTOR_NAME, EFFECTOR_COMPOSITE.getName())
              .configure(CompositeEffector.EFFECTORS, null)
      );
   }

   @Test(expectedExceptions = PropagatedRuntimeException.class)
   public void testWhenOneEffectorFails() throws InterruptedException {
      server.enqueue(response404());

      HttpCommandEffector eff1 = new HttpCommandEffector(ConfigBag.newInstance()
              .configure(HttpCommandEffector.EFFECTOR_NAME, "eff1")
              .configure(HttpCommandEffector.EFFECTOR_URI, url("/get?login=myLogin"))
              .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
              .configure(HttpCommandEffector.JSON_PATH, "$.args.login"));
      compositeEffector = new CompositeEffector(ConfigBag.newInstance()
              .configure(CompositeEffector.EFFECTOR_NAME, EFFECTOR_COMPOSITE.getName())
              .configure(CompositeEffector.EFFECTORS, ImmutableList.of("eff1", "eff2"))
      );
      assertNotNull(compositeEffector);
      TestEntity testEntity = app.createAndManageChild(buildEntitySpec(eff1, compositeEffector));
      List<Object> results = testEntity.invoke(EFFECTOR_COMPOSITE, ImmutableMap.<String, Object>of()).getUnchecked(Duration.seconds(1));
      Asserts.assertEquals(results.size(), 2);
   }

   private EntitySpec<TestEntity> buildEntitySpec(AddEffector... effectors) {
      EntitySpec<TestEntity> testEntitySpec = EntitySpec.create(TestEntity.class);
      for (AddEffector effector : effectors) {
         testEntitySpec.addInitializer(effector);
      }
      return testEntitySpec;
   }

}
