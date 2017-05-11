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

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.http.TestHttpServer;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.Jsonya.Navigator;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.jayway.jsonpath.JsonPath;

public class HttpCommandEffectorHttpBinTest {

    final static Effector<String> EFFECTOR_HTTPBIN = Effectors.effector(String.class, "Httpbin").buildAbstract();

    private TestHttpServer server;
    private String serverUrl;
    private TestApplication app;
    private EntityLocal entity;
    
    public static final class HttpBinRequestHandler implements HttpRequestHandler {
        private String serverUrl;
        @Override
        public void handle(HttpRequest request, HttpResponse response, HttpContext context)
                throws HttpException, IOException {
            Navigator<MutableMap<Object, Object>> j = Jsonya.newInstance().map();
            BasicHttpRequest req = (BasicHttpRequest)request;
            String url = req.getRequestLine().getUri();
            URI uri = URI.create(url);
            String method = req.getRequestLine().getMethod();
            boolean expectsPost = uri.getPath().equals("/post");
            if (expectsPost && !method.equals("POST") ||
                    !expectsPost && !method.equals("GET")) {
                throw new IllegalStateException("Method " + method + " not allowed on " + url);
            }
            List<NameValuePair> params = URLEncodedUtils.parse(uri, "UTF-8");
            if (!params.isEmpty()) {
                j.push().at("args");
                for (NameValuePair param : params) {
                    j.put(param.getName(), param.getValue());
                }
                j.pop();
            }
            j.put("origin", "127.0.0.1");
            j.put("url", serverUrl + url);

            response.setHeader("Content-Type", "application/json");
            response.setStatusCode(200);
            response.setEntity(new StringEntity(j.toString()));
        }
        public void setServerUrl(String serverUrl) {
            this.serverUrl = serverUrl;
        }
    }
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        app = TestApplication.Factory.newManagedInstanceForTests();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class).location(TestApplication.LOCALHOST_MACHINE_SPEC));
        app.start(ImmutableList.<Location>of());
        server = createHttpBinServer();
        serverUrl = server.getUrl();
    }

    public static TestHttpServer createHttpBinServer() {
        HttpBinRequestHandler handler = new HttpBinRequestHandler();
        TestHttpServer server = new TestHttpServer()
                .handler("/get", handler)
                .handler("/post", handler)
                .handler("/ip", handler)
                .start();
        handler.setServerUrl(server.getUrl());
        return server;
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (app != null) Entities.destroyAll(app.getManagementContext());
        server.stop();
    }

    @Test
    public void testHttpEffector() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "Httpbin")
                .configure(HttpCommandEffector.EFFECTOR_URI, serverUrl + "/get?login=myLogin")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
        ).apply(entity);

        String val = entity.invoke(EFFECTOR_HTTPBIN, MutableMap.<String,String>of()).get();
        Assert.assertEquals(JsonPath.parse(val).read("$.args.login", String.class), "myLogin");
    }

    @Test
    public void testHttpEffectorWithPayload() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "HttpbinPost")
                .configure(HttpCommandEffector.EFFECTOR_URI, serverUrl + "/post")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "POST")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_PAYLOAD, ImmutableMap.<String, Object>of(
                        "description", "Created via API", 
                        "public", "false",
                        "files", ImmutableMap.of("demo.txt", ImmutableMap.of("content","Demo"))))
                .configure(HttpCommandEffector.EFFECTOR_HTTP_HEADERS, ImmutableMap.of("Content-Type", "application/json"))
//                .configure(HttpCommandEffector.JSON_PATH, "$.url")
//                .configure(HttpCommandEffector.PUBLISH_SENSOR, "result")
                .configure(HttpCommandEffector.JSON_PATHS_AND_SENSORS, ImmutableMap.of("$.url", "result"))

        ).apply(entity);

        String url = entity.invoke(Effectors.effector(String.class, "HttpbinPost").buildAbstract(), MutableMap.<String,String>of()).get();
        Assert.assertNotNull(url, "url");
    }

    @Test
    public void testHttpEffectorWithJsonPath() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "Httpbin")
                .configure(HttpCommandEffector.EFFECTOR_URI, serverUrl + "/get?id=myId")
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
//                .configure(HttpCommandEffector.JSON_PATH, "$.args.id")
//                .configure(HttpCommandEffector.PUBLISH_SENSOR, "result")
                .configure(HttpCommandEffector.JSON_PATHS_AND_SENSORS, ImmutableMap.of("$.args.id", "result"))
        ).apply(entity);

        String val = entity.invoke(EFFECTOR_HTTPBIN, MutableMap.<String,String>of()).get();
        Assert.assertEquals(val, "myId");
        Assert.assertEquals(entity.sensors().get(Sensors.newStringSensor("result")), "myId");
    }
    
    @Test
    public void testHttpEffectorWithParameters() throws Exception {
        new HttpCommandEffector(ConfigBag.newInstance()
                .configure(HttpCommandEffector.EFFECTOR_NAME, "Httpbin")
                .configure(HttpCommandEffector.EFFECTOR_URI, serverUrl + "/get")                
                .configure(HttpCommandEffector.EFFECTOR_HTTP_VERB, "GET")
                .configure(HttpCommandEffector.EFFECTOR_PARAMETER_DEFS,
                        MutableMap.<String,Object>of("uri", MutableMap.of("defaultValue", serverUrl + "/get"))))
                .apply(entity);

        String val;
        // explicit value
        val = entity.invoke(EFFECTOR_HTTPBIN, MutableMap.of("uri", serverUrl + "/ip")).get();
        Assert.assertNotNull(JsonPath.parse(val).read("$.origin", String.class));

        // default value
        val = entity.invoke(EFFECTOR_HTTPBIN, MutableMap.<String,String>of()).get();
        Assert.assertEquals(JsonPath.parse(val).read("$.url", String.class), serverUrl + "/get");
    }
}
