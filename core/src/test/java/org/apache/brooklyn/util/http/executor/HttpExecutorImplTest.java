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
package org.apache.brooklyn.util.http.executor;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.Collection;
import java.util.Map;

import org.apache.brooklyn.util.core.http.BetterMockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.mockwebserver.MockResponse;
import com.google.mockwebserver.RecordedRequest;

public class HttpExecutorImplTest {
    protected BetterMockWebServer server;
    protected URL baseUrl;
    protected HttpExecutorFactoryImpl factory;

    protected String HTTP_HEADER_KEY = "content-type";
    protected String HTTP_HEADER_VALUE = "application/json";
    protected String HTTP_BODY = "{\"foo\":\"myfoo\"}";

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        factory = new HttpExecutorFactoryImpl();
        server = BetterMockWebServer.newInstanceLocalhost();
        server.play();
        baseUrl = server.getUrl("/");
    }

    @AfterMethod
    public void afterMethod() throws Exception {
        if (server != null) server.shutdown();
    }


    @Test
    public void testHttpRequest() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).addHeader(HTTP_HEADER_KEY + ":" + HTTP_HEADER_VALUE).setBody(HTTP_BODY));
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpResponse response = executor.execute(new HttpRequest.Builder()
                .method("get")
                .uri(baseUrl.toURI())
                .build());
        assertEquals(response.code(), 200);
    }

    @Test
    public void testHttpHeader() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).addHeader(HTTP_HEADER_KEY + ":" + HTTP_HEADER_VALUE).setBody(HTTP_BODY));
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpResponse response = executor.execute(new HttpRequest.Builder()
                .method("get")
                .uri(baseUrl.toURI())
                .build());
        Map<String, Collection<String>> headers = response.headers().asMap();

        assertTrue(headers.get(HTTP_HEADER_KEY).contains(HTTP_HEADER_VALUE));
    }
    @Test
    public void testHttpBody() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).addHeader(HTTP_HEADER_KEY + ":" + HTTP_HEADER_VALUE).setBody(HTTP_BODY));
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpResponse response = executor.execute(new HttpRequest.Builder()
                .method("post")
                .body(HTTP_BODY.getBytes())
                .uri(baseUrl.toURI())
                .build());
        RecordedRequest request = server.takeRequest();
        assertEquals(request.getBody(), ByteStreams.toByteArray(response.getContent()));
    }
    @Test
    public void testHttpPostRequest() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).addHeader(HTTP_HEADER_KEY + ":" + HTTP_HEADER_VALUE).setBody(HTTP_BODY));
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        @SuppressWarnings("unused")
        HttpResponse response = executor.execute(new HttpRequest.Builder()
                .method("post")
                .body(HTTP_BODY.getBytes())
                .uri(baseUrl.toURI())
                .build());
        RecordedRequest request = server.takeRequest();
        assertEquals(request.getPath(), baseUrl.getPath());
        assertEquals(request.getMethod(), "POST");
        assertEquals(new String(request.getBody()), HTTP_BODY);
    }
    @Test
    public void testHttpPutRequest() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).addHeader(HTTP_HEADER_KEY + ":" + HTTP_HEADER_VALUE).setBody(HTTP_BODY));
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        @SuppressWarnings("unused")
        HttpResponse response = executor.execute(new HttpRequest.Builder()
                .method("put")
                .uri(baseUrl.toURI())
                .build());
        RecordedRequest request = server.takeRequest();
        assertEquals(request.getMethod(), "PUT");
    }
    @Test
    public void testHttpDeleteRequest() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).addHeader(HTTP_HEADER_KEY + ":" + HTTP_HEADER_VALUE).setBody(HTTP_BODY));
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        @SuppressWarnings("unused")
        HttpResponse response = executor.execute(new HttpRequest.Builder()
                .method("delete")
                .uri(baseUrl.toURI())
                .build());
        RecordedRequest request = server.takeRequest();
        assertEquals(request.getMethod(), "DELETE");
    }

    protected Map<?, ?> getProps() {
        return ImmutableMap.of();
    }
}

