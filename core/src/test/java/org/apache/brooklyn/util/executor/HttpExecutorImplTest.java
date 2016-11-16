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
package org.apache.brooklyn.util.executor;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.Assert.assertEquals;

import java.net.URL;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import org.apache.brooklyn.util.core.http.BetterMockWebServer;
import org.apache.brooklyn.util.executor.HttpExecutorFactoryImpl;
import org.apache.brooklyn.util.http.executor.HttpExecutor;
import org.apache.brooklyn.util.http.executor.HttpRequest;
import org.apache.brooklyn.util.http.executor.HttpRequestImpl;
import org.apache.brooklyn.util.http.executor.HttpResponse;
import org.apache.brooklyn.util.http.executor.UsernamePassword;
import org.apache.commons.codec.binary.Base64;
import org.bouncycastle.util.Strings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.mockwebserver.MockResponse;
import com.google.mockwebserver.RecordedRequest;

import javax.annotation.Nullable;

public class HttpExecutorImplTest {
    protected BetterMockWebServer server;
    protected URL baseUrl;
    protected HttpExecutorFactoryImpl factory;

    protected String HTTP_RESPONSE_CUSTOM_HEADER_KEY = "Custom-Header";
    protected String HTTP_RESPONSE_CUSTOM_HEADER_VALUE = "Custom Value";
    protected String HTTP_RESPONSE_HEADER_KEY = "content-type";
    protected String HTTP_RESPONSE_HEADER_VALUE = "application/json";
    protected String HTTP_BODY = "{\"foo\":\"myfoo\"}";

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        factory = new HttpExecutorFactoryImpl();
        server = BetterMockWebServer.newInstanceLocalhost();
        server.play();
        baseUrl = server.getUrl("/");
    }

    @AfterMethod(alwaysRun=true)
    public void afterMethod() throws Exception {
        if (server != null) server.shutdown();
    }

    @Test
    public void testHttpResponse() throws Exception {
        Multimap<String, String> responseHeaders = ArrayListMultimap.<String, String>create();
        responseHeaders.put(HTTP_RESPONSE_CUSTOM_HEADER_KEY, HTTP_RESPONSE_CUSTOM_HEADER_VALUE);
        HttpResponse httpResponse = new HttpResponse.Builder()
                .headers(responseHeaders)
                .header(HTTP_RESPONSE_HEADER_KEY, HTTP_RESPONSE_HEADER_VALUE)
                .build();

        MockResponse serverResponse = new MockResponse()
                .setResponseCode(200)
                .setBody(HTTP_BODY);
        
        for (Map.Entry<String, String> entry : httpResponse.headers().entries()) {
            serverResponse.addHeader(entry.getKey(), entry.getValue());
        }
        server.enqueue(serverResponse);
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpRequest executorRequest = new HttpRequest.Builder()
                .method("GET")
                .uri(baseUrl.toURI())
                .build();
        HttpResponse executorResponse = executor.execute(executorRequest);
        assertTrue(executorResponse.headers().containsKey(HTTP_RESPONSE_CUSTOM_HEADER_KEY));
        assertTrue(Iterables.getOnlyElement(executorResponse.headers().get(HTTP_RESPONSE_HEADER_KEY)).equals(HTTP_RESPONSE_HEADER_VALUE));

        assertTrue(executorResponse.headers().containsKey(HTTP_RESPONSE_HEADER_KEY));
        assertTrue(Iterables.getOnlyElement(executorResponse.headers().get(HTTP_RESPONSE_CUSTOM_HEADER_KEY)).equals(HTTP_RESPONSE_CUSTOM_HEADER_VALUE));
    }

    @Test
    public void testHttpRequest() throws Exception {
        MockResponse serverResponse = new MockResponse().setResponseCode(200).addHeader(HTTP_RESPONSE_HEADER_KEY, HTTP_RESPONSE_HEADER_VALUE).setBody(HTTP_BODY);
        server.enqueue(serverResponse);
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpRequest executorRequest = new HttpRequest.Builder()
                .method("GET")
                .uri(baseUrl.toURI())
                .build();
        HttpResponse executorResponse = executor.execute(executorRequest);
        assertRequestAndResponse(server.takeRequest(), serverResponse, executorRequest, executorResponse);
    }

    @Test
    public void testHttpPostRequest() throws Exception {
        MockResponse serverResponse = new MockResponse().setResponseCode(200).addHeader(HTTP_RESPONSE_HEADER_KEY, HTTP_RESPONSE_HEADER_VALUE).setBody(HTTP_BODY);
        server.enqueue(serverResponse);
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpRequest executorRequest = new HttpRequest.Builder().headers(ImmutableMap.of("RequestHeader", "RequestHeaderValue"))
                .method("POST")
                .body(HTTP_BODY.getBytes())
                .uri(baseUrl.toURI())
                .build();
        HttpResponse executorResponse = executor.execute(executorRequest);
        assertRequestAndResponse(server.takeRequest(), serverResponse, executorRequest, executorResponse);

        // Big POST request with random bytes
        serverResponse = new MockResponse().setResponseCode(200).addHeader(HTTP_RESPONSE_HEADER_KEY + "Test", HTTP_RESPONSE_HEADER_VALUE).setBody(HTTP_BODY);
        server.enqueue(serverResponse);
        executor = factory.getHttpExecutor(getProps());
        byte[] requestBody = new byte[10 * 1024 * 1024];
        Random r = new Random();
        for (int i = 0; i < requestBody.length; i++) {
            requestBody[i] = (byte)(r.nextInt() % 256);
        }
        executorRequest = new HttpRequest.Builder()
                .method("POST")
                .body(requestBody)
                .uri(baseUrl.toURI())
                .build();
        executorResponse = executor.execute(executorRequest);
        assertRequestAndResponse(server.takeRequest(), serverResponse, executorRequest, executorResponse);
    }

    @Test
    public void testHttpPutRequest() throws Exception {
        MockResponse serverResponse = new MockResponse().setResponseCode(200).addHeader(HTTP_RESPONSE_HEADER_KEY, HTTP_RESPONSE_HEADER_VALUE).setBody(HTTP_BODY);
        server.enqueue(serverResponse);
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpRequest executorRequest = new HttpRequest.Builder()
                .method("PUT")
                .uri(baseUrl.toURI())
                .build();
        HttpResponse executorResponse = executor.execute(executorRequest);
        assertRequestAndResponse(server.takeRequest(), serverResponse, executorRequest, executorResponse);
    }

    @Test
    public void testHttpDeleteRequest() throws Exception {
        MockResponse serverResponse = new MockResponse().setResponseCode(200).addHeader(HTTP_RESPONSE_HEADER_KEY, HTTP_RESPONSE_HEADER_VALUE).setBody(HTTP_BODY);
        server.enqueue(serverResponse);
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpRequest executorRequest = new HttpRequest.Builder()
                .method("DELETE")
                .uri(baseUrl.toURI())
                .build();
        HttpResponse executorResponse = executor.execute(executorRequest);
        assertRequestAndResponse(server.takeRequest(), serverResponse, executorRequest, executorResponse);

        // No Headers returned
        serverResponse = new MockResponse().setResponseCode(200).setBody(HTTP_BODY);
        server.enqueue(serverResponse);
        executor = factory.getHttpExecutor(getProps());
        executorRequest = new HttpRequest.Builder()
                .method("DELETE")
                .uri(baseUrl.toURI())
                .build();
        executorResponse = executor.execute(executorRequest);
        assertRequestAndResponse(server.takeRequest(), serverResponse, executorRequest, executorResponse);
    }

    @Test
    public void testHttpPasswordRequest() throws Exception {
        MockResponse firstServerResponse = new MockResponse().setResponseCode(401).addHeader("WWW-Authenticate", "Basic realm=\"User Visible Realm\"").setBody("Not Authenticated");
        server.enqueue(firstServerResponse);
        MockResponse secondServerResponse = new MockResponse().setResponseCode(200).addHeader(HTTP_RESPONSE_HEADER_KEY, HTTP_RESPONSE_HEADER_VALUE).setBody(HTTP_BODY);
        server.enqueue(secondServerResponse);
        final String USER = "brooklyn",
                PASSWORD = "apache";
        String authUnencoded = USER + ":" + PASSWORD;
        String authEncoded = Base64.encodeBase64String(Strings.toByteArray(authUnencoded));
        HttpExecutor executor = factory.getHttpExecutor(getProps());
        HttpRequest executorRequest = new HttpRequest.Builder().headers(ImmutableMap.of("RequestHeader", "RequestHeaderValue"))
                .method("GET")
                .uri(baseUrl.toURI())
                .credentials(new UsernamePassword("brooklyn", "apache"))
                .build();
        HttpResponse executorResponse = executor.execute(executorRequest);

        RecordedRequest recordedFirstRequest = server.takeRequest();
        RecordedRequest recordedSecondRequest = server.takeRequest();

        assertRequestAndResponse(recordedFirstRequest,
                firstServerResponse,
                executorRequest,
                new HttpResponse.Builder()
                        .header("WWW-Authenticate", "Basic realm=\"User Visible Realm\"")
                        .header("Content-Length", "" + "Not Authenticated".length())
                        .content("Not Authenticated".getBytes())
                        .build());
        ArrayListMultimap newHeaders = ArrayListMultimap.create(executorRequest.headers());
        newHeaders.put("Authorization", "Basic " + authEncoded);
        assertRequestAndResponse(recordedSecondRequest,
                secondServerResponse,
                new HttpRequest.Builder().from((HttpRequestImpl)executorRequest).headers(newHeaders).build(),
                executorResponse);
    }

    private void assertRequestAndResponse(RecordedRequest serverRequest, MockResponse serverResponse, HttpRequest executorRequest, HttpResponse executorResponse) throws Exception {
        assertEquals(serverRequest.getMethod(), executorRequest.method());
        Function<Map.Entry<String, String>, String> headersMapper = new Function<Map.Entry<String, String>, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Map.Entry<String, String> input) {
                return input.getKey() + ": " + input.getValue();
            }
        };
        assertTrue(serverRequest.getHeaders().containsAll(Collections2.transform(executorRequest.headers().entries(), headersMapper)));
        assertEquals(serverRequest.getPath(), executorRequest.uri().getPath());
        if (executorRequest.body() != null) {
            assertEquals(serverRequest.getBody(), executorRequest.body());
        } else {
            assertEquals(serverRequest.getBody(), new byte[0]);
        }

        assertEquals(serverResponse.getBody(), ByteStreams.toByteArray(executorResponse.getContent()));
        assertTrue(serverResponse.getHeaders().containsAll(Collections2.transform(executorResponse.headers().entries(), headersMapper)));
        assertTrue(Collections2.transform(executorResponse.headers().entries(), headersMapper).containsAll(serverResponse.getHeaders()));
    }

    protected Map<?, ?> getProps() {
        return ImmutableMap.of();
    }
}
