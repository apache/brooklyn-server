package org.apache.brooklyn.rest;
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


import static org.testng.Assert.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import org.apache.brooklyn.rest.filter.CsrfTokenFilter;
import org.apache.brooklyn.rest.filter.CsrfTokenFilterTest;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/** TODO would prefer to run without launcher, as in {@link CsrfTokenFilterTest}; see comments there
 * (but tests more fleshed out here) */
public class CsrfTokenFilterLauncherTest extends BrooklynRestApiLauncherTestFixture {

    @Test
    public void testRequestToken() {
        useServerForTest(baseLauncher()
            .withoutJsgui()
            .start());

        HttpClient client = client();
        
        HttpToolResponse response = HttpTool.httpGet(
            client, URI.create(getBaseUriRest() + "server/status"),
            ImmutableMap.<String,String>of(
                CsrfTokenFilter.CSRF_TOKEN_REQUIRED_HEADER, CsrfTokenFilter.CsrfTokenRequiredForRequests.WRITE.toString()));
        
        // comes back okay
        assertOkayResponse(response, "MASTER");
        
        Map<String, List<String>> cookies = response.getCookieKeyValues();
        String token = Iterables.getOnlyElement(cookies.get(CsrfTokenFilter.CSRF_TOKEN_VALUE_COOKIE));
        Assert.assertNotNull(token);
        String tokenAngular = Iterables.getOnlyElement(cookies.get(CsrfTokenFilter.CSRF_TOKEN_VALUE_COOKIE_ANGULAR_NAME));
        Assert.assertEquals(token, tokenAngular);
        
        // can post subsequently with token
        response = HttpTool.httpPost(
            client, URI.create(getBaseUriRest() + "script/groovy"),
            ImmutableMap.<String,String>of(
                HttpHeaders.CONTENT_TYPE, "application/text",
                CsrfTokenFilter.CSRF_TOKEN_VALUE_HEADER, token ),
            "return 0;".getBytes());
        assertOkayResponse(response, "{\"result\":\"0\"}");

        // but fails without token
        response = HttpTool.httpPost(
            client, URI.create(getBaseUriRest() + "script/groovy"),
            ImmutableMap.<String,String>of(
                HttpHeaders.CONTENT_TYPE, "application/text" ),
            "return 0;".getBytes());
        assertEquals(response.getResponseCode(), HttpStatus.SC_UNAUTHORIZED);

        // can get without token
        response = HttpTool.httpGet(
            client, URI.create(getBaseUriRest() + "server/status"),
            ImmutableMap.<String,String>of());
        assertOkayResponse(response, "MASTER");
        
        // but if we set required ALL then need a token to get
        response = HttpTool.httpGet(
            client, URI.create(getBaseUriRest() + "server/status"),
            ImmutableMap.<String,String>of(
                CsrfTokenFilter.CSRF_TOKEN_REQUIRED_HEADER, CsrfTokenFilter.CsrfTokenRequiredForRequests.ALL.toString().toLowerCase()
                ));
        assertOkayResponse(response, "MASTER");
        response = HttpTool.httpGet(
            client, URI.create(getBaseUriRest() + "server/status"),
            ImmutableMap.<String,String>of());
        assertEquals(response.getResponseCode(), HttpStatus.SC_UNAUTHORIZED);
        
        // however note if we use a new client, with no session, then we can post with no token
        // (ie we don't guard against CSRF if your brooklyn is unsecured)
        client = client();
        response = HttpTool.httpPost(
            client, URI.create(getBaseUriRest() + "script/groovy"),
            ImmutableMap.<String,String>of(
                HttpHeaders.CONTENT_TYPE, "application/text" ),
            "return 0;".getBytes());
        assertOkayResponse(response, "{\"result\":\"0\"}");
    }

    protected HttpClient client() {
        return HttpTool.httpClientBuilder()
            .uri(getBaseUriRest(server))
            .build();
    }

    public static void assertOkayResponse(HttpToolResponse response, String expecting) {
        assertEquals(response.getResponseCode(), HttpStatus.SC_OK);
        assertEquals(response.getContentAsString(), expecting);
    }
}
