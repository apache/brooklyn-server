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
        useServerForTest(BrooklynRestApiLauncher.launcher()
            .withoutJsgui()
            .start());

        HttpToolResponse response = HttpTool.httpGet(
            client(), URI.create(getBaseUriRest() + "server/status"),
            ImmutableMap.<String,String>of(
                CsrfTokenFilter.REQUEST_CSRF_TOKEN_HEADER, CsrfTokenFilter.REQUEST_CSRF_TOKEN_HEADER_TRUE ));
        
        // comes back okay
        assertOkayResponse(response, "MASTER");
        
        System.out.println(response.getHeaderLists());
        List<String> tokens = response.getHeaderLists().get(CsrfTokenFilter.REQUIRED_CSRF_TOKEN_HEADER);
        String token = Iterables.getOnlyElement(tokens);
        Assert.assertNotNull(token);
        
        List<String> cookies = response.getHeaderLists().get(HttpHeaders.SET_COOKIE);
        String cookie = Iterables.getOnlyElement(cookies);
        Assert.assertNotNull(cookie);

        // can post subsequently with token
        response = HttpTool.httpPost(
            client(), URI.create(getBaseUriRest() + "script/groovy"),
            ImmutableMap.<String,String>of(
                HttpHeaders.CONTENT_TYPE, "application/text",
                HttpHeaders.COOKIE, cookie,
                CsrfTokenFilter.REQUIRED_CSRF_TOKEN_HEADER, token ),
            "return 0;".getBytes());
        assertOkayResponse(response, "{\"result\":\"0\"}");

        // but fails without token
        response = HttpTool.httpPost(
            client(), URI.create(getBaseUriRest() + "script/groovy"),
            ImmutableMap.<String,String>of(
                HttpHeaders.COOKIE, cookie,
                HttpHeaders.CONTENT_TYPE, "application/text" ),
            "return 0;".getBytes());
        assertEquals(response.getResponseCode(), HttpStatus.SC_UNAUTHORIZED);

        // but you can get subsequently without token
        response = HttpTool.httpGet(
            client(), URI.create(getBaseUriRest() + "server/status"),
            ImmutableMap.<String,String>of(
                HttpHeaders.COOKIE, cookie ));

        assertOkayResponse(response, "MASTER");
    }

    protected HttpClient client() {
        return HttpTool.httpClientBuilder()
            .uri(getBaseUriRest(server))
            .build();
    }

    protected void assertOkayResponse(HttpToolResponse response, String expecting) {
        assertEquals(response.getResponseCode(), HttpStatus.SC_OK);
        assertEquals(response.getContentAsString(), expecting);
    }

}
