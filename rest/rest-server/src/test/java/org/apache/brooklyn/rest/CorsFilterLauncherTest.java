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
package org.apache.brooklyn.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.rest.filter.CorsImplSupplierFilter;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.http.HttpTool;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;
import org.testng.annotations.Test;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.apache.brooklyn.rest.CsrfTokenFilterLauncherTest.assertOkayResponse;
import static org.apache.cxf.rs.security.cors.CorsHeaderConstants.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * It is important to first execute tests where CorsFilterImpl is enabled
 * after that execute tests where CorsImplSupplierFilter is disabled.
 * This is because CorsImplSupplierFilter is designed to be enabled and disabled only on startup.
 **/
public class CorsFilterLauncherTest extends BrooklynRestApiLauncherTestFixture {
    @Test
    public void test1CorsIsEnabledOnOneOriginGET() throws IOException {
        final String shouldAllowOrigin = "http://foo.bar.com";
        final String thirdPartyOrigin = "http://foo.bar1.com";
        setCorsFilterFeature(true, ImmutableList.of(shouldAllowOrigin));
    
        HttpClient client = client();
        // preflight request
        HttpToolResponse response = HttpTool.execAndConsume(client, httpOptionsRequest("server/status", "GET", shouldAllowOrigin));
        assertAcAllowOrigin(response, shouldAllowOrigin, "GET");
        assertOkayResponse(response, "");
    
        HttpUriRequest httpRequest = RequestBuilder.get(getBaseUriRest() + "server/status")
                .addHeader("Origin", shouldAllowOrigin)
                .addHeader(HEADER_AC_REQUEST_METHOD, "GET")
                .build();
        response = HttpTool.execAndConsume(client, httpRequest);
        assertAcAllowOrigin(response, shouldAllowOrigin, "GET", false);
        assertOkayResponse(response, "MASTER");
    
        // preflight request
        response = HttpTool.execAndConsume(client, httpOptionsRequest("server/status", "GET", thirdPartyOrigin));
        assertAcNotAllowOrigin(response);
        assertOkayResponse(response, "");

        httpRequest = RequestBuilder.get(getBaseUriRest() + "server/status")
                .addHeader("Origin", thirdPartyOrigin)
                .addHeader(HEADER_AC_REQUEST_METHOD, "GET")
                .build();
        response = HttpTool.execAndConsume(client, httpRequest);
        assertAcNotAllowOrigin(response);
        assertOkayResponse(response, "MASTER");
    }
    
    @Test
    public void test1CorsIsEnabledOnOneOriginPOST() throws IOException {
        final String shouldAllowOrigin = "http://foo.bar.com";
        final String thirdPartyOrigin = "http://foo.bar1.com";
        setCorsFilterFeature(true, ImmutableList.of(shouldAllowOrigin));
        HttpClient client = client();
        // preflight request
        HttpToolResponse response = HttpTool.execAndConsume(client, httpOptionsRequest("script/groovy", "POST", shouldAllowOrigin));
        assertAcAllowOrigin(response, shouldAllowOrigin, "POST");
        assertOkayResponse(response, "");
        
        response = HttpTool.httpPost(
                client, URI.create(getBaseUriRest() + "script/groovy"),
                ImmutableMap.<String,String>of(
                        "Origin", shouldAllowOrigin,
                        HttpHeaders.CONTENT_TYPE, "application/text"),
                "return 0;".getBytes());
        assertAcAllowOrigin(response, shouldAllowOrigin, "POST", false);
        assertOkayResponse(response, "{\"result\":\"0\"}");
    
        // preflight request
        response = HttpTool.execAndConsume(client, httpOptionsRequest("script/groovy", "POST", thirdPartyOrigin));
        assertAcNotAllowOrigin(response);
        assertOkayResponse(response, "");
    
        response = HttpTool.httpPost(
                client, URI.create(getBaseUriRest() + "script/groovy"),
                ImmutableMap.<String,String>of(
                        "Origin", thirdPartyOrigin,
                        HttpHeaders.CONTENT_TYPE, "application/text"),
                "return 0;".getBytes());
        assertAcNotAllowOrigin(response);
        assertOkayResponse(response, "{\"result\":\"0\"}");
    }

    @Test
    public void test1CorsIsEnabledOnAllDomainsGET() throws IOException {
        final String shouldAllowOrigin = "http://foo.bar.com";
        setCorsFilterFeature(true, ImmutableList.<String>of());
        HttpClient client = client();
        // preflight request
        HttpToolResponse response = HttpTool.execAndConsume(client, httpOptionsRequest("server/status", "GET", shouldAllowOrigin));
        List<String> accessControlAllowOrigin = response.getHeaderLists().get(HEADER_AC_ALLOW_ORIGIN);
        assertEquals(accessControlAllowOrigin.size(), 1);
        assertEquals(accessControlAllowOrigin.get(0), "*", "Should allow GET requests made from " + shouldAllowOrigin);

        assertEquals(response.getHeaderLists().get(HEADER_AC_ALLOW_HEADERS).size(), 1);
        assertEquals(response.getHeaderLists().get(HEADER_AC_ALLOW_HEADERS).get(0), "x-csrf-token", "Should have asked and allowed x-csrf-token header from " + shouldAllowOrigin);
        assertOkayResponse(response, "");

        HttpUriRequest httpRequest = RequestBuilder.get(getBaseUriRest() + "server/status")
                .addHeader("Origin", shouldAllowOrigin)
                .addHeader(HEADER_AC_REQUEST_METHOD, "GET")
                .build();
        response = HttpTool.execAndConsume(client, httpRequest);
        accessControlAllowOrigin = response.getHeaderLists().get(HEADER_AC_ALLOW_ORIGIN);
        assertEquals(accessControlAllowOrigin.size(), 1);
        assertEquals(accessControlAllowOrigin.get(0), "*", "Should allow GET requests made from " + shouldAllowOrigin);
        assertOkayResponse(response, "MASTER");
    }
    
    @Test
    public void test1CorsIsEnabledOnAllDomainsByDefaultPOST() throws IOException {
        final String shouldAllowOrigin = "http://foo.bar.com";
        BrooklynFeatureEnablement.enable(BrooklynFeatureEnablement.FEATURE_CORS_CXF_PROPERTY);
        BrooklynRestApiLauncher apiLauncher = baseLauncher().withoutJsgui();
        // In this test, management context has no value set for Allowed Origins.
        ManagementContext mgmt = LocalManagementContextForTests.builder(true)
                .useAdditionalProperties(MutableMap.<String, Object>of(
                        BrooklynFeatureEnablement.FEATURE_CORS_CXF_PROPERTY, true)
                ).build();
        apiLauncher.managementContext(mgmt);
        useServerForTest(apiLauncher.start());
        HttpClient client = client();
        // preflight request
        HttpToolResponse response = HttpTool.execAndConsume(client, httpOptionsRequest("script/groovy", "POST", shouldAllowOrigin));
        List<String> accessControlAllowOrigin = response.getHeaderLists().get(HEADER_AC_ALLOW_ORIGIN);
        assertEquals(accessControlAllowOrigin.size(), 1);
        assertEquals(accessControlAllowOrigin.get(0), "*", "Should allow POST requests made from " + shouldAllowOrigin);
        assertEquals(response.getHeaderLists().get(HEADER_AC_ALLOW_HEADERS).size(), 1);
        assertEquals(response.getHeaderLists().get(HEADER_AC_ALLOW_HEADERS).get(0), "x-csrf-token", "Should have asked and allowed x-csrf-token header from " + shouldAllowOrigin);
        assertOkayResponse(response, "");
        
        response = HttpTool.httpPost(
                client, URI.create(getBaseUriRest() + "script/groovy"),
                ImmutableMap.<String,String>of(
                        "Origin", shouldAllowOrigin,
                        HttpHeaders.CONTENT_TYPE, "application/text"),
                "return 0;".getBytes());
        accessControlAllowOrigin = response.getHeaderLists().get(HEADER_AC_ALLOW_ORIGIN);
        assertEquals(accessControlAllowOrigin.size(), 1);
        assertEquals(accessControlAllowOrigin.get(0), "*", "Should allow GET requests made from " + shouldAllowOrigin);
        assertOkayResponse(response, "{\"result\":\"0\"}");
    }

    @Test
    public void test2CorsIsDisabled() throws IOException {
        BrooklynFeatureEnablement.disable(BrooklynFeatureEnablement.FEATURE_CORS_CXF_PROPERTY);
        final String shouldAllowOrigin = "http://foo.bar.com";
        setCorsFilterFeature(false, null);
    
        HttpClient client = client();
        HttpToolResponse response = HttpTool.execAndConsume(client, httpOptionsRequest("server/status", "GET", shouldAllowOrigin));
        assertAcNotAllowOrigin(response);
        assertOkayResponse(response, "");
    
        response = HttpTool.execAndConsume(client, httpOptionsRequest("script/groovy", shouldAllowOrigin, "POST"));
        assertAcNotAllowOrigin(response);
        assertOkayResponse(response, "");
    }
    
    // TODO figure out a way to launch test in karaf like container supplying config from OSGI Persistency Identity file
    private void setCorsFilterFeature(boolean enable, List<String> allowedOrigins) {
        if (enable) {
            BrooklynFeatureEnablement.enable(BrooklynFeatureEnablement.FEATURE_CORS_CXF_PROPERTY);
        } else {
            BrooklynFeatureEnablement.disable(BrooklynFeatureEnablement.FEATURE_CORS_CXF_PROPERTY);
        }
        BrooklynRestApiLauncher apiLauncher = baseLauncher()
                .withoutJsgui();
        ManagementContext mgmt = LocalManagementContextForTests.builder(true)
                .useAdditionalProperties(MutableMap.<String, Object>of(
                        BrooklynFeatureEnablement.FEATURE_CORS_CXF_PROPERTY, enable,
                        BrooklynFeatureEnablement.FEATURE_CORS_CXF_PROPERTY + "." + CorsImplSupplierFilter.ALLOW_ORIGINS.getName(), allowedOrigins)
                ).build();
        apiLauncher.managementContext(mgmt);
        useServerForTest(apiLauncher.start());
    }

    protected HttpClient client() {
        return HttpClients.createMinimal();
    }
    
    public void assertAcAllowOrigin(HttpToolResponse response, String shouldAllowOrigin, String method) {
        assertAcAllowOrigin(response, shouldAllowOrigin, method, true);
    }

    public void assertAcAllowOrigin(HttpToolResponse response, String shouldAllowOrigin, String method, boolean preflightRequest) {
        List<String> accessControlAllowOrigin = response.getHeaderLists().get(HEADER_AC_ALLOW_ORIGIN);
        assertEquals(accessControlAllowOrigin.size(), 1);
        assertEquals(accessControlAllowOrigin.get(0), shouldAllowOrigin, "Should allow " + method + " requests made from " + shouldAllowOrigin);
    
        if (preflightRequest) {
            List<String> accessControlAllowHeaders = response.getHeaderLists().get(HEADER_AC_ALLOW_HEADERS);
            assertEquals(accessControlAllowHeaders.size(), 1);
            assertEquals(accessControlAllowHeaders.get(0), "x-csrf-token", "Should have asked and allowed x-csrf-token header from " + shouldAllowOrigin);
        }
    }
    
    public void assertAcNotAllowOrigin(HttpToolResponse response) {
        List<String> accessControlAllowOrigin = response.getHeaderLists().get(HEADER_AC_ALLOW_ORIGIN);
        assertNull(accessControlAllowOrigin, "Access Control Header should not be available.");
    }
    
    private HttpUriRequest httpOptionsRequest(String apiCall, String acRequestMethod, String origin) {
        return RequestBuilder.options(getBaseUriRest() + apiCall)
                .addHeader("Origin", origin)
                .addHeader(HEADER_AC_REQUEST_HEADERS, "x-csrf-token")
                .addHeader(HEADER_AC_REQUEST_METHOD, acRequestMethod)
                .build();
    }
}
