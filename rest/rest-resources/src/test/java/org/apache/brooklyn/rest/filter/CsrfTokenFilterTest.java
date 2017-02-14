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
package org.apache.brooklyn.rest.filter;

import static org.testng.Assert.assertEquals;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.http.HttpStatus;

/** TODO test fails using BRRT because we have no session; see CsrfTokenFilterLauncherTest in different project,
 * and comments in here; would prefer to have this test, so keep if we can enable sessions here */
public class CsrfTokenFilterTest extends BrooklynRestResourceTest {

    public static class SampleActionResource {
        @GET
        @Path("/test-get")
        public String testGet() {
            return "got";
        }
        @POST
        @Path("/test-post")
        public String testPost() {
            return "posted";
        }
    }

    @Override
    protected void addBrooklynResources() {
        addResource(new CsrfTokenFilter());
        addResource(new SampleActionResource());
    }
    
    @Override
    protected void configureCXF(JAXRSServerFactoryBean sf) {
        super.configureCXF(sf);
        
        /*
        TODO how can we turn on sessions??
        
        // normal handler way 
        val webapp = new ServletContextHandler(ServletContextHandler.SESSIONS)
        SessionHandler sh = new SessionHandler();
        webapp.setSessionHandler()

        try {
            // suggested bean way - but only for new servers 
            sf.getBus().getExtension(JettyHTTPServerEngineFactory.class) 
                .createJettyHTTPServerEngine(8000, "http") 
                .setSessionSupport(true);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
        */
    }

//    @Test
//    public void testRequestToken() {
//        client().path("/logout").post(null);
//        Response response = request("/logout").header(
////        Response response = request("/test-get").header(
//            CsrfTokenFilter.REQUEST_CSRF_TOKEN_HEADER, CsrfTokenFilter.REQUEST_CSRF_TOKEN_HEADER_TRUE).get();
//        
//        // comes back okay
//        assertOkayResponse(response, "got");
//        
//        String token = response.getHeaderString(CsrfTokenFilter.REQUIRED_CSRF_TOKEN_HEADER);
//        Assert.assertNotNull(token);
//
//        // can get subsequently
//        response = request("/test-get").get();
//        assertOkayResponse(response, "got");
//    }

    protected void assertOkayResponse(Response response, String expecting) {
        assertEquals(response.getStatus(), HttpStatus.SC_OK);
        String content = response.readEntity(String.class);
        assertEquals(content, expecting);
    }

    protected WebClient request(String path) {
        return WebClient.create(getEndpointAddress(), clientProviders, "admin", "admin", null)
            .path(path)
            .accept(MediaType.APPLICATION_JSON_TYPE);
    }

}
