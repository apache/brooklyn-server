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
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.jaas.JaasUtils;
import org.apache.brooklyn.rest.security.provider.ExplicitUsersSecurityProvider;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.cxf.interceptor.security.JAASLoginInterceptor;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.http.HttpStatus;
import org.testng.annotations.Test;

public class EntitlementContextFilterTest extends BrooklynRestResourceTest {

    private static final String USER_PASS = "admin";

    public static class EntitlementResource {
        @GET
        @Path("/test")
        public String test() {
            WebEntitlementContext context = (WebEntitlementContext)Entitlements.getEntitlementContext();
            return context.user();
        }
    }

    @Override
    protected void configureCXF(JAXRSServerFactoryBean sf) {
        BrooklynProperties props = (BrooklynProperties)getManagementContext().getConfig();
        props.put(BrooklynWebConfig.USERS, USER_PASS);
        props.put(BrooklynWebConfig.PASSWORD_FOR_USER(USER_PASS), USER_PASS);
        props.put(BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE, new ExplicitUsersSecurityProvider(getManagementContext()));

        super.configureCXF(sf);

        JaasUtils.init(getManagementContext());

        JAASLoginInterceptor jaas = new JAASLoginInterceptor();
        jaas.setContextName("webconsole");
        sf.getInInterceptors().add(jaas);

    }

    @Override
    protected void addBrooklynResources() {
        addResource(new RequestTaggingRsFilter());
        addResource(new EntitlementContextFilter());
        addResource(new EntitlementResource());
    }

    @Test
    public void testEntitlementContextSet() {
        Response response = fetch("/test");
        assertEquals(response.getStatus(), HttpStatus.SC_OK);
        String tag = response.readEntity(String.class);
        assertEquals(tag, USER_PASS);
    }

    protected Response fetch(String path) {
        WebClient resource = WebClient.create(getEndpointAddress(), clientProviders, USER_PASS, USER_PASS, null)
            .path(path)
            .accept(MediaType.APPLICATION_JSON_TYPE);
        Response response = resource.get();
        return response;
    }

}
