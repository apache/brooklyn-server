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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.http.HttpStatus;
import org.testng.annotations.Test;

public class RequestTaggingRsFilterTest extends BrooklynRestResourceTest {

    @Path("/tag")
    @Produces(MediaType.APPLICATION_JSON)
    public static class TagResource {
        @GET
        public String tag() {
            return RequestTaggingRsFilter.getTag();
        }
    }

    @Override
    protected void addBrooklynResources() {
        addResource(new RequestTaggingRsFilter());
        addResource(new TagResource());
    }

    @Test
    public void testTaggingFilter() {
        String tag1 = fetchTag();
        String tag2 = fetchTag();
        assertNotEquals(tag1, tag2);
    }

    private String fetchTag() {
        Response response = fetch("/tag");
        assertEquals(response.getStatus(), HttpStatus.SC_OK);
        String tag = (String) response.readEntity(String.class);
        assertNotNull(tag);
        return tag;
    }

    protected Response fetch(String path) {
        WebClient resource = client().path(path)
                .accept(MediaType.APPLICATION_JSON_TYPE);
        Response response = resource.get();
        return response;
    }

}
