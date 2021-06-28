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
package org.apache.brooklyn.rest.resources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.rest.api.LogbookApi;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.http.HttpStatus;
import org.testng.annotations.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;

/**
 * Tests the {@link LogbookApi} implementation.
 */
@Test(singleThreaded = true)
public class LogbookResourceTest extends BrooklynRestResourceTest {

    @Test
    public void testQueryLogbookNoArgs(){

        // Post null query.
        Response response = client()
                .path("/logbook")
                .accept(MediaType.APPLICATION_JSON)
                .post(null);

        assertEquals(response.getStatus(), HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testQueryLogbookUnknownArgs() throws IOException {

        // Prepare query with unknown args.
        ImmutableMap qb = ImmutableMap.builder()
                .put("unknownArg", false)
                .build();

        Response response = client()
                .path("/logbook")
                .accept(MediaType.APPLICATION_JSON)
                .post(toJsonEntity(qb));

        assertEquals(response.getStatus(), HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    @Test
    public void testQueryLogbookValidArgs() throws IOException {

        // Prepare a valid query.
        ImmutableMap qb = ImmutableMap.builder()
                .put("numberOfItems", 3)
                .put("reverseOrder", false)
                .put("levels", ImmutableList.of("WARN", "DEBUG"))
                .build();

        Response response = client()
                .path("/logbook")
                .accept(MediaType.APPLICATION_JSON)
                .post(toJsonEntity(qb));

        assertEquals(response.getStatus(), HttpStatus.SC_OK);

        ArrayList<BrooklynLogEntry> brooklynLogEntries = response.readEntity(new GenericType<ArrayList<BrooklynLogEntry>>() {});
        assertEquals(brooklynLogEntries.size(), 3);
    }

    // TODO: complete this test, find a way to test the un-entitled access.
    @Test(groups="WIP")
    public void testQueryLogbookNotEntitled() throws Exception {

        // Prepare a valid query.
        ImmutableMap qb = ImmutableMap.builder()
                .put("numberOfItems", 3)
                .put("reverseOrder", false)
                .put("levels", ImmutableList.of("WARN", "DEBUG"))
                .build();

        // Access the logbook resource with un-entitled user.
        WebClient resource = WebClient.create(getEndpointAddress(), clientProviders, "user", "password", null)
                .path("/logbook")
                .accept(MediaType.APPLICATION_JSON);
        Response response = resource.post(toJsonEntity(qb));

        assertEquals(response.getStatus(),  HttpStatus.SC_UNAUTHORIZED);
    }
}
