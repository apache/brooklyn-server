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
package org.apache.brooklyn.rest.api;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/logbook")
@Api("Logbook")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface LogbookApi {

    @POST
    @ApiOperation(value = "Execute query for getting log data",
            response = org.apache.brooklyn.rest.domain.SensorSummary.class)
    Response logbookQuery(
            @Context HttpServletRequest request,
            @ApiParam(name = "query", value = "Query filter", required = true)
                    String query);

    @GET
    @Path("/getEntries")
    @ApiOperation(value = "Returns a range of stored log entries",
            response = org.apache.brooklyn.rest.domain.SensorSummary.class)
    Response getEntries(
            @Context HttpServletRequest request,
            @ApiParam(name = "from", value = "Initial value for the range")
            @QueryParam("from") Integer from,
            @ApiParam(name = "numberOfItems", value = "Number of items to return")
            @QueryParam("numberOfItems") @DefaultValue("25") Integer numberOfItems);

    @GET
    @Path("/tail")
    @ApiOperation(value = "Returns the last requested entries",
            response = org.apache.brooklyn.rest.domain.SensorSummary.class)
    Response tail(
            @Context HttpServletRequest request,
            @ApiParam(name = "numberOfItems", value = "Number of items to return")
            @QueryParam("numberOfItems") @DefaultValue("25") Integer numberOfItems);
}
