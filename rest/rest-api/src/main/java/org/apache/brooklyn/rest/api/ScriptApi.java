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

import io.swagger.annotations.*;
import org.apache.brooklyn.rest.domain.ScriptExecutionSummary;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("/script")
@Api("Scripting")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface ScriptApi {

    /** @deprecated since 0.10.0. Use constant in ScriptResource instead. */
    @Deprecated
    public static final String USER_DATA_MAP_SESSION_ATTRIBUTE = "brooklyn.script.groovy.user.data";
    /** @deprecated since 0.10.0. Use constant in ScriptResource instead. */
    @Deprecated
    public static final String USER_LAST_VALUE_SESSION_ATTRIBUTE = "brooklyn.script.groovy.user.last";

    @POST
    @Path("/groovy")
    @Consumes("application/text")
    @ApiOperation(value = "Execute a groovy script",
            response = org.apache.brooklyn.rest.domain.SensorSummary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public ScriptExecutionSummary groovy(
            @Context HttpServletRequest request,
            @ApiParam(name = "script", value = "Groovy script to execute", required = true)
            String script);
}
