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
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.rest.domain.TaskSummary;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.brooklyn.util.core.task.TaskInternal;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/activities")
@Api("Activities")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface ActivityApi {

    @GET
    @Path("/{task}")
    @ApiOperation(value = "Fetch task details", response = org.apache.brooklyn.rest.domain.TaskSummary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find task"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response get(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(name = "timeout", value = "Delay before server should block until task completes before returinging (in millis if no unit specified): " +
                    "'0' means 'always' return task activity ID and is the default; " +
                    "'never' or '-1' means wait until the task finishes (or HTTP times out); " +
                    "and e.g. '1000' or '1s' will return the task as soon as it completes or after one second whichever is sooner, with 202 returned if the task is still ongoing",
                    required = false, defaultValue = "0")
            @QueryParam("timeout")
            String timeout,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @GET
    @Path("/{task}/children")
    @ApiOperation(value = "Fetch list of children tasks of this task")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find task"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<TaskSummary> children(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(value = "Whether to include non-subtask backgrounded tasks submitted by this task", required = false)
            @QueryParam("includeBackground") @DefaultValue("false") Boolean includeBackground,
            @ApiParam(value = "Max number of tasks to include, or -1 for all (default 200)", required = false)
            @QueryParam("limit") @DefaultValue("200") int limit,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @GET
    @Path("/{task}/children/recurse")
    @ApiOperation(
            value = "Fetch all child tasks and their descendants with details as Map<String,TaskSummary> map key == Task ID",
            response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find task"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Map<String,TaskSummary> getAllChildrenAsMap(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(value = "Max number of tasks to include, or -1 for all (default 200)", required = false)
            @QueryParam("limit") @DefaultValue("200") int limit,
            @ApiParam(value = "Max depth to traverse, or -1 for all (default)", required = false)
            @QueryParam("maxDepth") @DefaultValue("-1") int maxDepth,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @GET
    @Path("/{task}/stream/{streamId}")
    @ApiOperation(value = "Return the contents of the given stream")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find task"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Produces(MediaType.TEXT_PLAIN)
    public String stream(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(value = "Stream ID", required = true) @PathParam("streamId") String streamId);

    @POST
    @Path("/{task}/cancel")
    @ApiOperation(value = "Sends a cancel to a task. Returns true if it was in a cancellable state (running and not already cancelled). It is task dependent at what point tasks stop running when cancelled.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find task"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Boolean cancel(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(value = "Whether to skip sending an interrupt to the task; if true, tasks may continue to run but not be easily trackable, so use with care, only with tasks that check their cancelled status and will clean up nicely.", required = false)
            @QueryParam("noInterrupt") @DefaultValue("false") Boolean noInterrupt);
}
