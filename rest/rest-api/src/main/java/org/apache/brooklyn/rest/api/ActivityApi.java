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

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/activities")
@Api("Activities")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface ActivityApi {

    @GET
    @Path("/{task}")
    @ApiOperation(value = "Fetch task details", response = org.apache.brooklyn.rest.domain.TaskSummary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find task")
    })
    public TaskSummary get(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId);

    @GET
    @Path("/{task}/children")
    @ApiOperation(value = "Fetch list of children tasks of this task")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find task")
    })
    public List<TaskSummary> children(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(value = "Whether to include non-subtask backgrounded tasks submitted by this task", required = false)
            @QueryParam("includeBackground") @DefaultValue("false") Boolean includeBackground);

    @GET
    @Path("/{task}/children/recurse")
    @ApiOperation(
            value = "Fetch all child tasks and their descendants with details as Map<String,TaskSummary> map key == Task ID",
            response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find task")
    })
    public Map<String,TaskSummary> getAllChildrenAsMap(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(value = "Max number of tasks to include, or -1 for all (default 200)", required = false) 
            @QueryParam("limit") @DefaultValue("200") int limit,
            @ApiParam(value = "Max depth to traverse, or -1 for all (default)", required = false) 
            @QueryParam("maxDepth") @DefaultValue("-1") int maxDepth);
    
    /** @deprecated since 0.12.0 use {@link #getAllChildrenAsMap(String, int, int)} with depth -1 */
    @Deprecated
    public Map<String,TaskSummary> getAllChildrenAsMap(String taskId);

    @GET
    @Path("/{task}/stream/{streamId}")
    @ApiOperation(value = "Return the contents of the given stream")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find task or stream")
    })
    public String stream(
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(value = "Stream ID", required = true) @PathParam("streamId") String streamId);
}
