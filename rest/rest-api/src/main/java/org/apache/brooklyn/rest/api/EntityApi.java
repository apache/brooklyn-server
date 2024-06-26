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
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.rest.domain.RelationSummary;
import org.apache.brooklyn.rest.domain.EntitySummary;
import org.apache.brooklyn.rest.domain.LocationSummary;
import org.apache.brooklyn.rest.domain.TaskSummary;

import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

@Path("/applications/{application}/entities")
@Api("Entities")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface EntityApi {

    @GET
    @ApiOperation(value = "Fetch the list of children entities directly under the root of an application",
            response = org.apache.brooklyn.rest.domain.EntitySummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<EntitySummary> list(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application);

    @GET
    @Path("/{entity}")
    @ApiOperation(value = "Fetch details of an entity",
            response = org.apache.brooklyn.rest.domain.EntitySummary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public EntitySummary get(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity);

    // TODO rename as "/children" ?
    @GET
    @ApiOperation(value = "Fetch the list of children of an entity",
            response = org.apache.brooklyn.rest.domain.EntitySummary.class)
    @Path("/{entity}/children")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<EntitySummary> getChildren(
            @PathParam("application") final String application,
            @PathParam("entity") final String entity);

    @GET
    @ApiOperation(value = "Fetch the list of relations of an entity",
            response = RelationSummary.class)
    @Path("/{entity}/relations")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    List<RelationSummary> getRelations(
            @PathParam("application") final String applicationId,
            @PathParam("entity") final String entityId);

    @POST
    @ApiOperation(value = "Add a child or children to this entity given a YAML spec",
            response = org.apache.brooklyn.rest.domain.TaskSummary.class)
    @Consumes({"application/x-yaml",
            // see http://stackoverflow.com/questions/332129/yaml-mime-type
            "text/yaml", "text/x-yaml", "application/yaml", MediaType.APPLICATION_JSON})
    @Path("/{entity}/children")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Created"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response addChildren(
            @PathParam("application") final String application,
            @PathParam("entity") final String entity,

            @ApiParam(
                    name = "start",
                    value = "Whether to automatically start this child; if omitted, true for Startable entities")
            @QueryParam("start") final Boolean start,

            @ApiParam(name = "timeout", value = "Delay before server should respond with incomplete activity task, rather than completed task: " +
                    "'never' means block until complete; " +
                    "'0' means return task immediately; " +
                    "and e.g. '20ms' (the default) will wait 20ms for completed task information to be available", 
                    required = false, defaultValue = "20ms")
            @QueryParam("timeout") final String timeout,

            @ApiParam(
                    name = "childrenSpec",
                    value = "Entity spec in CAMP YAML format (including 'services' root element)",
                    required = true)
            String yaml);

    @GET
    @Path("/{entity}/activities")
    @ApiOperation(value = "Fetch list of tasks for this entity")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<TaskSummary> listTasks(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId,
            @ApiParam(value = "Max number of tasks, or -1 for all (default 200)", required = false) 
            @QueryParam("limit") @DefaultValue("200") int limit,
            @ApiParam(value = "Whether to include subtasks recursively across different entities (default false)", required = false)
            @QueryParam("recurse") @DefaultValue("false") Boolean recurse,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @GET
    @Path("/{entity}/activities/{task}")
    @ApiOperation(value = "Fetch task details", response = org.apache.brooklyn.rest.domain.TaskSummary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find application, entity or task"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Produces("text/json")
    public TaskSummary getTask(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") final String entityToken,
            @ApiParam(value = "Task ID", required = true) @PathParam("task") String taskId,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @GET
    @ApiOperation(value = "Returns an icon for the entity, if defined")
    @Path("/{entity}/icon")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response getIcon(
            @PathParam("application") final String application,
            @PathParam("entity") final String entity);

    @GET
    @Path("/{entity}/tags")
    @ApiOperation(value = "Fetch list of tags on this entity")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<Object> listTags(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId);

    // this feels too dangerous; see other items
//    @POST
//    @Path("/{entity}/tags")
//    @ApiOperation(value = "Set the tags on this entity (replaces all, use with care)")
//    @ApiResponses(value = {
//            @ApiResponse(code = 404, message = "Could not find application or entity")
//    })
//    public void setTags(
//            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
//            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId,
//            @ApiParam(value = "Tags to set", required = true) List<Object> tags);

    @POST
    @Path("/{entity}/tag/add")
    @ApiOperation(value = "Add a tag on this entity")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "No Content"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void addTag(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId,
            @ApiParam(value = "Tag to add", required = true) Object tag);

    @POST
    @Path("/{entity}/tag/delete")
    @ApiOperation(value = "Delete a tag on this entity, returning whether the tag was found (and deleted)")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public boolean deleteTag(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId,
            @ApiParam(value = "Tag to delete", required = true) Object tag);

    @POST
    @Path("/{entity}/tag/upsert/{tagKey}")
    @ApiOperation(value = "Inserts a tag which is a single-key map with the given key (path parameter) and value (post body), removing any existing tag matching the key")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "No Content"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void upsertTag(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("tagKey") String tagKey,
            @ApiParam(value = "Tag map value to upsert for the given key", required = true) Object tagValue);

    @GET
    @Path("/{entity}/tag/get/{tagKey}")
    @ApiOperation(value = "Returns the tag value for a tag which is a single-key map with the given key, or null (not 404 for missing tag key)")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Object getTag(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("tagKey") String tagKey);

    @POST
    @ApiOperation(
            value = "Rename an entity"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Undefined application or entity"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/{entity}/name")
    public Response rename(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") final String applicationId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") final String entityId,
            @ApiParam(value = "New name for this entity", required = true) @QueryParam("name") final String name);

    @POST
    @ApiOperation(
            value = "Expunge an entity",
            response = org.apache.brooklyn.rest.domain.TaskSummary.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Accepted. The entity is submitted for expunging."),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/{entity}/expunge")
    public Response expunge(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") final String applicationId, 
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") final String entityId, 
            @ApiParam(value = "Whether to gracefully release all resources (failing and keeping if unsuccessful)", required = true) @QueryParam("release") final boolean release);

    @GET
    @Path("/{entity}/descendants")
    @ApiOperation(value = "Fetch entity info for all (or filtered) descendants",
            response = org.apache.brooklyn.rest.domain.EntitySummary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<EntitySummary> getDescendants(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity,
            @ApiParam(value="Regular expression for an entity type which must be matched", required=false)
            @DefaultValue(".*")
            @QueryParam("typeRegex") String typeRegex);

    @GET
    @Path("/{entity}/descendants/sensor/{sensor}")
    @ApiOperation(value = "Fetch values of a given sensor for all (or filtered) descendants")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Map<String,Object> getDescendantsSensor(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity,
            @ApiParam(value = "Sensor name", required = true)
            @PathParam("sensor") String sensor,
            @ApiParam(value="Regular expression applied to filter descendant entities based on their type", required=false)
            @DefaultValue(".*")
            @QueryParam("typeRegex") String typeRegex,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @GET
    @Path("/{entity}/locations")
    @ApiOperation(value = "List the locations set on the entity")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<LocationSummary> getLocations(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity);

    // see http://stackoverflow.com/questions/332129/yaml-mime-type for "@Produces"
    @GET
    @Path("/{entity}/spec")
    @ApiOperation(value = "Get the YAML spec used to create the entity, if available")
    @Produces({"text/x-yaml", "application/x-yaml", "text/yaml", "text/plain", "application/yaml", MediaType.TEXT_PLAIN})
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public String getSpec(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity);

    @GET
    @Path("/{entity}/speclist")
    @ApiOperation(value = "Get the list of YAML spec used to create the entity, if available")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<Object> getSpecList(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity);

    @GET
    @Path("/{entity}/workflows")
    @ApiOperation(value = "Get all workflows stored on this entity", response = WorkflowExecutionContext.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response getWorkflows(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @GET
    @Path("/{entity}/workflows/{workflowId}")
    @ApiOperation(value = "Get a workflow on this entity", response = WorkflowExecutionContext.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response getWorkflow(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity,
            @ApiParam(value = "Workflow ID", required = true)
            @PathParam("workflowId") String workflowId,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @DELETE
    @Path("/{entity}/workflows/{workflowId}")
    @ApiOperation(value = "Delete a workflow on this entity, causing it not to be retained. Not supported for ongoing workflows (cancel first).", response = WorkflowExecutionContext.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response deleteWorkflow(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity,
            @ApiParam(value = "Workflow ID", required = true)
            @PathParam("workflowId") String workflowId,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);


    @POST
    @ApiOperation(value = "Run a workflow on this entity from a YAML workflow spec",
            response = org.apache.brooklyn.rest.domain.TaskSummary.class)
    @Consumes({"application/x-yaml",
            // per addChildren
            "text/yaml", "text/x-yaml", "application/yaml", MediaType.APPLICATION_JSON})
    @Path("/{entity}/workflows")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public TaskSummary runWorkflow(
            @PathParam("application") final String application,
            @PathParam("entity") final String entity,

            @ApiParam(name = "timeout", value = "Delay before server should respond with incomplete activity task, rather than completed task: " +
                    "'never' means block until complete; " +
                    "'0' means return task immediately; " +
                    "and e.g. '20ms' (the default) will wait 20ms for completed task information to be available",
                    required = false, defaultValue = "20ms")
            @QueryParam("timeout") final String timeout,

            @ApiParam(
                    name = "workflowSpec",
                    value = "Workflow spec in YAML (including 'steps' root element with a list of steps)",
                    required = true)
                    String yaml);

    @POST
    @Path("/{entity}/workflows/{workflowId}/replay/from/{step}")
    @ApiOperation(value = "Replays a workflow from the given step, or 'start' to restart, 'last' to use the last replay point, or 'end' to replay resuming; returns the task of the replay")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public TaskSummary replayWorkflow(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entity,
            @ApiParam(value = "Workflow ID", required = true)
            @PathParam("workflowId") String workflowId,
            @ApiParam(value = "step", required = true)
            @PathParam("step") String step,
            @ApiParam(value = "reason", required = false)
            @QueryParam("reason") String reason,
            @ApiParam(value = "force", required = false)
            @QueryParam("force") Boolean force);

}
