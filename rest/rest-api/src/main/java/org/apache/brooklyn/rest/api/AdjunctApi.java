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

import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.rest.domain.AdjunctDetail;
import org.apache.brooklyn.rest.domain.AdjunctSummary;
import org.apache.brooklyn.rest.domain.ConfigSummary;
import org.apache.brooklyn.rest.domain.Status;
import org.apache.brooklyn.rest.domain.TaskSummary;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/applications/{application}/entities/{entity}/adjuncts")
@Api("Entity Adjuncts (policies, enrichers, feeds)")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface AdjunctApi {

    @GET
    @ApiOperation(value = "Fetch the adjuncts attached to a specific application entity",
            response = org.apache.brooklyn.rest.domain.AdjunctSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application or entity"),
            @ApiResponse(code = 400, message = "Type is not known adjunct kind")
    })
    public List<AdjunctSummary> list(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Filter by adjunct type (policy, enricher, feed)", required = false)
            @QueryParam("adjunctType") final String adjunctType);

    // TODO support YAML ?
    @POST
    @ApiOperation(value = "Create and add an adjunct (e.g. a policy, enricher, or feed) to this entity", notes = "Returns a summary of the added adjunct")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application or entity"),
            @ApiResponse(code = 400, message = "Type is not a suitable adjunct")
    })
    public AdjunctDetail addAdjunct(
            @ApiParam(name = "application", value = "Application ID or name", required = true)
            @PathParam("application") String application,

            @ApiParam(name = "entity", value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,

            @ApiParam(name = "type", value = "Adjunct from the type registry to instantiate and add", required = true)
            @QueryParam("type")
            String adjunctRegisteredTypeName,

            // TODO would like to make this optional but jersey complains if we do
            @ApiParam(name = "config", value = "Configuration for the adjunct (as key value pairs)", required = true)
            Map<String, String> config);

    @GET
    @Path("/{adjunct}")
    @ApiOperation(value = "Gets detail of an adjunct")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity or adjunct")
    })
    public AdjunctDetail get(
            @ApiParam(name = "application", value = "Application ID or name", required = true)
            @PathParam("application") String application,

            @ApiParam(name = "entity", value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,

            @ApiParam(name = "adjunct", value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") String adjunctId);
    
    @GET
    @Path("/{adjunct}/status")
    @ApiOperation(value = "Gets status of an adjunct (RUNNING / SUSPENDED)")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity or adjunct")
    })
    public Status getStatus(
            @ApiParam(name = "application", value = "Application ID or name", required = true)
            @PathParam("application") String application,

            @ApiParam(name = "entity", value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,

            @ApiParam(name = "adjunct", value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") String adjunctId);

    @POST
    @Path("/{adjunct}/start")
    @ApiOperation(value = "Start or resume an adjunct")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity or adjunct"),
            @ApiResponse(code = 400, message = "Adjunct does not support start/stop")
    })
    public Response start(
            @ApiParam(name = "application", value = "Application ID or name", required = true)
            @PathParam("application") String application,

            @ApiParam(name = "entity", value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,

            @ApiParam(name = "adjunct", value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") String adjunctId);

    @POST
    @Path("/{adjunct}/stop")
    @ApiOperation(value = "Suspends an adjunct")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity or adjunct"),
            @ApiResponse(code = 400, message = "Adjunct does not support start/stop")
    })
    public Response stop(
            @ApiParam(name = "application", value = "Application ID or name", required = true)
            @PathParam("application") String application,

            @ApiParam(name = "entity", value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,

            @ApiParam(name = "adjunct", value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") String adjunctId);

    @DELETE
    @Path("/{adjunct}")
    @ApiOperation(value = "Destroy an adjunct", notes="Removes an adjunct from being associated with the entity and destroys it (stopping first if running)")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity or adjunct")
    })
    public Response destroy(
            @ApiParam(name = "application", value = "Application ID or name", required = true)
            @PathParam("application") String application,

            @ApiParam(name = "entity", value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,

            @ApiParam(name = "adjunct", value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") String adjunctId);

    /// ---------- config ---------------
    
    @GET
    @Path("/{adjunct}/config")
    @ApiOperation(value = "Fetch the config keys for a specific adjunct",
            response = org.apache.brooklyn.rest.domain.ConfigSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application or entity or adjunct")
    })
    public List<ConfigSummary> listConfig(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") final String adjunctToken);

    // TODO support parameters  ?show=value,summary&name=xxx &format={string,json,xml}
    // (and in sensors class)
    @GET
    @Path("/{adjunct}/config-current")
    @ApiOperation(value = "Fetch config key values in batch", notes="Returns a map of config name to value")
    public Map<String, Object> batchConfigRead(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,
            @ApiParam(value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") String adjunctToken) ;

    @GET
    @Path("/{adjunct}/config/{config}")
    @ApiOperation(value = "Fetch config value", response = Object.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity, adjunct or config key")
    })
    public String getConfig(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,
            @ApiParam(value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") String adjunctToken,
            @ApiParam(value = "Config key ID", required = true)
            @PathParam("config") String configKeyName);

    @POST
    @Path("/{adjunct}/config/{config}")
    @Consumes(value = {"*/*"})
    @ApiOperation(value = "Sets the given config on this adjunct")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity, adjunct or config key")
    })
    public Response setConfig(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,
            @ApiParam(value = "Adjunct ID or name", required = true)
            @PathParam("adjunct") String adjunctToken,
            @ApiParam(value = "Config key ID", required = true)
            @PathParam("config") String configKeyName,
            @ApiParam(name = "value", value = "New value for the configuration", required = true)
            Object value);
    

    @GET
    @Path("/{adjunct}/activities")
    @ApiOperation(value = "Fetch list of tasks for this adjunct")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity, or adjunct")
    })
    public List<TaskSummary> listTasks(
            @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
            @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId,
            @ApiParam(value = "Adjunct ID or name", required = true) @PathParam("adjunct") String adjunctToken,
            @ApiParam(value = "Max number of tasks, or -1 for all (default 200)", required = false) 
            @QueryParam("limit") @DefaultValue("200") int limit,
            @ApiParam(value = "Whether to include subtasks recursively across different entities (default false)", required = false)
            @QueryParam("recurse") @DefaultValue("false") Boolean recurse);

    @GET
    @ApiOperation(value = "Returns an icon for the adjunct, if defined")
    @Path("/{adjunct}/icon")
    public Response getIcon(
        @ApiParam(value = "Application ID or name", required = true) @PathParam("application") String applicationId,
        @ApiParam(value = "Entity ID or name", required = true) @PathParam("entity") String entityId,
        @ApiParam(value = "Adjunct ID or name", required = true) @PathParam("adjunct") String adjunctToken);

}
