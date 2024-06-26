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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.brooklyn.rest.domain.ConfigSummary;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/applications/{application}/entities/{entity}/config")
@Api("Entity Config")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface EntityConfigApi {

    @GET
    @ApiOperation(value = "Fetch the config keys for a specific application entity",
            response = org.apache.brooklyn.rest.domain.ConfigSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find application, entity or config key"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<ConfigSummary> list(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken);

    // TODO support parameters  ?show=value,summary&name=xxx &format={string,json,xml}
    // (and in sensors class)
    @GET
    @Path("/current-state")
    @ApiOperation(value = "Fetch config key values in batch", notes="Returns a map of config name to value")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find application, entity or config key"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Map<String, Object> batchConfigRead(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,

            @ApiParam(value = "Whether to format/annotate values with hints for for display", required = false)
            @QueryParam("useDisplayHint") @DefaultValue("true") final Boolean useDisplayHints,
            @ApiParam(value = "Whether to skip resolution of all values", required = false)
            @QueryParam("skipResolution") @DefaultValue("false") final Boolean skipResolution,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") @DefaultValue("false") final Boolean suppressSecrets,

            @ApiParam(value = "Return raw config data instead of display values (deprecated, see useDisplayHints)", required = false)
            @Deprecated
            @QueryParam("raw") @DefaultValue("false") final Boolean raw);

    //To call this endpoint set the Accept request field e.g curl -H "Accept: application/json" ...
    @GET
    @Path("/{config}")
    @ApiOperation(value = "Fetch config value (json)", response = Object.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK. The sensor value is returned."),
            @ApiResponse(code = 204, message = "No Content. The sensor is known, but unset."),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application, entity, or config not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Produces(MediaType.APPLICATION_JSON)
    public Object get(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,
            @ApiParam(value = "Config key ID", required = true)
            @PathParam("config") String configKeyName,

            @ApiParam(value = "Whether to format/annotate values with hints for for display", required = false)
            @QueryParam("useDisplayHint") @DefaultValue("true") final Boolean useDisplayHints,
            @ApiParam(value = "Whether to skip resolution of all values", required = false)
            @QueryParam("skipResolution") @DefaultValue("false") final Boolean skipResolution,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") @DefaultValue("false") final Boolean suppressSecrets,

            @ApiParam(value = "Return raw config data instead of display values (deprecated, see useDisplayHints)", required = false)
            @Deprecated
            @QueryParam("raw") @DefaultValue("false") final Boolean raw);

    // if user requests plain value we skip some json post-processing
    @GET
    @Path("/{config}")
    @ApiOperation(value = "Fetch config value (text/plain)", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 204, message = "No Content. The config is known, but unset."),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find application, entity or config key"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Produces(MediaType.TEXT_PLAIN + ";qs=0.9")
    public String getPlain(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,
            @ApiParam(value = "Config key ID", required = true)
            @PathParam("config") String configKeyName,

            @ApiParam(value = "Whether to format/annotate values with hints for for display", required = false)
            @QueryParam("useDisplayHints") @DefaultValue("true") final Boolean useDisplayHints,
            @ApiParam(value = "Whether to skip resolution of all values", required = false)
            @QueryParam("skipResolution") @DefaultValue("false") final Boolean skipResolution,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") @DefaultValue("false") final Boolean suppressSecrets,

            @ApiParam(value = "Return raw config data instead of display values (deprecated, see useDisplayHints)", required = false)
            @Deprecated
            @QueryParam("raw") @DefaultValue("false") final Boolean raw);

    @POST
    @ApiOperation(value = "Manually set multiple config values")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "No Content"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find application or entity"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @SuppressWarnings("rawtypes")
    public void setFromMap(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Apply the config to all pre-existing descendants", required = false)
            @QueryParam("recurse") @DefaultValue("false") final Boolean recurse,
            @ApiParam(value = "Map of config key names to values", required = true)
            Map newValues);

    @POST
    @Path("/{config}")
    @ApiOperation(value = "Manually set a config value")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "No Content"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find application, entity or config key"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void set(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Config key name", required = true)
            @PathParam("config") String configName,
            @ApiParam(value = "Apply the config to all pre-existing descendants", required = false)
            @QueryParam("recurse") @DefaultValue("false") final Boolean recurse,
            @ApiParam(value = "Value to set")
            Object newValue);

    // deletion of config is not supported; you can set it null
}
