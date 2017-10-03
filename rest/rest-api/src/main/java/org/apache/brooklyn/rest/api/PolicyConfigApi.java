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
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.rest.domain.PolicyConfigSummary;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/applications/{application}/entities/{entity}/policies/{policy}/config")
@Api("Entity Policy Config")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
/** @deprecated since 0.12.0 use AdjunctApi */
@Deprecated
public interface PolicyConfigApi {

    @GET
    @ApiOperation(value = "Fetch the config keys for a specific policy (deprecated, use adjuncts/ endpoint instead)",
            response = org.apache.brooklyn.rest.domain.ConfigSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application or entity or policy")
    })
    public List<PolicyConfigSummary> list(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Policy ID or name", required = true)
            @PathParam("policy") final String policyToken);

    // TODO support parameters  ?show=value,summary&name=xxx &format={string,json,xml}
    // (and in sensors class)
    @GET
    @Path("/current-state")
    @ApiOperation(value = "Fetch config key values in batch (deprecated, use adjuncts/ endpoint instead)", notes="Returns a map of config name to value")
    public Map<String, Object> batchConfigRead(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,
            @ApiParam(value = "Policy ID or name", required = true)
            @PathParam("policy") String policyToken) ;

    @GET
    @Path("/{config}")
    @ApiOperation(value = "Fetch config value (deprecated, use adjuncts/ endpoint instead)", response = Object.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity, policy or config key")
    })
    public String get(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,
            @ApiParam(value = "Policy ID or name", required = true)
            @PathParam("policy") String policyToken,
            @ApiParam(value = "Config key ID", required = true)
            @PathParam("config") String configKeyName);

    @POST
    @Path("/{config}")
    @Consumes(value = {"*/*"})
    @ApiOperation(value = "Sets the given config on this policy (deprecated, use adjuncts/ endpoint instead)")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Could not find application, entity, policy or config key")
    })
    public Response set(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") String entityToken,
            @ApiParam(value = "Policy ID or name", required = true)
            @PathParam("policy") String policyToken,
            @ApiParam(value = "Config key ID", required = true)
            @PathParam("config") String configKeyName,
            @ApiParam(name = "value", value = "New value for the configuration", required = true)
            Object value);
}
