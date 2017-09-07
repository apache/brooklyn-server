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

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.brooklyn.rest.domain.TypeSummary;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Path("/subtypes")
@Api("Subtypes")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface SubtypeApi {

    @Path("/{supertype}")
    @GET
    @ApiOperation(value = "Get all known types which declare the given argument as a supertype", 
            response = TypeSummary.class, responseContainer = "List")
    public List<TypeSummary> list(
        @ApiParam(name = "supertype", value = "Supertype to query", required = true)
        @PathParam("supertype")
        String supertype,
        @ApiParam(name = "versions", value = "Whether to list 'latest' of each symbolic-name or 'all' versions", required = false, defaultValue = "latest")
        @QueryParam("versions")
        String versions,
        @ApiParam(name = "regex", value = "Regular expression to search for")
        @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
        @QueryParam("fragment") @DefaultValue("") String fragment);

    // conveniences for common items where internally it uses java class name
    // caller can of course use /subtypes/org.apache.brooklyn.api.Entity
    
    @GET @Path("/application")
    @ApiOperation(value = "Get all applications", response = TypeSummary.class, responseContainer = "List")
    public List<TypeSummary> listApplications(@ApiParam(name = "versions", value = "Whether to list 'latest' of each symbolic-name or 'all' versions", required = false, defaultValue = "latest") @QueryParam("versions") String versions,
        @ApiParam(name = "regex", value = "Regular expression to search for") @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for") @QueryParam("fragment") @DefaultValue("") String fragment);
    
    @GET @Path("/entity")
    @ApiOperation(value = "Get all entities", response = TypeSummary.class, responseContainer = "List")
    public List<TypeSummary> listEntities(@ApiParam(name = "versions", value = "Whether to list 'latest' of each symbolic-name or 'all' versions", required = false, defaultValue = "latest") @QueryParam("versions") String versions,
        @ApiParam(name = "regex", value = "Regular expression to search for") @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for") @QueryParam("fragment") @DefaultValue("") String fragment);
    
    @GET @Path("/policy")
    @ApiOperation(value = "Get all policies", response = TypeSummary.class, responseContainer = "List")
    public List<TypeSummary> listPolicies(@ApiParam(name = "versions", value = "Whether to list 'latest' of each symbolic-name or 'all' versions", required = false, defaultValue = "latest") @QueryParam("versions") String versions,
        @ApiParam(name = "regex", value = "Regular expression to search for") @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for") @QueryParam("fragment") @DefaultValue("") String fragment);
    
    @GET @Path("/enricher")
    @ApiOperation(value = "Get all enrichers", response = TypeSummary.class, responseContainer = "List")
    public List<TypeSummary> listEnrichers(@ApiParam(name = "versions", value = "Whether to list 'latest' of each symbolic-name or 'all' versions", required = false, defaultValue = "latest") @QueryParam("versions") String versions,
        @ApiParam(name = "regex", value = "Regular expression to search for") @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for") @QueryParam("fragment") @DefaultValue("") String fragment);

    @GET @Path("/location")
    // note some (deprecated) locations are only available on location manager 
    @ApiOperation(value = "Get all locations stored in the registry", response = TypeSummary.class, responseContainer = "List")
    public List<TypeSummary> listLocations(@ApiParam(name = "versions", value = "Whether to list 'latest' of each symbolic-name or 'all' versions", required = false, defaultValue = "latest") @QueryParam("versions") String versions,
        @ApiParam(name = "regex", value = "Regular expression to search for") @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for") @QueryParam("fragment") @DefaultValue("") String fragment);

    // in future could have others, eg: tasks, feeds, etc
    
}
