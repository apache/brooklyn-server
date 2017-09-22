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
import javax.ws.rs.core.Response;

import org.apache.brooklyn.rest.domain.TypeDetail;
import org.apache.brooklyn.rest.domain.TypeSummary;

import com.google.common.annotations.Beta;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Path("/catalog/types")
@Api("Catalog Types")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Beta
public interface TypeApi {

    @GET
    @ApiOperation(value = "List types registered in the system", 
            response = TypeSummary.class,
            responseContainer = "List")
    public List<TypeSummary> list(
        @ApiParam(name = "supertype", value = "Supertype to require (beta, currently intended only for 'entity', 'policy', 'enricher', and 'location')", required = false)
        @QueryParam("supertype")
        String supertype,
        @ApiParam(name = "versions", value = "Whether to list 'latest' of each symbolic-name or 'all' versions", 
        required = false, defaultValue = "latest")
        @QueryParam("versions")
        String versions,
        @ApiParam(name = "regex", value = "Regular expression to search for (in name and description)")
        @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for (in name and description)")
        @QueryParam("fragment") @DefaultValue("") String fragment);

    @Path("/{nameOrAlias}")
    @GET
    @ApiOperation(value = "Get summaries for all versions and instances of a given type or alias, with best match first", 
            response = TypeSummary.class,
            responseContainer = "List")
    public List<TypeSummary> listVersions(
        @ApiParam(name = "nameOrAlias", value = "Type name to query", required = true)
        @PathParam("nameOrAlias")
        String nameOrAlias);

    @Path("/{symbolicName}/{version}")
    @GET
    @ApiOperation(value = "Get detail on a given type and version, allowing 'latest' to match the most recent version (preferring non-SNAPSHOTs)", 
            response = TypeDetail.class)
    public TypeDetail detail(
        @ApiParam(name = "symbolicName", value = "Type name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "version", value = "Version to query", required = true)
        @PathParam("version")
        String version);
    
    @Path("/{symbolicName}/{version}/icon")
    @GET
    @ApiOperation(value = "Returns the icon image registered for this item")
    @Produces("application/image")
    public Response icon(
        @ApiParam(name = "symbolicName", value = "Type name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "version", value = "Version to query", required = true)
        @PathParam("version")
        String version);
    
}
