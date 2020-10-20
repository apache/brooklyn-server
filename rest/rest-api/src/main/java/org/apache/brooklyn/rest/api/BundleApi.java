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

import javax.validation.Valid;
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

import org.apache.brooklyn.rest.domain.BundleInstallationRestResult;
import org.apache.brooklyn.rest.domain.BundleSummary;
import org.apache.brooklyn.rest.domain.TypeDetail;
import org.apache.brooklyn.rest.domain.TypeSummary;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/catalog/bundles")
@Api("Catalog Bundles")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface BundleApi {

    @GET
    @ApiOperation(value = "List bundles registered in the system including their types", 
            response = BundleSummary.class,
            responseContainer = "List")
    public List<BundleSummary> list(
        @ApiParam(name = "versions", value = "Whether to list 'latest' for each symbolic-name or 'all' versions", 
        required = false, defaultValue = "latest")
        @QueryParam("versions")
        String versions,
        @ApiParam(name = "detail", value = "Whether to include types and other detail info, default 'false'", 
        required = false, defaultValue = "false")
        @QueryParam("detail")
        boolean detail);

    @Path("/{symbolicName}")
    @GET
    @ApiOperation(value = "Get summaries for all versions of the given bundle, with more recent ones first (preferring non-SNAPSHOTs)", 
            response = BundleSummary.class,
            responseContainer = "List")
    public List<BundleSummary> listVersions(
        @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "detail", value = "Whether to include types and other detail info, default 'false'", 
        required = false, defaultValue = "false")
        @QueryParam("detail")
        boolean detail);

    @Path("/{symbolicName}/{version}")
    @GET
    @ApiOperation(value = "Get detail on a specific bundle given its symbolic name and version", 
            response = BundleSummary.class)
    public BundleSummary detail(
        @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "version", value = "Version to query", required = true)
        @PathParam("version")
        String version);


    @Path("/{symbolicName}/{version}/types")
    @GET
    @ApiOperation(value = "Get all types declared in a given bundle", 
            response = TypeDetail.class)
    public List<TypeSummary> getTypes(
        @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "version", value = "Version of bundle and of type to query", required = true)
        @PathParam("version")
        String version);
    
    @Path("/{symbolicName}/{version}/types/{typeSymbolicName}")
    @GET
    @ApiOperation(value = "Get detail on a given type in a given bundle", 
            response = TypeDetail.class)
    public TypeDetail getType(
        @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "version", value = "Version of bundle and of type to query", required = true)
        @PathParam("version")
        String version,
        @ApiParam(name = "typeSymbolicName", value = "Type name to query", required = true)
        @PathParam("typeSymbolicName")
        String typeSymbolicName);
    
    @Path("/{symbolicName}/{version}/types/{typeSymbolicName}/{typeVersion}")
    @GET
    @ApiOperation(value = "Get detail on a given type and version in a bundle (special method for unusual cases where type has different version)", 
            response = TypeDetail.class)
    public TypeDetail getTypeExplicitVersion(
        @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "version", value = "Bundle version to query", required = true)
        @PathParam("version")
        String version,
        @ApiParam(name = "typeSymbolicName", value = "Type name to query", required = true)
        @PathParam("typeSymbolicName")
        String typeSymbolicName,
        @ApiParam(name = "typeVersion", value = "Version to query (if different to bundle version)", required = true)
        @PathParam("typeVersion")
        String typeVersion);
    
    @Path("/{symbolicName}/{version}")
    @DELETE
    @ApiOperation(value = "Removes a bundle, unregistering all the types it declares", 
            response = BundleInstallationRestResult.class)
    public BundleInstallationRestResult remove(
        @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "version", value = "Version to query", required = true)
        @PathParam("version")
        String version,
        @ApiParam(name = "force", value = "Whether to forcibly remove it, even if in use and/or errors", required = false, defaultValue = "false")
        @QueryParam("force") @DefaultValue("false")
        Boolean force);

    /** @deprecated since 1.1 use {@link #create(byte[], String, Boolean)} instead */
    @Deprecated
    @POST
    @Consumes("application/deprecated-yaml")
    @ApiOperation(value = "(deprecated)", hidden = true)
    public Response createFromYaml(
            @ApiParam(name = "yaml", value = "BOM YAML declaring the types to be installed", required = true)
            @Valid String yaml,
            @ApiParam(name="force", value="Force installation including replacing any different bundle of the same name and version")
            @QueryParam("force") @DefaultValue("false")
            Boolean forceUpdate);

    /** @deprecated since 1.1 use {@link #create(byte[], String, Boolean)} instead */
    @Deprecated
    @POST
    @Consumes({"application/deprecated-zip"})
    @ApiOperation(value = "(deprecated)", hidden = true)
    public Response createFromArchive(
            @ApiParam(
                    name = "archive",
                    value = "Bundle to install, in ZIP or JAR format, requiring catalog.bom containing bundle name and version",
                    required = true)
            byte[] archive,
            @ApiParam(name = "force", value = "Whether to forcibly remove it, even if in use and/or errors", required = false, defaultValue = "false")
            @QueryParam("force") @DefaultValue("false")
            Boolean force);

    /** @deprecated since 1.1 use {@link #create(byte[], String, Boolean)} instead */
    @Deprecated
    @POST
    @Consumes // anything - now autodetect is done for everything unless 'format' is specified
    // (mime type is ignored; though it could be useful in the "score" function, and probably is available on the thread)
    @ApiOperation(
            value = "Add a bundle of types (entities, etc) to the type registry",
            notes = "This will auto-detect the format, with the 'brooklyn-bom-bundle' being common and consisting of "
                    + "a ZIP/JAR containing a catalog.bom",
            response = BundleInstallationRestResult.class)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Error processing the given archive, or the catalog.bom is invalid"),
            @ApiResponse(code = 201, message = "Catalog items added successfully")
    })
    public Response create(
            @ApiParam(
                    name = "archive",
                    value = "Bundle contents to install, eg for brooklyn-catalog-bundle a ZIP or JAR containing a catalog.bom file",
                    required = true)
                    byte[] archive,
            @ApiParam(name = "format", value="Specify the format to indicate a specific resolver for handling this", required=false)
            @QueryParam("format") @DefaultValue("")
                    String format,
            @ApiParam(name = "force", value = "Whether to forcibly remove it, even if in use and/or errors", required = false, defaultValue = "false")
            @QueryParam("force") @DefaultValue("false")
                    Boolean force);

}
