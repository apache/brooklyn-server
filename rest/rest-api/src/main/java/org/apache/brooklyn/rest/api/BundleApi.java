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
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
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
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Symbolic name not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
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
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Symbolic name not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public BundleSummary detail(
        @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
        @PathParam("symbolicName")
        String symbolicName,
        @ApiParam(name = "version", value = "Version to query", required = true)
        @PathParam("version")
        String version);

    @Path("/{symbolicName}/{version}/download")
    @GET
    @ApiOperation(value = "Download a ZIP archive of a specific bundle given its symbolic name and version")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Cannot find Zip archive or bundle"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response download(
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
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Symbolic name not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
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
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Symbolic name or type not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
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
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Symbolic name, type or version not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
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

    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Symbolic name, type or version not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/{symbolicName}/{version}/types/{typeSymbolicName}/{typeVersion}/icon")
    @GET
    @ApiOperation(value = "Returns the icon image registered for this type")
    @Produces("application/image")
    public Response getTypeExplicitVersionIcon(
            @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
            @PathParam("symbolicName")
                    String symbolicName,
            @ApiParam(name = "version", value = "Bundle version to query", required = true)
            @PathParam("version")
                    String version,
            @ApiParam(name = "typeSymbolicName", value = "Type name to query", required = true)
            @PathParam("typeSymbolicName")
                    String typeSymbolicName,
            @ApiParam(name = "typeVersion", value = "Version to query (if different to bundle version, or * or empty)", required = true)
            @PathParam("typeVersion")
                    String typeVersion);

    @Path("/{symbolicName}/{version}")
    @DELETE
    @ApiOperation(value = "Removes a bundle, unregistering all the types it declares", 
            response = BundleInstallationRestResult.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Symbolic name, type, or version not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
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
    @ApiOperation(value = "(deprecated; use same endpoint accepting optional format)", hidden = true, response = BundleInstallationRestResult.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK. The bundle is already installed."),
            @ApiResponse(code = 201, message = "Created. The bundle has been installed."),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application, entity, or sensor not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
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
    @ApiOperation(value = "(deprecated; use same endpoint accepting optional format)", hidden = true, response = BundleInstallationRestResult.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK. The bundle is already installed."),
            @ApiResponse(code = 201, message = "Created. The bundle has been installed."),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application, entity, or sensor not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response createFromArchive(
            @ApiParam(
                    name = "archive",
                    value = "Bundle to install, in ZIP or JAR format, requiring catalog.bom containing bundle name and version",
                    required = true)
            byte[] archive,
            @ApiParam(name = "force", value = "Whether to forcibly remove it, even if in use and/or errors", required = false, defaultValue = "false")
            @QueryParam("force") @DefaultValue("false")
            Boolean force);

    @POST
    @Consumes // anything - now autodetect is done for everything unless 'format' is specified
    // (mime type is ignored; though it could be useful in the "score" function, and probably is available on the thread)
    @ApiOperation(
            value = "Add a bundle of types (entities, etc) to the type registry",
            notes = "Format can be omitted for auto-detection, or supplied explicitly eg 'brooklyn-bom-bundle' to upload "
                    + "a ZIP/JAR containing a catalog.bom and optional other items",
            response = BundleInstallationRestResult.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK. The bundle is already installed."),
            @ApiResponse(code = 201, message = "Created. The bundle has been installed."),
            @ApiResponse(code = 400, message = "Error processing the given archive, or the catalog.bom is invalid"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response create(
            @ApiParam(
                    name = "archive",
                    value = "Bundle contents to install, eg for brooklyn-catalog-bundle a ZIP or JAR containing a catalog.bom file",
                    required = true)
                    byte[] archive,
            @ApiParam(name = "format", value="Specify the format to indicate a specific resolver for handling this (auto-detect if omitted)", required=false)
            @QueryParam("format") @DefaultValue("")
                    String format,
            @ApiParam(name = "force", value = "Whether to forcibly remove it, even if in use and/or errors", required = false, defaultValue = "false")
            @QueryParam("force") @DefaultValue("false")
                    Boolean force);

    @Path("/{symbolicName}/{version}/icon")
    @GET
    @ApiOperation(value = "Gets the icon for a specific bundle given its symbolic name and version, if defined",
            response = BundleSummary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Symbolic name not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response getIcon(
            @ApiParam(name = "symbolicName", value = "Bundle name to query", required = true)
            @PathParam("symbolicName")
            String symbolicName,
            @ApiParam(name = "version", value = "Version to query", required = true)
            @PathParam("version")
            String version);

}
