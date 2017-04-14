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

import org.apache.brooklyn.rest.domain.CatalogEntitySummary;
import org.apache.brooklyn.rest.domain.CatalogItemSummary;
import org.apache.brooklyn.rest.domain.CatalogLocationSummary;
import org.apache.brooklyn.rest.domain.CatalogPolicySummary;

import com.google.common.annotations.Beta;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/catalog")
@Api("Catalog")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface CatalogApi {

    /** @deprecated since 0.11.0 use {@link #createFromYaml(String)} instead */
    @Deprecated
    @POST
    @ApiOperation(
            value = "Add a catalog items (e.g. new type of entity, policy or location) by uploading YAML descriptor.",
            notes = "Return value is map of ID to CatalogItemSummary.",
            response = String.class,
            hidden = true
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Error processing the given YAML"),
            @ApiResponse(code = 201, message = "Catalog items added successfully")
    })
    public Response create(String yaml);

    @POST
    @Consumes({MediaType.APPLICATION_JSON, "application/x-yaml",
        // see http://stackoverflow.com/questions/332129/yaml-mime-type
        "text/yaml", "text/x-yaml", "application/yaml"})
    @ApiOperation(
            value = "Add a catalog items (e.g. new type of entity, policy or location) by uploading YAML descriptor.",
            notes = "Return value is map of ID to CatalogItemSummary.",
            response = String.class,
            hidden = true
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Error processing the given YAML"),
            @ApiResponse(code = 201, message = "Catalog items added successfully")
    })
    public Response createFromYaml(
            @ApiParam(name = "yaml", value = "YAML descriptor of catalog item", required = true)
            @Valid String yaml);

    @Beta
    @POST
    @Consumes({"application/x-zip", "application/x-jar"})
    @ApiOperation(
            value = "Add a catalog items (e.g. new type of entity, policy or location) by uploading a ZIP/JAR archive.",
            notes = "Accepts either an OSGi bundle JAR, or ZIP which will be turned into bundle JAR. Bother format must "
                    + "contain a catalog.bom at the root of the archive, which must contain the bundle and version key."
                    + "Return value is map of ID to CatalogItemSummary.",
            response = String.class,
            hidden = true)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Error processing the given archive, or the catalog.bom is invalid"),
            @ApiResponse(code = 201, message = "Catalog items added successfully")
    })
    public Response createFromArchive(
            @ApiParam(
                    name = "archive",
                    value = "Bundle to install, in ZIP or JAR format, requiring catalog.bom containing bundle name and version",
                    required = true)
            byte[] archive);

    @Beta
    @POST
    @Consumes // anything (if doesn't match other methods with specific content types
    @ApiOperation(
            value = "Add a catalog items (e.g. new type of entity, policy or location) by uploading either YAML or ZIP/JAR archive (format autodetected)",
            notes = "Specify a content-type header to skip auto-detection and invoke one of the more specific methods. "
                    + "Accepts either an OSGi bundle JAR, or ZIP which will be turned into bundle JAR. Bother format must "
                    + "contain a catalog.bom at the root of the archive, which must contain the bundle and version key."
                    + "Return value is map of ID to CatalogItemSummary.",
            response = String.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "Error processing the given archive, or the catalog.bom is invalid"),
            @ApiResponse(code = 201, message = "Catalog items added successfully")
    })
    public Response createFromUpload(
            @ApiParam(
                    name = "item",
                    value = "Item to install, as JAR/ZIP or Catalog YAML (autodetected)",
                    required = true)
                    byte[] item);
    
    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Path("/reset")
    @ApiOperation(value = "Resets the catalog to the given (XML) format")
    public Response resetXml(
            @ApiParam(name = "xml", value = "XML descriptor of the entire catalog to install", required = true)
            @Valid String xml,
            @ApiParam(name ="ignoreErrors", value ="Don't fail on invalid bundles, log the errors only")
            @QueryParam("ignoreErrors")  @DefaultValue("false")
            boolean ignoreErrors);

    @DELETE
    @Path("/applications/{symbolicName}/{version}")
    @ApiOperation(value = "Deletes a specific version of an application's definition from the catalog")
    @ApiResponses(value = {
        @ApiResponse(code = 404, message = "Entity not found")
    })
    public void deleteApplication(
        @ApiParam(name = "symbolicName", value = "The symbolic name of the application or template to delete", required = true)
        @PathParam("symbolicName") String symbolicName,

        @ApiParam(name = "version", value = "The version identifier of the application or template to delete", required = true)
        @PathParam("version") String version) throws Exception;

    @DELETE
    @Path("/entities/{symbolicName}/{version}")
    @ApiOperation(value = "Deletes a specific version of an entity's definition from the catalog")
    @ApiResponses(value = {
        @ApiResponse(code = 404, message = "Entity not found")
    })
    public void deleteEntity(
        @ApiParam(name = "symbolicName", value = "The symbolic name of the entity or template to delete", required = true)
        @PathParam("symbolicName") String symbolicName,

        @ApiParam(name = "version", value = "The version identifier of the entity or template to delete", required = true)
        @PathParam("version") String version) throws Exception;

    @DELETE
    @Path("/policies/{policyId}/{version}")
    @ApiOperation(value = "Deletes a specific version of an policy's definition from the catalog")
    @ApiResponses(value = {
        @ApiResponse(code = 404, message = "Policy not found")
    })
    public void deletePolicy(
        @ApiParam(name = "policyId", value = "The ID of the policy to delete", required = true)
        @PathParam("policyId") String policyId,

        @ApiParam(name = "version", value = "The version identifier of the policy to delete", required = true)
        @PathParam("version") String version) throws Exception;

    @DELETE
    @Path("/locations/{locationId}/{version}")
    @ApiOperation(value = "Deletes a specific version of an location's definition from the catalog")
    @ApiResponses(value = {
        @ApiResponse(code = 404, message = "Location not found")
    })
    public void deleteLocation(
        @ApiParam(name = "locationId", value = "The ID of the location to delete", required = true)
        @PathParam("locationId") String locationId,

        @ApiParam(name = "version", value = "The version identifier of the location to delete", required = true)
        @PathParam("version") String version) throws Exception;

    @GET
    @Path("/entities")
    @ApiOperation(value = "List available entity types optionally matching a query", 
            response = CatalogItemSummary.class,
            responseContainer = "List")
    public List<CatalogEntitySummary> listEntities(
        @ApiParam(name = "regex", value = "Regular expression to search for")
        @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
        @QueryParam("fragment") @DefaultValue("") String fragment,
        @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
        @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    @GET
    @Path("/applications")
    @ApiOperation(value = "Fetch a list of application templates optionally matching a query", 
            response = CatalogItemSummary.class,
            responseContainer = "List")
    public List<CatalogItemSummary> listApplications(
            @ApiParam(name = "regex", value = "Regular expression to search for")
            @QueryParam("regex") @DefaultValue("") String regex,
            @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
            @QueryParam("fragment") @DefaultValue("") String fragment,
            @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
            @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    @GET
    @Path("/entities/{symbolicName}/{version}")
    @ApiOperation(value = "Fetch a specific version of an entity's definition from the catalog", 
            response = CatalogEntitySummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 404, message = "Entity not found")
    })
    public CatalogEntitySummary getEntity(
        @ApiParam(name = "symbolicName", value = "The symbolic name of the entity or template to retrieve", required = true)
        @PathParam("symbolicName") String symbolicName,

        @ApiParam(name = "version", value = "The version identifier of the entity or template to retrieve", required = true)
        @PathParam("version") String version) throws Exception;

    @GET
    @Path("/applications/{symbolicName}/{version}")
    @ApiOperation(value = "Fetch a specific version of an application's definition from the catalog", 
            response = CatalogEntitySummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 404, message = "Entity not found")
    })
    public CatalogEntitySummary getApplication(
        @ApiParam(name = "symbolicName", value = "The symbolic name of the application to retrieve", required = true)
        @PathParam("symbolicName") String symbolicName,

        @ApiParam(name = "version", value = "The version identifier of the application to retrieve", required = true)
        @PathParam("version") String version) throws Exception;

    @GET
    @Path("/policies")
    @ApiOperation(value = "List available policies optionally matching a query", 
            response = CatalogPolicySummary.class,
            responseContainer = "List")
    public List<CatalogPolicySummary> listPolicies(
            @ApiParam(name = "regex", value = "Regular expression to search for")
            @QueryParam("regex") @DefaultValue("") String regex,
            @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
            @QueryParam("fragment") @DefaultValue("") String fragment,
            @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
            @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    @GET
    @Path("/policies/{policyId}/{version}")
    @ApiOperation(value = "Fetch a policy's definition from the catalog", 
            response = CatalogItemSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 404, message = "Entity not found")
    })
    public CatalogPolicySummary getPolicy(
        @ApiParam(name = "policyId", value = "The ID of the policy to retrieve", required = true)
        @PathParam("policyId") String policyId,
        @ApiParam(name = "version", value = "The version identifier of the application to retrieve", required = true)
        @PathParam("version") String version) throws Exception;

    @GET
    @Path("/locations")
    @ApiOperation(value = "List available locations optionally matching a query", 
            response = CatalogLocationSummary.class,
            responseContainer = "List")
    public List<CatalogLocationSummary> listLocations(
            @ApiParam(name = "regex", value = "Regular expression to search for")
            @QueryParam("regex") @DefaultValue("") String regex,
            @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
            @QueryParam("fragment") @DefaultValue("") String fragment,
            @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
            @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    @GET
    @Path("/locations/{locationId}/{version}")
    @ApiOperation(value = "Fetch a location's definition from the catalog", 
            response = CatalogItemSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 404, message = "Entity not found")
    })
    public CatalogItemSummary getLocation(
        @ApiParam(name = "locationId", value = "The ID of the location to retrieve", required = true)
        @PathParam("locationId") String locationId,
        @ApiParam(name = "version", value = "The version identifier of the application to retrieve", required = true)
        @PathParam("version") String version) throws Exception;

    @GET
    @Path("/icon/{itemId}/{version}")
    @ApiOperation(value = "Return the icon for a given catalog entry (application/image or HTTP redirect)")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Item not found")
        })
    @Produces("application/image")
    public Response getIcon(
        @ApiParam(name = "itemId", value = "ID of catalog item (application, entity, policy, location)", required=true)
        @PathParam("itemId") String itemId,

        @ApiParam(name = "version", value = "version identifier of catalog item (application, entity, policy, location)", required=true)
        @PathParam("version") String version);
    
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM, MediaType.TEXT_PLAIN})
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Undefined catalog item"),
    })
    @Path("/entities/{itemId}/deprecated")
    public void setDeprecated(
        @ApiParam(name = "itemId", value = "The ID of the catalog item to be deprecated", required = true)
        @PathParam("itemId") String itemId,
        @ApiParam(name = "deprecated", value = "Whether or not the catalog item is deprecated", required = true)
        boolean deprecated);
    
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM, MediaType.TEXT_PLAIN})
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Undefined catalog item"),
    })
    @Path("/entities/{itemId}/disabled")
    public void setDisabled(
        @ApiParam(name = "itemId", value = "The ID of the catalog item to be disabled", required = true)
        @PathParam("itemId") String itemId,
        @ApiParam(name = "disabled", value = "Whether or not the catalog item is disabled", required = true)
        boolean disabled);
}
