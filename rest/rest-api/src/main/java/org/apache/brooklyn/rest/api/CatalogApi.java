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

import org.apache.brooklyn.rest.domain.CatalogEnricherSummary;
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
@Api(value = "Catalog (deprecated; use Catalog Types endpoint)", hidden = true)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Deprecated
/** @deprecated since 1.1 use {@link TypeApi} instead */
public interface CatalogApi {

    /** @deprecated since 0.11.0 use {@link #createFromYaml(String, boolean)} instead */
    @Deprecated
    @Consumes("application/deprecated-yaml-old")  // prevent this from taking things
    @POST
    @ApiOperation(value = "(deprecated)", hidden = true, response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response create(String yaml,
            @ApiParam(name="forceUpdate", value="Force update of catalog item (overwriting existing catalog items with same name and version)")
            @QueryParam("forceUpdate") @DefaultValue("false") boolean forceUpdate);

    /** @deprecated since 1.1 use {@link #create(byte[], String, boolean, boolean, boolean)} instead */
    @Deprecated
    @POST
    @Consumes("application/deprecated-yaml")
    @ApiOperation(value = "(deprecated)", hidden = true, response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response createFromYaml(
            @ApiParam(name = "yaml", value = "YAML descriptor of catalog item", required = true)
            @Valid String yaml,
            @ApiParam(name="forceUpdate", value="Force update of catalog item (overwriting existing catalog items with same name and version)")
            @QueryParam("forceUpdate") @DefaultValue("false")
            boolean forceUpdate);

    /** @deprecated since 1.1 use {@link #create(byte[], String, boolean, boolean, boolean)} instead */
    @Deprecated
    @POST
    @Consumes("application/deprecated-x-zip")
    @ApiOperation(value = "(deprecated)", hidden = true, response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response createFromArchive(
            @ApiParam(
                    name = "archive",
                    value = "Bundle to install, in ZIP or JAR format, requiring catalog.bom containing bundle name and version",
                    required = true)
            byte[] archive,
            @ApiParam(name="detail", value="Provide a wrapping details map", required=false)
            @QueryParam("detail") @DefaultValue("false")
            boolean detail,
            @ApiParam(name="forceUpdate", value="Force update of catalog item (overwriting existing catalog items with same name and version)")
            @QueryParam("forceUpdate") @DefaultValue("false")
            boolean forceUpdate);

    @Beta
    /** @deprecated since 1.1 use {@link #create(byte[], String, boolean, boolean, boolean)} instead */
    @Deprecated
    @POST
    @Consumes("application/deprecated-autodetect")
    @ApiOperation(value = "(deprecated)", hidden = true, response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response createFromUpload(
            @ApiParam(
                    name = "item",
                    value = "Item to install, as JAR/ZIP or Catalog YAML (autodetected)",
                    required = true)
                    byte[] item,
            @ApiParam(name="forceUpdate", value="Force update of catalog item (overwriting existing catalog items with same name and version)")
            @QueryParam("forceUpdate") @DefaultValue("false")
                    boolean forceUpdate);

    /** @deprecated since 1.1.0 delete the bundle via DELETE /catalog/bundles/xxx */
    // but we will probably keep this around for a while as many places use it
    // the /bundles endpoint is preferred and things (Go client and UI) should be switched to use it exclusively
    @Deprecated
    @Beta
    @POST
    @Consumes // anything - now autodetect is done for everything unless 'format' is specified
    // (mime type is ignored; though it could be useful in the "score" function, and probably is available on the thread)
    @ApiOperation(
            value = "Add a bundle of types (entities, etc) to the type registry (deprecated, use /catalog/bundles endpoint instead)",
            notes = "This will auto-detect the format, with the 'brooklyn-bom-bundle' being common and consisting of "
                    + "a ZIP/JAR containing a catalog.bom. "
                    + "Return value is map of ID to CatalogItemSummary unless detail=true is passed as a parameter in which "
                    + "case the return value is a BundleInstallationRestResult map containing the types map in types along "
                    + "with a message, bundle, and code.",
            response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 201, message = "Catalog items added successfully"),
            @ApiResponse(code = 400, message = "Error processing the given archive, or the catalog.bom is invalid"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Response create(
            @ApiParam(
                    name = "bundle",
                    value = "Bundle contents to install, eg for brooklyn-catalog-bundle a ZIP or JAR containing a catalog.bom file (deprecated, use /catalog/bundles endpoint instead)",
                    required = true)
                    byte[] archive,
            @ApiParam(name="format", value="Specify the format to indicate a specific resolver for handling this", required=false)
            @QueryParam("format") @DefaultValue("")
                    String format,
            @ApiParam(name="detail", value="Provide a wrapping details map (false for backwards compatibility, but true is recommended for migration to bundle API)", required=false)
            @QueryParam("detail") @DefaultValue("false")
                    boolean detail,
            @ApiParam(name="itemDetails", value="Include legacy item details in the map of types (true for backwards compatibility, but false is recommended for migration to bundle API)", required=false)
            @QueryParam("itemDetails") @DefaultValue("true")
                    boolean itemDetails,
            @ApiParam(name="forceUpdate", value="Force update of catalog item (overwriting existing catalog items with same name and version)")
            @QueryParam("forceUpdate") @DefaultValue("false")
                    boolean forceUpdate);
    
    /** @deprecated since 1.0.0 delete the bundle via DELETE /catalog/bundles/xxx */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @DELETE
    @Path("/applications/{symbolicName}/{version}")
    @ApiOperation(
            value = "Deletes a specific version of an application's definition from the catalog (deprecated, use /catalog/bundles endpoint instead)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'symbolicName'"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void deleteApplication(
        @ApiParam(name = "symbolicName", value = "The symbolic name of the application or template to delete", required = true)
        @PathParam("symbolicName") String symbolicName,

        @ApiParam(name = "version", value = "The version identifier of the application or template to delete", required = true)
        @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.0.0 delete the bundle via DELETE /catalog/bundles/xxx */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @DELETE
    @Path("/entities/{symbolicName}/{version}")
    @ApiOperation(
            value = "Deletes a specific version of an entity's definition from the catalog (deprecated, use /catalog/bundles endpoint instead, as we add/delete bundles now)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'symbolicName'"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void deleteEntity(
        @ApiParam(name = "symbolicName", value = "The symbolic name of the entity or template to delete", required = true)
        @PathParam("symbolicName") String symbolicName,

        @ApiParam(name = "version", value = "The version identifier of the entity or template to delete", required = true)
        @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.0.0 delete the bundle via DELETE /catalog/bundles/xxx */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @DELETE
    @Path("/policies/{policyId}/{version}")
    @ApiOperation(
            value = "Deletes a specific version of an policy's definition from the catalog (deprecated, use /catalog/bundles endpoint instead, as we add/delete bundles now)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'policyId'")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void deletePolicy(
        @ApiParam(name = "policyId", value = "The ID of the policy to delete", required = true)
        @PathParam("policyId") String policyId,

        @ApiParam(name = "version", value = "The version identifier of the policy to delete", required = true)
        @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.0.0 delete the bundle via DELETE /catalog/bundles/xxx */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @DELETE
    @Path("/locations/{locationId}/{version}")
    @ApiOperation(
            value = "Deletes a specific version of an location's definition from the catalog (deprecated, use /catalog/bundles endpoint instead, as we add/delete bundles now)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'locationId'"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void deleteLocation(
        @ApiParam(name = "locationId", value = "The ID of the location to delete", required = true)
        @PathParam("locationId") String locationId,

        @ApiParam(name = "version", value = "The version identifier of the location to delete", required = true)
        @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/entities")
    @ApiOperation(value = "List available entity types optionally matching a query  (deprecated, use /catalog/types endpoint instead, with supertype=entity)", 
            response = CatalogItemSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<CatalogEntitySummary> listEntities(
        @ApiParam(name = "regex", value = "Regular expression to search for")
        @QueryParam("regex") @DefaultValue("") String regex,
        @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
        @QueryParam("fragment") @DefaultValue("") String fragment,
        @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
        @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    // bad name - it is just templates
    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/applications")
    @ApiOperation(value = "Fetch a list of templates (for applications) optionally matching a query (deprecated, use /catalog/types endpoint instead, with supertype=application; note some semantics of templates are changing as definition becomes more precise)", 
            response = CatalogItemSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<CatalogItemSummary> listApplications(
            @ApiParam(name = "regex", value = "Regular expression to search for")
            @QueryParam("regex") @DefaultValue("") String regex,
            @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
            @QueryParam("fragment") @DefaultValue("") String fragment,
            @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
            @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/entities/{symbolicName}/{version}")
    @ApiOperation(
            value = "Fetch a specific version of an entity's definition from the catalog (deprecated, use /catalog/types endpoint instead)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'symbolicName'",
            response = CatalogEntitySummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public CatalogEntitySummary getEntity(
        @ApiParam(name = "symbolicName", value = "The symbolic name of the entity or template to retrieve", required = true)
        @PathParam("symbolicName") String symbolicName,

        @ApiParam(name = "version", value = "The version identifier of the entity or template to retrieve", required = true)
        @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/applications/{symbolicName}/{version}")
    @ApiOperation(
            value = "Fetch a specific version of an application's definition from the catalog (deprecated, use /catalog/types endpoint instead)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'symbolicName'",
            response = CatalogEntitySummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public CatalogEntitySummary getApplication(
        @ApiParam(name = "symbolicName", value = "The symbolic name of the application to retrieve", required = true)
        @PathParam("symbolicName") String symbolicName,

        @ApiParam(name = "version", value = "The version identifier of the application to retrieve", required = true)
        @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/policies")
    @ApiOperation(value = "List available policies optionally matching a query (deprecated, use /catalog/types endpoint instead)", 
            response = CatalogPolicySummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<CatalogPolicySummary> listPolicies(
            @ApiParam(name = "regex", value = "Regular expression to search for")
            @QueryParam("regex") @DefaultValue("") String regex,
            @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
            @QueryParam("fragment") @DefaultValue("") String fragment,
            @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
            @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/policies/{policyId}/{version}")
    @ApiOperation(
            value = "Fetch a policy's definition from the catalog (deprecated, use /catalog/types endpoint instead)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'policyId'",
            response = CatalogItemSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public CatalogPolicySummary getPolicy(
        @ApiParam(name = "policyId", value = "The ID of the policy to retrieve", required = true)
        @PathParam("policyId") String policyId,
        @ApiParam(name = "version", value = "The version identifier of the application to retrieve", required = true)
        @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/locations")
    @ApiOperation(value = "List available locations optionally matching a query (deprecated, use /catalog/types endpoint instead)", 
            response = CatalogLocationSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<CatalogLocationSummary> listLocations(
            @ApiParam(name = "regex", value = "Regular expression to search for")
            @QueryParam("regex") @DefaultValue("") String regex,
            @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
            @QueryParam("fragment") @DefaultValue("") String fragment,
            @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
            @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/locations/{locationId}/{version}")
    @ApiOperation(
            value = "Fetch a location's definition from the catalog (deprecated, use /catalog/types endpoint instead)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'locationId'",
            response = CatalogItemSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public CatalogItemSummary getLocation(
        @ApiParam(name = "locationId", value = "The ID of the location to retrieve", required = true)
        @PathParam("locationId") String locationId,
        @ApiParam(name = "version", value = "The version identifier of the application to retrieve", required = true)
        @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.1.0 delete the bundle via DELETE /catalog/bundles/xxx */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/icon/{itemId}/{version}")
    @ApiOperation(
            value = "Return the icon for a given catalog entry (application/image or HTTP redirect) (deprecated, use /catalog/types/.../icon endpoint instead)",
            notes = "Version must exists, otherwise the API will return a 404. Alternatively, passing 'latest' will" +
                    "pick up the latest version for the given 'itemId'"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Produces("application/image")
    public Response getIcon(
        @ApiParam(name = "itemId", value = "ID of catalog item (application, entity, policy, location)", required=true)
        @PathParam("itemId") String itemId,

        @ApiParam(name = "version", value = "version identifier of catalog item (application, entity, policy, location)", required=true)
        @PathParam("version") String version);
    
    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=...;
     * deprecation/disabling needs to be done in the bundle, and we might support deprecating/disabling bundles */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM, MediaType.TEXT_PLAIN})
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @ApiOperation(value = "Deprecate an item (deprecated, use /catalog/types endpoint instead, but disabled/deprecating is not supported for individual types)")
    @Path("/entities/{itemId}/deprecated")
    public void setDeprecated(
        @ApiParam(name = "itemId", value = "The ID of the catalog item to be deprecated", required = true)
        @PathParam("itemId") String itemId,
        @ApiParam(name = "deprecated", value = "Whether or not the catalog item is deprecated", required = true)
        boolean deprecated);
    
    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=...;
     * deprecation/disabling needs to be done in the bundle, and we might support deprecating/disabling bundles */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM, MediaType.TEXT_PLAIN})
    @ApiOperation(value = "Disable an item (deprecated, use /catalog/types endpoint instead, but disabled/deprecating is not supported for individual types)")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Path("/entities/{itemId}/disabled")
    public void setDisabled(
        @ApiParam(name = "itemId", value = "The ID of the catalog item to be disabled", required = true)
        @PathParam("itemId") String itemId,
        @ApiParam(name = "disabled", value = "Whether or not the catalog item is disabled", required = true)
        boolean disabled);

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/enrichers")
    @ApiOperation(value = "List available enrichers types optionally matching a query (deprecated, use /catalog/types endpoint instead)",
            response = CatalogItemSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application or entity missing"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<CatalogEnricherSummary> listEnrichers(
            @ApiParam(name = "regex", value = "Regular expression to search for")
            @QueryParam("regex") @DefaultValue("") String regex,
            @ApiParam(name = "fragment", value = "Substring case-insensitive to search for")
            @QueryParam("fragment") @DefaultValue("") String fragment,
            @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)")
            @QueryParam("allVersions") @DefaultValue("false") boolean includeAllVersions);

    /** @deprecated since 1.0.0 use /catalog/bundles and /catalog/types?supertype=... */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @GET
    @Path("/enrichers/{enricherId}/{version}")
    @ApiOperation(value = "Fetch an enricher's definition from the catalog (deprecated, use /catalog/types endpoint instead)",
            response = CatalogItemSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Enricher not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public CatalogEnricherSummary getEnricher(
            @ApiParam(name = "enricherId", value = "The ID of the enricher to retrieve", required = true)
            @PathParam("enricherId") String enricherId,
            @ApiParam(name = "version", value = "The version identifier of the enricher to retrieve", required = true)
            @PathParam("version") String version) throws Exception;

    /** @deprecated since 1.0.0 delete the bundle via DELETE /catalog/bundles/xxx */
    // but we will probably keep this around for a while as many places use it
    @Deprecated
    @DELETE
    @Path("/enrichers/{enricherId}/{version}")
    @ApiOperation(value = "Deletes a specific version of an enricher's definition from the catalog (deprecated, use /catalog/types endpoint instead)")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Enricher not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void deleteEnricher(
            @ApiParam(name = "enricherId", value = "The ID of the enricher to delete", required = true)
            @PathParam("enricherId") String enricherId,
            @ApiParam(name = "version", value = "The version identifier of the enricher to delete", required = true)
            @PathParam("version") String version) throws Exception;
}
