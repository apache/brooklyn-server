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

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.rest.domain.ApplicationSummary;
import org.apache.brooklyn.rest.domain.EntityDetail;
import org.apache.brooklyn.rest.domain.EntitySummary;

import com.google.common.annotations.Beta;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Path("/applications")
@Api("Applications")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface ApplicationApi {

    @GET
    @Path("/details")
    @ApiOperation(
            value = "Get details for all applications and optionally selected additional entity items, "
                + "including tags, values for selected sensor and config glob patterns, "
                + "and recursively this info for children, up to a given depth."
    )
    public List<EntitySummary> details(
            @ApiParam(value="Any additional entity ID's to include, as JSON or comma-separated list; ancestors will also be included", required=false)
            @DefaultValue("")
            @QueryParam("items") String items,
            @ApiParam(value="Whether to include all applications in addition to any explicitly requested IDs; "
                + "default is true so no items need to be listed; "
                + "set false to return only info for entities whose IDs are listed in `items` and their ancestors", required=false)
            @DefaultValue("true")
            @QueryParam("includeAllApps") boolean includeAllApps,
            @ApiParam(value="Any additional sensors to include, as JSON or comma-separated list, accepting globs (* and ?); "
                + "current sensor values if present are returned for each entity in a name-value map under the 'sensors' key", required=false)
            @DefaultValue("")
            @QueryParam("sensors") String sensors,
            @ApiParam(value="Any config to include, as JSON or comma-separated list, accepting globs (* and ?); "
                + "current config values if present are returned for each entity in a name-value map under the 'config' key", required=false)
            @DefaultValue("")
            @QueryParam("config") String config,
            @ApiParam(value="Tree depth to traverse in children for returning detail; "
                + "default 1 means to have detail for just applications and additional entity IDs explicitly requested, "
                + "with references to children but not their details", required=false)
            @DefaultValue("1")
            @QueryParam("depth") int depth);

    @GET
    @Path("/fetch")
    @ApiOperation(
            value = "Fetch details for all applications and optionally selected additional entity items, "
                + "optionally also with the values for selected sensors. "
                + "Deprecated since 1.0.0. Use the '/details' endpoint with better semantics. "
                + "(This returns the complete tree which is wasteful and not usually wanted.)"
    )
    @Deprecated
    /** @deprecated since 1.0.0 use {@link #details(String, String, int)} */
    public List<EntityDetail> fetch(
            @ApiParam(value="Any additional entity ID's to include, as JSON or comma-separated list", required=false)
            @DefaultValue("")
            @QueryParam("items") String items,
            @ApiParam(value="Any additional sensors to include, as JSON or comma-separated list; "
                + "current sensor values if present are returned for each entity in a name-value map under the 'sensors' key", required=false)
            @DefaultValue("")
            @QueryParam("sensors") String sensors);
    
    @GET
    @ApiOperation(
            value = "List a summary object for applications managed here, optionally filtered by a type regex. "
                + "The `details` endpoint returns a more informative record of applications.",
            response = org.apache.brooklyn.rest.domain.ApplicationSummary.class
    )
    public List<ApplicationSummary> list(
            @ApiParam(value = "Regular expression to filter by", required = false)
            @DefaultValue(".*")
            @QueryParam("typeRegex") String typeRegex);

    // would be nice to have this on the API so default type regex not needed, but
    // not yet implemented, as per: https://issues.jboss.org/browse/RESTEASY-798
    // (this method was added to this class, but it breaks the rest client)
//    /** As {@link #list(String)}, filtering for <code>.*</code>. */
//    public List<ApplicationSummary> list();

    @GET
    @Path("/{application}")
    @ApiOperation(
            value = "Fetch details of an application",
            response = org.apache.brooklyn.rest.domain.ApplicationSummary.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Application not found")
    })
    public ApplicationSummary get(
            @ApiParam(
                    value = "ID or name of application whose details will be returned",
                    required = true)
            @PathParam("application") String application);

    @POST
    @Consumes({"application/x-yaml",
            // see http://stackoverflow.com/questions/332129/yaml-mime-type
            "text/yaml", "text/x-yaml", "application/yaml"})
    @ApiOperation(
            value = "Create and start a new application from YAML",
            response = org.apache.brooklyn.rest.domain.TaskSummary.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Undefined entity or location"),
    })
    public Response createFromYaml(
            @ApiParam(
                    name = "applicationSpec",
                    value = "App spec in CAMP YAML format",
                    required = true)
            String yaml);

    @Beta
    @PUT
    @Path("/{application}")
    @Consumes({"application/x-yaml",
            // see http://stackoverflow.com/questions/332129/yaml-mime-type
            "text/yaml", "text/x-yaml", "application/yaml"})
    @ApiOperation(
            value = "[BETA] Create and start a new application from YAML, with the given id",
            response = org.apache.brooklyn.rest.domain.TaskSummary.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Undefined entity or location"),
            @ApiResponse(code = 409, message = "Application already registered")
    })
    public Response createFromYamlWithAppId(
            @ApiParam(name = "applicationSpec", value = "App spec in CAMP YAML format", required = true) String yaml,
            @ApiParam(name = "application", value = "Application id", required = true) @PathParam("application") String appId);

    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM, MediaType.TEXT_PLAIN})
    @ApiOperation(
            value = "Create and start a new application from miscellaneous types, including JSON either new CAMP format or legacy AppSpec format",
            response = org.apache.brooklyn.rest.domain.TaskSummary.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Undefined entity or location")
    })
    public Response createPoly(
            @ApiParam(
                    name = "applicationSpec",
                    value = "App spec in JSON, YAML, or other (auto-detected) format",
                    required = true)
            byte[] autodetectedInput);

    @POST
    @Consumes({MediaType.APPLICATION_FORM_URLENCODED})
    @ApiOperation(
            value = "Create and start a new application from form URL-encoded contents (underlying type autodetected)",
            response = org.apache.brooklyn.rest.domain.TaskSummary.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Undefined entity or location")
    })
    public Response createFromForm(
            @ApiParam(
                    name = "applicationSpec",
                    value = "App spec in form-encoded YAML, JSON, or other (auto-detected) format",
                    required = true)
            @Valid String contents);

    @DELETE
    @Path("/{application}")
    @ApiOperation(
            value = "Delete a specified application",
            response = org.apache.brooklyn.rest.domain.TaskSummary.class
    )
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Application not found")
    })
    public Response delete(
            @ApiParam(
                    name = "application",
                    value = "Application name",
                    required = true)
            @PathParam("application") String application);

    @GET
    @Path("/{application}/descendants")
    @ApiOperation(value = "Fetch entity info for all (or filtered) descendants",
            response = org.apache.brooklyn.rest.domain.EntitySummary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Application or entity missing")
    })
    public List<EntitySummary> getDescendants(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value="Regular expression for an entity type which must be matched", required=false)
            @DefaultValue(".*")
            @QueryParam("typeRegex") String typeRegex);

    @GET
    @Path("/{application}/descendants/sensor/{sensor}")
            @ApiOperation(value = "Fetch values of a given sensor for all (or filtered) descendants")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Application or entity missing")
    })
    public Map<String,Object> getDescendantsSensor(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") String application,
            @ApiParam(value = "Sensor name", required = true)
            @PathParam("sensor") String sensor,
            @ApiParam(value="Regular expression for an entity type which must be matched", required=false)
            @DefaultValue(".*")
            @QueryParam("typeRegex") String typeRegex);

}
