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

import io.swagger.annotations.Api;
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

import org.apache.brooklyn.rest.domain.SensorSummary;

import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Path("/applications/{application}/entities/{entity}/sensors")
@Api("Entity Sensors")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface SensorApi {

    @GET
    @ApiOperation(value = "Fetch the sensor list for a specific application entity",
            response = org.apache.brooklyn.rest.domain.SensorSummary.class,
            responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find application or entity"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public List<SensorSummary> list(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken);

    @GET
    @Path("/current-state")
    @ApiOperation(value = "Fetch sensor values in batch", notes="Returns a map of sensor name to value")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Could not find application or entity"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Map<String, Object> batchSensorRead(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,

            @ApiParam(value = "Whether to format/annotate values with hints for for display", required = false)
            @QueryParam("useDisplayHints") @DefaultValue("true") final Boolean useDisplayHints,
            @ApiParam(value = "Return raw sensor data instead of display values (deprecated, see useDisplayHints)", required = false)
            @Deprecated @QueryParam("raw") @DefaultValue("false") final Boolean raw,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @GET
    @Path("/{sensor}")
    @ApiOperation(value = "Fetch sensor value (json)", response = Object.class)
    @Produces({MediaType.APPLICATION_JSON})
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK. The sensor value is returned."),
            @ApiResponse(code = 204, message = "No Content. The sensor is known, but unset."),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application, entity, or sensor not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public Object get(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Sensor name", required = true)
            @PathParam("sensor") String sensorName,

            @ApiParam(value = "Whether to format/annotate values with hints for for display", required = false)
            @QueryParam("useDisplayHints") @DefaultValue("true") final Boolean useDisplayHints,
            @ApiParam(value = "Return raw sensor data instead of display values (deprecated, see useDisplayHints)", required = false)
            @Deprecated @QueryParam("raw") @DefaultValue("false") final Boolean raw,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    // this method is used if user has requested plain (ie not converting to json)
    @GET
    @Path("/{sensor}")
    @ApiOperation(value = "Fetch sensor value (text/plain)", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application, entity, or sensor not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @Produces(MediaType.TEXT_PLAIN + ";qs=0.9")
    public String getPlain(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Sensor name", required = true)
            @PathParam("sensor") String sensorName,

            @ApiParam(value = "Whether to format/annotate values with hints for for display", required = false)
            @QueryParam("useDisplayHints") @DefaultValue("true") final Boolean useDisplayHints,
            @ApiParam(value = "Return raw sensor data instead of display values (deprecated, see useDisplayHints)", required = false)
            @Deprecated @QueryParam("raw") @DefaultValue("false") final Boolean raw,
            @ApiParam(value = "Whether to suppress secrets", required = false)
            @QueryParam("suppressSecrets") final Boolean suppressSecrets);

    @POST
    @ApiOperation(value = "Manually set multiple sensor values")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "No Content"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application, entity, or sensor not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    @SuppressWarnings("rawtypes")
    public void setFromMap(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Map of sensor names to values", required = true)
            Map newValues);

    @POST
    @Path("/{sensor}")
    @ApiOperation(value = "Manually set a sensor value")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application, entity, or sensor not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void set(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Sensor name", required = true)
            @PathParam("sensor") String sensorName,
            @ApiParam(value = "Value to set")
            Object newValue);

    @DELETE
    @Path("/{sensor}")
    @ApiOperation(value = "Manually clear a sensor value")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Bad Request"),
            @ApiResponse(code = 401, message = "Unauthorized"),
            @ApiResponse(code = 404, message = "Application, entity, or sensor not found"),
            @ApiResponse(code = 500, message = "Internal Server Error")
    })
    public void delete(
            @ApiParam(value = "Application ID or name", required = true)
            @PathParam("application") final String application,
            @ApiParam(value = "Entity ID or name", required = true)
            @PathParam("entity") final String entityToken,
            @ApiParam(value = "Sensor name", required = true)
            @PathParam("sensor") String sensorName);
}
