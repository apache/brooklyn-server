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

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;

@Path("/logout")
@Api("Logout")
public interface LogoutApi {
    @POST
    @ApiOperation(value = "Request a logout and clean session")
    @ApiResponse(code = 401, message = "Unauthorized")
    Response logout();

    @POST
    @Path("/redirect")
    @ApiOperation(value = "Intermediate redirect for browsers in order to make sure browser will reload its DOM")
    Response redirectToLogout();

    @POST
    @Path("/{user}")
    @ApiOperation(value = "Deprecated: Logout and clean session if matching user logged")
    @Deprecated
    Response logoutUser(
        @ApiParam(value = "User to log out", required = true)
        @PathParam("user") final String user);
}
