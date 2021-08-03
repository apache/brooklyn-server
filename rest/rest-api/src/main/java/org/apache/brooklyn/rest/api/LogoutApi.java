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

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Path("/logout")
@Api("Logout")
public interface LogoutApi {

    @POST
    @ApiOperation(value = "Logout and clean session")
    Response logout(
            @ApiParam(value = "Instead of 200 (the default) to indicate successful logout, "
                + "return a 401 with this value in a message key in the body (a 401 will cause browsers to clear some locally cached credentials)", 
                required = false) 
            @QueryParam("unauthorize") String unauthorize,
            
            @ApiParam(value = "Require that this user be logged in", required = false) 
            @QueryParam("user") String user
        );

    // misleading as it does not unauthorize the _session_ or log out in any way;
    // deprecating as at 2019-01
    /** @deprecated since 1.0 in favour of /logout query parameter */
    @Deprecated
    @POST
    @Path("/unauthorize")
    @ApiOperation(value = "Return UNAUTHORIZED 401 response, but without disabling the session [deprecated in favour of /logout query parameter]")
    Response unAuthorize();

    /** @deprecated since 1.0 in favour of /logout query parameter */
    @Deprecated
    @POST
    @Path("/{user}")
    @ApiOperation(value = "Logout and clean session if matching user logged in (deprecated; username should now be omitted)")
    Response logoutUser(
        @ApiParam(value = "User to log out", required = true)
        @PathParam("user") final String user);

}
