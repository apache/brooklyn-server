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
package org.apache.brooklyn.rest.resources;

import java.net.URI;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;
import org.apache.brooklyn.rest.api.LogoutApi;
import org.apache.brooklyn.util.exceptions.Exceptions;

public class LogoutResource extends AbstractBrooklynRestResource implements LogoutApi {
    @Context HttpServletRequest req;
    @Context UriInfo uri;

    @Override
    public Response redirectToLogout() {
        URI dest = uri.getBaseUriBuilder().path(LogoutApi.class).build();

        // Return response with Javascript which will make an asynchronous POST request to the logout method.
        // (When calling logout it is important to use wrong username and password in order to make browser forget the old ones)
        return Response.status(Status.OK)
                .entity(String.format("<!DOCTYPE html>\n<body>\n" +
                        "<script>\n" +
                        "var a=new window.XMLHttpRequest;" +
                        "a.open('POST','%1$s',0,'user','wrong'+(new Date).getTime().toString());a.send(\"\");\n" +
                        "window.location.href='/';</script></body>", dest.toASCIIString()))
                .build();
    }

    @Override
    public Response logout() {
        WebEntitlementContext ctx = (WebEntitlementContext) Entitlements.getEntitlementContext();

        if (ctx != null && ctx.user() != null) {
            doLogout();
        }

        URI dest = uri.getBaseUriBuilder().build();

        return Response.status(Status.UNAUTHORIZED)
                .header("WWW-Authenticate", "Basic realm=\"webconsole\"")
                // For Status 403, HTTP Location header may be omitted.
                // Location is best to be used for http status 302 https://tools.ietf.org/html/rfc2616#section-10.3.3
                .header("Location", dest.toASCIIString())
                .entity("<script>window.location.replace(\"/\");</script>")
                .build();
    }

    @Override
    @Deprecated
    public Response logoutUser(String user) {
        return Response.status(Status.FOUND)
                .header("Location", uri.getBaseUriBuilder().path(LogoutApi.class, "logout").build())
                .build();
    }

    private void doLogout() {
        try {
            req.logout();
        } catch (ServletException e) {
            Exceptions.propagate(e);
        }

        req.getSession().invalidate();
    }

}
