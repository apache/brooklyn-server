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


import javax.servlet.ServletConfig;
import javax.ws.rs.Path;

import io.swagger.annotations.Api;
import io.swagger.jaxrs.listing.ApiListingResource;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.brooklyn.rest.apidoc.RestApiResourceScanner;

/**
 * @author Ciprian Ciubotariu <cheepeero@gmx.net>
 */
@Api("API Documentation")
@Path("/apidoc")
public class ApidocResource extends ApiListingResource {

    private void preprocess(Application app, ServletConfig sc) {
        RestApiResourceScanner.rescanIfNeeded(() -> scan(app, sc));
    }

    @Override
    public Response getListing(Application app, ServletConfig sc, HttpHeaders headers, UriInfo uriInfo, String type) {
        preprocess(app, sc);
        return super.getListing(app, sc, headers, uriInfo, type);
    }

    @Override
    public Response getListingJson(Application app, ServletConfig sc, HttpHeaders headers, UriInfo uriInfo) {
        return super.getListingJson(app, sc, headers, uriInfo);
    }

    @Override
    public Response getListingYaml(Application app, ServletConfig sc, HttpHeaders headers, UriInfo uriInfo) {
        return super.getListingYaml(app, sc, headers, uriInfo);
    }

}
