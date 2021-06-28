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

import com.google.common.base.Preconditions;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.rest.api.LogbookApi;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.core.logbook.BrooklynLogEntry;
import org.apache.brooklyn.util.core.logbook.DelegatingLogStore;
import org.apache.brooklyn.util.core.logbook.LogBookQueryParams;
import org.apache.brooklyn.util.core.logbook.LogStore;
import org.apache.brooklyn.util.exceptions.Exceptions;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

public class LogbookResource extends AbstractBrooklynRestResource implements LogbookApi {

    @Override
    public Response logbookQuery(HttpServletRequest request, LogBookQueryParams params) {

        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.LOGBOOK_LOG_STORE_QUERY, null)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to perform this operation", Entitlements.getEntitlementContext().user());
        }

        Preconditions.checkNotNull(params, "params must not be null");

        LogStore logStore = new DelegatingLogStore(mgmt()).getDelegate();
        try {
            List<BrooklynLogEntry> logs = logStore.query(params);
            return Response
                    .ok(logs, MediaType.APPLICATION_JSON)
                    .build();
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }
}
