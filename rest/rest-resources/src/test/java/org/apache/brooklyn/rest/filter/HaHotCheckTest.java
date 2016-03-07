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
package org.apache.brooklyn.rest.filter;

import static org.testng.Assert.assertEquals;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityManager;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.rest.util.TestingHaHotStateCheckClassResource;
import org.apache.brooklyn.rest.util.TestingHaHotStateCheckResource;
import org.apache.brooklyn.rest.util.TestingHaMasterCheckResource;
import org.apache.cxf.jaxrs.client.WebClient;
import org.testng.annotations.Test;

public class HaHotCheckTest extends BrooklynRestResourceTest {

    @Override
    protected void addBrooklynResources() {
        addResource(new HaHotCheckResourceFilter());
        addResource(new TestingHaHotStateCheckResource());
        addResource(new TestingHaHotStateCheckClassResource());
        addResource(new TestingHaMasterCheckResource());
        
        ((LocalManagementContext)getManagementContext()).noteStartupComplete();
    }

    @Override
    protected boolean isMethodInit() {
        return true;
    }

    @Test
    public void testHaCheck() {
        HighAvailabilityManager ha = getManagementContext().getHighAvailabilityManager();
        assertEquals(ha.getNodeState(), ManagementNodeState.MASTER);
        testResourceFetch("/ha/method/ok", 200);
        testResourceFetch("/ha/method/fail", 200);
        testResourceFetch("/ha/class/fail", 200);
        testResourcePost("/ha/post", 204);
        testResourcePost("/server/shutdown", 204);

        getManagementContext().getHighAvailabilityManager().changeMode(HighAvailabilityMode.STANDBY);
        assertEquals(ha.getNodeState(), ManagementNodeState.STANDBY);

        testResourceFetch("/ha/method/ok", 200);
        testResourceFetch("/ha/method/fail", 403);
        testResourceFetch("/ha/class/fail", 403);
        testResourcePost("/ha/post", 403);
        testResourcePost("/server/shutdown", 204);

        ((ManagementContextInternal)getManagementContext()).terminate();
        assertEquals(ha.getNodeState(), ManagementNodeState.TERMINATED);

        testResourceFetch("/ha/method/ok", 200);
        testResourceFetch("/ha/method/fail", 403);
        testResourceFetch("/ha/class/fail", 403);
        testResourcePost("/ha/post", 403);
        testResourcePost("/server/shutdown", 204);
    }

    @Test
    public void testHaCheckForce() {
        HighAvailabilityManager ha = getManagementContext().getHighAvailabilityManager();
        assertEquals(ha.getNodeState(), ManagementNodeState.MASTER);
        testResourceForcedFetch("/ha/method/ok", 200);
        testResourceForcedFetch("/ha/method/fail", 200);
        testResourceForcedFetch("/ha/class/fail", 200);
        testResourceForcedPost("/ha/post", 204);
        testResourceForcedPost("/server/shutdown", 204);

        getManagementContext().getHighAvailabilityManager().changeMode(HighAvailabilityMode.STANDBY);
        assertEquals(ha.getNodeState(), ManagementNodeState.STANDBY);

        testResourceForcedFetch("/ha/method/ok", 200);
        testResourceForcedFetch("/ha/method/fail", 200);
        testResourceForcedFetch("/ha/class/fail", 200);
        testResourceForcedPost("/ha/post", 204);
        testResourceForcedPost("/server/shutdown", 204);

        ((ManagementContextInternal)getManagementContext()).terminate();
        assertEquals(ha.getNodeState(), ManagementNodeState.TERMINATED);

        testResourceForcedFetch("/ha/method/ok", 200);
        testResourceForcedFetch("/ha/method/fail", 200);
        testResourceForcedFetch("/ha/class/fail", 200);
        testResourceForcedPost("/ha/post", 204);
        testResourceForcedPost("/server/shutdown", 204);
    }


    private void testResourceFetch(String resourcePath, int code) {
        testResourceFetch(resourcePath, false, code);
    }

    private void testResourcePost(String resourcePath, int code) {
        testResourcePost(resourcePath, false, code);
    }

    private void testResourceForcedFetch(String resourcePath, int code) {
        testResourceFetch(resourcePath, true, code);
    }

    private void testResourceForcedPost(String resourcePath, int code) {
        testResourcePost(resourcePath, true, code);
    }

    private void testResourceFetch(String resourcePath, boolean force, int code) {
        WebClient resource = client().path(resourcePath)
                .accept(MediaType.APPLICATION_JSON_TYPE);
        if (force) {
            resource.header(HaHotCheckResourceFilter.SKIP_CHECK_HEADER, "true");
        }
        Response response = resource
                .get();
        assertEquals(response.getStatus(), code);
    }

    private void testResourcePost(String resourcePath, boolean force, int code) {
        WebClient resource = client().path(resourcePath)
                .accept(MediaType.APPLICATION_JSON_TYPE);
        if (force) {
            resource.header(HaHotCheckResourceFilter.SKIP_CHECK_HEADER, "true");
        }
        Response response = resource.post(null);
        assertEquals(response.getStatus(), code);
    }

}
