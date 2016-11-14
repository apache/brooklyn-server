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
package org.apache.brooklyn.rest.entitlement;

import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class ServerApiEntitlementsTest extends AbstractRestApiEntitlementsTest {

    @Test(groups = "Integration")
    public void testGetHealthy() throws Exception {
        String path = "/v1/server/up";
        assert401(path);
        assertPermitted("myRoot", path);
        assertPermitted("myUser", path);
        assertForbidden("myReadonly", path);
        assertForbidden("myMinimal", path);
        assertForbidden("unrecognisedUser", path);
    }

    @Test(groups = "Integration")
    public void testReloadProperties() throws Exception {
        String resource = "/v1/server/properties/reload";
        assert401(resource);
        assertPermittedPost("myRoot", resource, null);
        assertForbiddenPost("myUser", resource, null);
        assertForbiddenPost("myReadonly", resource, null);
        assertForbiddenPost("myMinimal", resource, null);
        assertForbiddenPost("unrecognisedUser", resource, null);
    }

    @Test(groups = "Integration")
    public void testGetConfig() throws Exception {
        // Property set in test setup.
        String path = "/v1/server/config/" + Entitlements.GLOBAL_ENTITLEMENT_MANAGER.getName();
        assert401(path);
        assertPermitted("myRoot", path);
        assertForbidden("myUser", path);
        assertForbidden("myReadonly", path);
        assertForbidden("myMinimal", path);
        assertForbidden("unrecognisedUser", path);
    }

}
