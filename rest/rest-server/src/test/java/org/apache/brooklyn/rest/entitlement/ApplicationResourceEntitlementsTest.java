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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.ws.rs.WebApplicationException;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementClass;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementManager;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.PerUserEntitlementManager;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.entity.stock.BasicStartableImpl;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.resources.ApplicationResource;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class ApplicationResourceEntitlementsTest extends AbstractRestApiEntitlementsTest {

    private static final AtomicBoolean INDICATOR = new AtomicBoolean();

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        INDICATOR.set(false);
    }

    @Test
    public void testUnentitledDeploy() throws Exception {
        final Set<Application> initialApps = ImmutableSet.copyOf(mgmt.getApplications());
        final String bp = "services:\n- type: " + StartRecordingEntity.class.getName();

        StaticDelegatingEntitlementManager.setDelegate(new InvokeEffector(false));
        httpPost("myCustom", "/v1/applications", bp.getBytes(), ImmutableMap.of("Content-Type", "text/yaml"));
        // Check the app wasn't started
        Sets.SetView<Application> afterApps = Sets.difference(ImmutableSet.copyOf(mgmt.getApplications()), initialApps);
        assertEquals(afterApps.size(), 1, "expected one element: " + afterApps);
        Application newApp = afterApps.iterator().next();
        assertFalse(newApp.sensors().get(Attributes.SERVICE_UP));
        assertFalse(INDICATOR.get());
    }

    public static class InvokeEffector implements EntitlementManager {
        private final boolean mayInvoke;

        public InvokeEffector(boolean mayInvoke) {
            this.mayInvoke = mayInvoke;
        }

        @Override
        public <T> boolean isEntitled(EntitlementContext context, @Nonnull EntitlementClass<T> entitlementClass, T entitlementClassArgument) {
            String type = entitlementClass.entitlementClassIdentifier();
            return !Entitlements.INVOKE_EFFECTOR.entitlementClassIdentifier().equals(type) || mayInvoke;
        }
    }

    @ImplementedBy(StartRecordingEntityImpl.class)
    public interface StartRecordingEntity extends BasicStartable {}
    public static class StartRecordingEntityImpl extends BasicStartableImpl implements StartRecordingEntity {
        @Override
        public void start(Collection<? extends Location> locations) {
            super.start(locations);
            INDICATOR.set(true);
        }
    }

}
