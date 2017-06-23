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
package org.apache.brooklyn.core.mgmt.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

public class AccessManagerTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testEntityManagementAllowed() throws Exception {
        // default is allowed
        TestEntity e1 = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        // when forbidden, should give error trying to create+manage new entity
        mgmt.getAccessManager().setEntityManagementAllowed(false);
        try {
            app.createAndManageChild(EntitySpec.create(TestEntity.class));
            fail();
        } catch (Exception e) {
            // expect it to be forbidden
            if (Exceptions.getFirstThrowableOfType(e, IllegalStateException.class) == null) {
                throw e;
            }
        }

        // when forbidden, should refuse to create new app
        try {
            mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
            fail();
        } catch (Exception e) {
            // expect it to be forbidden
            if (Exceptions.getFirstThrowableOfType(e, IllegalStateException.class) == null) {
                throw e;
            }
        }

        // but when forbidden, still allowed to create locations
        mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
        
        // when re-enabled, can create entities again
        mgmt.getAccessManager().setEntityManagementAllowed(true);
        TestEntity e3 = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        assertEquals(ImmutableSet.copyOf(mgmt.getEntityManager().getEntities()), ImmutableSet.of(app, e1, e3));
    }
    
    @Test
    public void testLocationManagementAllowed() throws Exception {
        // default is allowed
        Location loc1 = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));

        // when forbidden, should give error
        mgmt.getAccessManager().setLocationManagementAllowed(false);
        try {
            mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
            fail();
        } catch (Exception e) {
            // expect it to be forbidden
            if (Exceptions.getFirstThrowableOfType(e, IllegalStateException.class) == null) {
                throw e;
            }
        }

        // but when forbidden, still allowed to create entity
        mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        
        // when re-enabled, can create entities again
        mgmt.getAccessManager().setLocationManagementAllowed(true);
        Location loc3 = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
        
        assertEquals(ImmutableSet.copyOf(mgmt.getLocationManager().getLocations()), ImmutableSet.of(loc1, loc3));
    }
    
    @Test
    public void testLocationProvisioningAllowed() throws Exception {
        SimulatedLocation loc = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
        
        // default is allowed
        assertTrue(mgmt.getAccessController().canProvisionLocation(loc).isAllowed());

        // when forbidden, should say so
        mgmt.getAccessManager().setLocationProvisioningAllowed(false);
        assertFalse(mgmt.getAccessController().canProvisionLocation(loc).isAllowed());

        // but when forbidden, still allowed to create locations
        mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
        
        // when re-enabled, can create entities again
        mgmt.getAccessManager().setLocationProvisioningAllowed(true);
        assertTrue(mgmt.getAccessController().canProvisionLocation(loc).isAllowed());
    }
}
