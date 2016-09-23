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

package org.apache.brooklyn.entity.software.base;

import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;

public class SoftwareProcessRebindNotRunningEntityTest extends BrooklynAppUnitTestSupport {

    private List<LocalhostMachineProvisioningLocation> locations;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        locations =  ImmutableList.of(app.newLocalhostProvisioningLocation());
    }

    //TODO cover more cases and all entity states

    @Test
    public void testRebindAfterStarting() {
        MyService entity = app.createAndManageChild(EntitySpec.create(MyService.class));
        entity.start(locations); //FIXME - NPE
        //TODO find better solution to rebind starting app than only setting its attributes
        ((MyServiceImpl) entity).setAttribute(entity.SERVICE_STATE_EXPECTED, new Lifecycle.Transition(Lifecycle.ON_FIRE, new Date()));
        ((MyServiceImpl) entity).setAttribute(entity.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);
        ((MyServiceImpl) entity).rebind();

        assertEntityIsOnFire(entity);
    }

    @Test
    public void testRebindAfterStopping() {

    }

    @Test
    public void testRebindRunningEntity() {
        MyService entity = app.createAndManageChild(EntitySpec.create(MyService.class));
        entity.start(locations);
        ((MyServiceImpl) entity).rebind();
        assertEntityIsRunning(entity);
    }

    //TODO - more precise assert?
    protected void assertEntityIsOnFire(MyService entity) {
        EntityAsserts.assertAttributeEquals(entity, entity.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        //TODO assert SERVICE_STATE_EXPECTED
    }

    protected void assertEntityIsRunning(MyService entity) {
        Asserts.assertTrue(((MyServiceImpl) entity).getDriver().isRunning());
    }

    //TODO
    @ImplementedBy(MyServiceImpl.class)
    public interface MyService extends SoftwareProcess {
    }

    public static class MyServiceImpl extends SoftwareProcessImpl implements MyService {
        public MyServiceImpl() {}
        public MyServiceImpl(Entity parent) { super(parent); }

        @Override
        public Class getDriverInterface() {
            return null;
        }
    }
}