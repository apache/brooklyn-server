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

import static org.testng.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityRebindTest.MyProvisioningLocation;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest.MyService;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest.MyServiceImpl;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest.SimulatedDriver;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SoftwareProcessEntityFeedRebindTest extends RebindTestFixtureWithApp {
    
    private static final Logger LOG = LoggerFactory.getLogger(SoftwareProcessEntityFeedRebindTest.class);

    @Override
    protected boolean enablePersistenceBackups() {
        return false;
    }

    @Test
    public void testFeedsDoNotPollUntilManaged() throws Exception {
        runFeedsDoNotPollUntilManaged(1);
    }

    @Test(groups="Integeration")
    public void testFeedsDoNotPollUntilManagedManyEntities() throws Exception {
        runFeedsDoNotPollUntilManaged(100);
    }
    
    protected void runFeedsDoNotPollUntilManaged(int numEntities) throws Exception {
        List<MyService> origEs = Lists.newArrayList();
        
        LOG.info("Creating "+numEntities+" entities");
        for (int i = 0; i < numEntities; i++) {
            origEs.add(origApp.createAndManageChild(EntitySpec.create(MyService.class)
                    .impl(MyServiceWithFeedsImpl.class)
                    .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)));
        }
        
        LOG.info("Starting "+numEntities+" entities");
        MyProvisioningLocation origLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(MyProvisioningLocation.class)
                .displayName("mylocname"));
        origApp.start(ImmutableList.of(origLoc));

        LOG.info("Rebinding "+numEntities+" entities");
        newApp = (TestApplication) rebind();
        List<Entity> newEs = ImmutableList.copyOf(newApp.getChildren());
        
        LOG.info("Checking state of "+numEntities+" entities");
        for (Entity newE : newEs) {
            EntityAsserts.assertAttributeChangesEventually(newE, MyServiceWithFeeds.COUNTER);
            assertFalse(((MyServiceWithFeeds)newE).isFeedCalledWhenNotManaged());
            SimulatedDriverWithFeeds driver = (SimulatedDriverWithFeeds) ((MyServiceWithFeeds)newE).getDriver();
            assertFalse(driver.isRunningCalledWhenNotManaged);
        }
    }

    public static interface MyServiceWithFeeds extends MyService {
        AttributeSensor<Integer> COUNTER = Sensors.newIntegerSensor("counter");
        
        boolean isFeedCalledWhenNotManaged();
    }
    
    public static class MyServiceWithFeedsImpl extends MyServiceImpl implements MyServiceWithFeeds {
        protected FunctionFeed functionFeed;
        protected boolean feedCalledWhenNotManaged;
        
        @Override
        public boolean isFeedCalledWhenNotManaged() {
            return feedCalledWhenNotManaged;
        }
        
        @Override
        public void init() {
            super.init();
            
            // By calling feeds().add(...), it will persist the feed, and rebind it
            functionFeed = feeds().add(FunctionFeed.builder()
                    .entity(this)
                    .period(Duration.millis(10))
                    .uniqueTag("MyserviceWithFeeds-functionFeed")
                    .poll(new FunctionPollConfig<Integer, Integer>(COUNTER)
                            .suppressDuplicates(true)
                            .onException(Functions.constant(-1))
                            .callable(new Callable<Integer>() {
                                public Integer call() {
                                    if (!Entities.isManaged(MyServiceWithFeedsImpl.this)) {
                                        feedCalledWhenNotManaged = true;
                                        throw new IllegalStateException("Entity "+MyServiceWithFeedsImpl.this+" is not managed in feed.call");
                                    }
                                    Integer oldVal = sensors().get(COUNTER);
                                    return (oldVal == null ? 0 : oldVal) + 1;
                                }
                            }))
                    .build());
        }
        
        @Override
        protected void connectSensors() {
            // connectSensors is called on rebind; it will re-register the feed
            super.connectSensors();
            super.connectServiceUpIsRunning();
        }

        @Override
        protected void disconnectSensors() {
            super.disconnectSensors();
            super.disconnectServiceUpIsRunning();
        }
        
        @Override
        public Class<?> getDriverInterface() {
            return SimulatedDriverWithFeeds.class;
        }
    }
    
    public static class SimulatedDriverWithFeeds extends SimulatedDriver {
        protected boolean isRunningCalledWhenNotManaged = false;
        
        public SimulatedDriverWithFeeds(EntityLocal entity, SshMachineLocation machine) {
            super(entity, machine);
        }
        
        @Override
        public boolean isRunning() {
            if (!Entities.isManaged(entity)) {
                isRunningCalledWhenNotManaged = true;
                throw new IllegalStateException("Entity "+entity+" is not managed in driver.isRunning");
            }
            return super.isRunning();
        }
    }
}
