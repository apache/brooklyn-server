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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityRebindTest.MyProvisioningLocation;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest.SimulatedDriver;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
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
        runFeedsDoNotPollUntilManaged(1, Duration.millis(250));
    }

    @Test(groups="Integration")
    public void testFeedsDoNotPollUntilManagedManyEntities() throws Exception {
        runFeedsDoNotPollUntilManaged(100, Duration.ONE_SECOND);
    }

    /**
     * Test for https://issues.apache.org/jira/browse/BROOKLYN-322.
     * 
     * The entity registers a couple of feeds: the standard connectServiceUpIsRunning, and a custom
     * poller that is registered during init (and persisted). We do a few assertions after rebind:
     * <ol>
     *   <li>The persisted-feed is active (so changes the sensor value)
     *   <li>The persisted-feed did not executed before the entity was managed.
     *   <li>The driver.isRunning (called periodically by connectServiceUpIsRunning) was never
     *       called before the entity was managed.
     *   <li>The entity's state was never reported as faulty. We check the service.state, service.isUp
     *       and the service.process.isRunning.
     * </ol>
     * 
     * This tests both the underlying cause, and the symptoms.
     */
    protected void runFeedsDoNotPollUntilManaged(int numEntities, Duration delayAfterRebind) throws Exception {
        List<MyServiceWithFeeds> origEs = Lists.newArrayList();
        
        LOG.info("Creating "+numEntities+" entities");
        for (int i = 0; i < numEntities; i++) {
            origEs.add(origApp.createAndManageChild(EntitySpec.create(MyServiceWithFeeds.class)
                    .configure(SoftwareProcess.SERVICE_PROCESS_IS_RUNNING_POLL_PERIOD, Duration.millis(10))
                    .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)));
        }
        
        LOG.info("Starting "+numEntities+" entities");
        MyProvisioningLocation origLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(MyProvisioningLocation.class)
                .displayName("mylocname"));
        origApp.start(ImmutableList.of(origLoc));

        for (Entity child : origApp.getChildren()) {
            EntityAsserts.assertAttributeEquals(child, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
            EntityAsserts.assertAttributeEquals(child, Attributes.SERVICE_UP, Boolean.TRUE);
            EntityAsserts.assertAttributeEquals(child, SoftwareProcess.SERVICE_PROCESS_IS_RUNNING, Boolean.TRUE);
        }

        LOG.info("Rebinding "+numEntities+" entities");
        newApp = (TestApplication) rebind();
        
        // Slight pause is to give the feeds a chance to execute, to publish their event(s)
        Duration.sleep(delayAfterRebind);
        
        LOG.info("Checking state of "+numEntities+" entities, after rebind");
        for (Entity newERaw : newApp.getChildren()) {
            MyServiceWithFeeds newE = (MyServiceWithFeeds) newERaw;
            EntityAsserts.assertAttributeChangesEventually(newE, MyServiceWithFeeds.COUNTER);
            assertFalse(newE.isFeedCalledWhenNotManaged());
            
            SimulatedDriverWithFeeds driver = (SimulatedDriverWithFeeds) newE.getDriver();
            assertFalse(driver.isRunningCalledWhenNotManaged);
            
            List<Lifecycle> states = newE.getServiceStateEvents();
            Lifecycle currentState = newE.sensors().get(Attributes.SERVICE_STATE_ACTUAL);
            List<Boolean> ups = newE.getServiceUpEvents();
            Boolean currentUp = newE.sensors().get(Attributes.SERVICE_UP);
            List<Boolean> processRunnings = newE.getProcessRunningEvents();
            Boolean currentProcessRunning = newE.sensors().get(SoftwareProcess.SERVICE_PROCESS_IS_RUNNING);
            String errMsg = "Entity "+newE+": states="+states+"; current="+currentState+"; ups="+ups+"; current="+currentUp+"; processRunnings="+processRunnings+"; current="+currentProcessRunning;
            LOG.info(errMsg);
            assertFalse(states.contains(Lifecycle.ON_FIRE), errMsg);
            assertEquals(currentState, Lifecycle.RUNNING, errMsg);
            assertFalse(ups.contains(Boolean.FALSE), errMsg);
            assertEquals(currentUp, Boolean.TRUE, errMsg);
            assertFalse(processRunnings.contains(Boolean.FALSE), errMsg);
            assertEquals(currentProcessRunning, Boolean.TRUE, errMsg);
        }
    }

    @ImplementedBy(MyServiceWithFeedsImpl.class)
    public static interface MyServiceWithFeeds extends SoftwareProcess {
        AttributeSensor<Integer> COUNTER = Sensors.newIntegerSensor("counter");
        
        SoftwareProcessDriver getDriver();
        List<Lifecycle> getServiceStateEvents();
        List<Boolean> getServiceUpEvents();
        List<Boolean> getProcessRunningEvents();
        boolean isFeedCalledWhenNotManaged();
    }
    
    public static class MyServiceWithFeedsImpl extends SoftwareProcessImpl implements MyServiceWithFeeds {
        protected RecordingSensorEventListener<Lifecycle> stateListener;
        protected RecordingSensorEventListener<Boolean> upListener;
        protected RecordingSensorEventListener<Boolean> processRunningListener;
        protected FunctionFeed functionFeed;
        protected boolean feedCalledWhenNotManaged;
        
        @Override
        public boolean isFeedCalledWhenNotManaged() {
            return feedCalledWhenNotManaged;
        }
        
        @Override
        public List<Lifecycle> getServiceStateEvents() {
            return getServiceStateEvents(stateListener);
        }

        @Override
        public List<Boolean> getServiceUpEvents() {
            return getServiceStateEvents(upListener);
        }
        
        @Override
        public List<Boolean> getProcessRunningEvents() {
            return getServiceStateEvents(processRunningListener);
        }

        private <T> List<T> getServiceStateEvents(RecordingSensorEventListener<T> listener) {
            if (stateListener == null) {
                return ImmutableList.of();
            } else {
                return MutableList.copyOf(listener.getEventValues()).asUnmodifiable();
            }
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
            
            subscribeToServiceState();
        }
        
        @Override
        public void rebind() {
            super.rebind();
            
            subscribeToServiceState();
        }
        
        protected void subscribeToServiceState() {
            stateListener = new RecordingSensorEventListener<Lifecycle>();
            subscriptions().subscribe(this, SERVICE_STATE_ACTUAL, stateListener);
            
            upListener = new RecordingSensorEventListener<Boolean>();
            subscriptions().subscribe(this, SERVICE_UP, upListener);
            
            processRunningListener = new RecordingSensorEventListener<Boolean>();
            subscriptions().subscribe(this, SERVICE_PROCESS_IS_RUNNING, processRunningListener);
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
            return true;
        }
    }
}
