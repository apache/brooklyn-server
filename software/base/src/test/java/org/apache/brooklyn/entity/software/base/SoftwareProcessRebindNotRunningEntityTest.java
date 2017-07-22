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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.internal.AttributesInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.AsyncStartable;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponseGenerator;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecParams;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class SoftwareProcessRebindNotRunningEntityTest extends RebindTestFixtureWithApp {

    private static final Logger LOG = LoggerFactory.getLogger(SoftwareProcessRebindNotRunningEntityTest.class);

    private ListeningExecutorService executor;
    private LocationSpec<SshMachineLocation> machineSpec;
    private FixedListMachineProvisioningLocation<?> locationProvisioner;
    
    // We track the latches, so we can countDown() them all to unblock them. Otherwise they can
    // interfere with tearDown by blocking threads.
    // TODO Longer term, we should investigate/fix that so tearDown finishes promptly no matter what!
    private List<CountDownLatch> latches;
    
    @DataProvider
    public Object[][] terminateOrigManagementContextProvider() {
        return new Object[][]{{false}, {true}};
    }

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        latches = Lists.newCopyOnWriteArrayList();
        
        machineSpec = LocationSpec.create(SshMachineLocation.class)
                .configure("address", "1.2.3.4")
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName());
        
        locationProvisioner = app().getManagementContext().getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .configure(FixedListMachineProvisioningLocation.MACHINE_SPECS, ImmutableList.<LocationSpec<? extends MachineLocation>>of(
                        machineSpec)));

        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        
        RecordingSshTool.clear();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            for (CountDownLatch latch : latches) {
                while (latch.getCount() > 0) {
                    latch.countDown();
                }
            }
            super.tearDown();
            if (executor != null) executor.shutdownNow();
        } finally {
            RecordingSshTool.clear();
        }
    }

    @Override
    protected TestApplication createApp() {
        return mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
    }

    @Override
    protected HighAvailabilityMode getHaMode() {
        return HighAvailabilityMode.MASTER;
    }

    @Test(dataProvider="terminateOrigManagementContextProvider")
    public void testRebindWhileWaitingForCheckRunning(boolean terminateOrigManagementContext) throws Exception {
        final CountDownLatch checkRunningCalledLatch = newLatch(1);
        RecordingSshTool.setCustomResponse(".*myCheckRunning.*", new CustomResponseGenerator() {
            @Override
            public CustomResponse generate(ExecParams execParams) {
                checkRunningCalledLatch.countDown();
                return new CustomResponse(1, "", "");
            }});
        
        VanillaSoftwareProcess entity = app().createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "myLaunch")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "myCheckRunning"));
        
        startAsync(app(), ImmutableList.of(locationProvisioner));
        awaitOrFail(checkRunningCalledLatch, Asserts.DEFAULT_LONG_TIMEOUT);

        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);

        TestApplication newApp = rebind(RebindOptions.create().terminateOrigManagementContext(terminateOrigManagementContext));
        final VanillaSoftwareProcess newEntity = (VanillaSoftwareProcess) Iterables.find(newApp.getChildren(), Predicates.instanceOf(VanillaSoftwareProcess.class));

        assertMarkedAsOnfire(newEntity, Lifecycle.STARTING);
        assertMarkedAsOnfire(newApp, Lifecycle.STARTING);
    }

    @Test(dataProvider="terminateOrigManagementContextProvider")
    public void testRebindWhileLaunching(boolean terminateOrigManagementContext) throws Exception {
        final CountDownLatch launchCalledLatch = newLatch(1);
        final CountDownLatch launchBlockedLatch = newLatch(1);
        RecordingSshTool.setCustomResponse(".*myLaunch.*", new CustomResponseGenerator() {
            @Override
            public CustomResponse generate(ExecParams execParams) throws Exception {
                launchCalledLatch.countDown();
                launchBlockedLatch.await();
                return new CustomResponse(0, "", "");
            }});
        
        VanillaSoftwareProcess entity = app().createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "myLaunch")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "myCheckRunning"));
        
        startAsync(app(), ImmutableList.of(locationProvisioner));
        awaitOrFail(launchCalledLatch, Asserts.DEFAULT_LONG_TIMEOUT);

        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);

        TestApplication newApp = rebind(RebindOptions.create().terminateOrigManagementContext(terminateOrigManagementContext));
        final VanillaSoftwareProcess newEntity = (VanillaSoftwareProcess) Iterables.find(newApp.getChildren(), Predicates.instanceOf(VanillaSoftwareProcess.class));

        assertMarkedAsOnfire(newEntity, Lifecycle.STARTING);
        assertMarkedAsOnfire(newApp, Lifecycle.STARTING);
    }

    @Test(dataProvider="terminateOrigManagementContextProvider")
    public void testRebindWhileStoppingProcess(boolean terminateOrigManagementContext) throws Exception {
        final CountDownLatch stopCalledLatch = newLatch(1);
        final CountDownLatch stopBlockedLatch = newLatch(1);
        RecordingSshTool.setCustomResponse(".*myStop.*", new CustomResponseGenerator() {
            @Override
            public CustomResponse generate(ExecParams execParams) throws Exception {
                stopCalledLatch.countDown();
                stopBlockedLatch.await();
                return new CustomResponse(0, "", "");
            }});
        
        VanillaSoftwareProcess entity = app().createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "myLaunch")
                .configure(VanillaSoftwareProcess.STOP_COMMAND, "myStop")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "myCheckRunning"));
        app().start(ImmutableList.of(locationProvisioner));
        
        stopAsync(entity);
        awaitOrFail(stopCalledLatch, Asserts.DEFAULT_LONG_TIMEOUT);

        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);

        TestApplication newApp = rebind(RebindOptions.create().terminateOrigManagementContext(terminateOrigManagementContext));
        final VanillaSoftwareProcess newEntity = (VanillaSoftwareProcess) Iterables.find(newApp.getChildren(), Predicates.instanceOf(VanillaSoftwareProcess.class));

        assertMarkedAsOnfire(newEntity, Lifecycle.STOPPING);
    }

    @Test
    public void testRebindWhileProvisioning() throws Exception {
        testRebindWhileProvisioning(true);
    }
    
    @Test(dataProvider="terminateOrigManagementContextProvider")
    public void testRebindWhileProvisioning(boolean terminateOrigManagementContext) throws Exception {
        final CountDownLatch obtainCalledLatch = newLatch(1);
        final CountDownLatch obtainBlockedLatch = newLatch(1);
        MyProvisioningLocation blockingProvisioner = mgmt().getLocationManager().createLocation(LocationSpec.create(MyProvisioningLocation.class)
                .configure(MyProvisioningLocation.OBTAIN_CALLED_LATCH, obtainCalledLatch)
                .configure(MyProvisioningLocation.OBTAIN_BLOCKED_LATCH, obtainBlockedLatch)
                .configure(MyProvisioningLocation.MACHINE_SPEC, machineSpec));
        
        VanillaSoftwareProcess entity = app().createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "myLaunch")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "myCheckRunning"));
        
        startAsync(app(), ImmutableList.of(blockingProvisioner));
        awaitOrFail(obtainCalledLatch, Asserts.DEFAULT_LONG_TIMEOUT);

        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);
        EntityAsserts.assertAttributeEquals(entity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, AttributesInternal.ProvisioningTaskState.RUNNING);

        TestApplication newApp = rebind(RebindOptions.create().terminateOrigManagementContext(terminateOrigManagementContext));
        final VanillaSoftwareProcess newEntity = (VanillaSoftwareProcess) Iterables.find(newApp.getChildren(), Predicates.instanceOf(VanillaSoftwareProcess.class));

        assertMarkedAsOnfire(newEntity, Lifecycle.STARTING);
        assertMarkedAsOnfire(newApp, Lifecycle.STARTING);
        assertMarkedAsVmLost(newEntity, Lifecycle.STARTING);

        // Expect the marker to have been cleared on rebind (sensible because task is not running).
        EntityAsserts.assertAttributeEquals(newEntity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, null);
        EntityAsserts.assertAttributeEquals(newEntity, AttributesInternal.INTERNAL_TERMINATION_TASK_STATE, null);
    }

    @Test(dataProvider="terminateOrigManagementContextProvider")
    public void testRebindWhileTerminatingVm(boolean terminateOrigManagementContext) throws Exception {
        final CountDownLatch releaseCalledLatch = newLatch(1);
        final CountDownLatch obtainBlockedLatch = newLatch(1);
        MyProvisioningLocation blockingProvisioner = mgmt().getLocationManager().createLocation(LocationSpec.create(MyProvisioningLocation.class)
                .configure(MyProvisioningLocation.RELEASE_CALLED_LATCH, releaseCalledLatch)
                .configure(MyProvisioningLocation.RELEASE_BLOCKED_LATCH, obtainBlockedLatch)
                .configure(MyProvisioningLocation.MACHINE_SPEC, machineSpec));
        
        VanillaSoftwareProcess entity = app().createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "myLaunch")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "myCheckRunning"));
        
        app().start(ImmutableList.of(blockingProvisioner));
        
        stopAsync(entity);
        awaitOrFail(releaseCalledLatch, Asserts.DEFAULT_LONG_TIMEOUT);

        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);
        EntityAsserts.assertAttributeEquals(entity, AttributesInternal.INTERNAL_TERMINATION_TASK_STATE, AttributesInternal.ProvisioningTaskState.RUNNING);

        TestApplication newApp = rebind(RebindOptions.create().terminateOrigManagementContext(terminateOrigManagementContext));
        final VanillaSoftwareProcess newEntity = (VanillaSoftwareProcess) Iterables.find(newApp.getChildren(), Predicates.instanceOf(VanillaSoftwareProcess.class));
        
        
        assertMarkedAsOnfire(newEntity, Lifecycle.STOPPING);
        assertMarkedAsVmLost(newEntity, Lifecycle.STOPPING);

        // Expect the marker to have been cleared on rebind (sensible because task is not running).
        EntityAsserts.assertAttributeEquals(newEntity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, null);
        EntityAsserts.assertAttributeEquals(newEntity, AttributesInternal.INTERNAL_TERMINATION_TASK_STATE, null);
    }

    @Test
    public void testLaunchHotStandbyWhileEntityStarting() throws Exception {
        final CountDownLatch launchCalledLatch = newLatch(1);
        final CountDownLatch launchBlockedLatch = newLatch(1);
        RecordingSshTool.setCustomResponse(".*myLaunch.*", new CustomResponseGenerator() {
            @Override
            public CustomResponse generate(ExecParams execParams) throws Exception {
                launchCalledLatch.countDown();
                launchBlockedLatch.await();
                return new CustomResponse(0, "", "");
            }});
        
        VanillaSoftwareProcess entity = app().createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "myLaunch")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "myCheckRunning"));
        
        startAsync(app(), ImmutableList.of(locationProvisioner));
        awaitOrFail(launchCalledLatch, Asserts.DEFAULT_LONG_TIMEOUT);

        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);

        // Check that the read-only hot standby does not overwrite the entity's state; it should still say "STARTING"
        TestApplication newApp = hotStandby();
        final VanillaSoftwareProcess newEntity = (VanillaSoftwareProcess) Iterables.find(newApp.getChildren(), Predicates.instanceOf(VanillaSoftwareProcess.class));

        assertNotMarkedOnfire(newEntity, Lifecycle.STARTING);
        assertNotMarkedOnfire(newApp, Lifecycle.STARTING);
    }
    
    @Test
    public void testRebindAsyncStartableWhileStarting() throws Exception {
        AsyncEntity entity = app().createAndManageChild(EntitySpec.create(AsyncEntity.class));
        
        app().start(ImmutableList.of(locationProvisioner));

        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);
        EntityAsserts.assertAttributeEqualsEventually(app(), Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertEquals(entity.sensors().get(Attributes.SERVICE_STATE_EXPECTED).getState(), Lifecycle.STARTING);

        TestApplication newApp = rebind();
        final AsyncEntity newEntity = (AsyncEntity) Iterables.find(newApp.getChildren(), Predicates.instanceOf(AsyncEntity.class));

        assertNotMarkedOnfire(newEntity, Lifecycle.STARTING);
        assertNotMarkedOnfire(newApp, Lifecycle.RUNNING);

        // Set the async entity to completed; expect it to correctly transition (even after rebind) 
        newEntity.clearNotUpIndicator();
        newEntity.setExpected(Lifecycle.RUNNING);
        
        EntityAsserts.assertAttributeEqualsEventually(newEntity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        EntityAsserts.assertAttributeEqualsEventually(newEntity, Attributes.SERVICE_UP, true);
    }

    protected ListenableFuture<Void> startAsync(final Startable entity, final Collection<? extends Location> locs) {
        return executor.submit(new Callable<Void>() {
            @Override public Void call() throws Exception {
                entity.start(locs);
                return null;
            }});
    }

    protected ListenableFuture<Void> stopAsync(final Startable entity) {
        return executor.submit(new Callable<Void>() {
            @Override public Void call() throws Exception {
                entity.stop();
                return null;
            }});
    }

    protected void awaitOrFail(CountDownLatch latch, Duration timeout) throws Exception {
        boolean success = latch.await(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertTrue(success, "latch "+latch+" not satisfied in "+timeout);
    }
    
    protected CountDownLatch newLatch(int count) {
        CountDownLatch result = new CountDownLatch(count);
        latches.add(result);
        return result;
    }

    protected void assertMarkedAsOnfire(Entity entity, Lifecycle previousState) throws Exception {
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, false);
        assertNotUpIndicatorIncludesEventually(entity, "Task aborted on rebind",
                "Set to on-fire (from previous expected state "+previousState+") because tasks aborted on shutdown");
    }

    protected void assertMarkedAsVmLost(Entity entity, Lifecycle previousState) throws Exception {
        String expectedReason = "VM " + (previousState == Lifecycle.STARTING ? "provisioning" : "termination") 
                + " may have been in-progress and now lost, because tasks aborted on shutdown";
        assertNotUpIndicatorIncludesEventually(entity, "VM may be lost on rebind", expectedReason);
    }

    protected void assertNotUpIndicatorIncludesEventually(final Entity entity, final String expectedKey, final String expectedVal) throws Exception {
        EntityAsserts.assertAttributeEventually(entity, Attributes.SERVICE_NOT_UP_INDICATORS, new Predicate<Map<?,?>>() {
            @Override
            public boolean apply(Map<?, ?> input) {
                if (input == null) return false;
                for (Map.Entry<?, ?> entry : input.entrySet()) {
                    boolean keyMatches = expectedKey.equals(entry.getKey());
                    boolean valueMatches = expectedVal.equals(entry.getValue());
                    if (keyMatches && valueMatches) return true;
                }
                LOG.info("entity "+entity+" not-up-indicators: "+input);
                return false;
            }});
    }

    protected void assertNotMarkedOnfire(final Entity entity, final Lifecycle expectedState) throws Exception {
        assertEquals(entity.getAttribute(Attributes.SERVICE_STATE_ACTUAL), expectedState);
        Map<String, Object> indicators = entity.getAttribute(Attributes.SERVICE_NOT_UP_INDICATORS);
        assertFalse(indicators.keySet().contains("Task aborted on rebind"), "indicators="+indicators);
    }

    public static class MyProvisioningLocation extends AbstractLocation implements MachineProvisioningLocation<SshMachineLocation> {
        public static final ConfigKey<CountDownLatch> OBTAIN_CALLED_LATCH = ConfigKeys.newConfigKey(CountDownLatch.class, "obtainCalledLatch");
        public static final ConfigKey<CountDownLatch> OBTAIN_BLOCKED_LATCH = ConfigKeys.newConfigKey(CountDownLatch.class, "obtainBlockedLatch");
        public static final ConfigKey<CountDownLatch> RELEASE_CALLED_LATCH = ConfigKeys.newConfigKey(CountDownLatch.class, "releaseCalledLatch");
        public static final ConfigKey<CountDownLatch> RELEASE_BLOCKED_LATCH = ConfigKeys.newConfigKey(CountDownLatch.class, "releaseBlockedLatch");
        
        @SuppressWarnings("serial")
        public static final ConfigKey<LocationSpec<SshMachineLocation>> MACHINE_SPEC = ConfigKeys.newConfigKey(
                new TypeToken<LocationSpec<SshMachineLocation>>() {},
                "machineSpec");

        protected List<CallInfo> callHistory = Collections.synchronizedList(Lists.<CallInfo>newArrayList());
        
        @Override
        public MachineProvisioningLocation<SshMachineLocation> newSubLocation(Map<?, ?> newFlags) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public SshMachineLocation obtain(Map<?,?> flags) throws NoMachinesAvailableException {
            callHistory.add(new CallInfo("obtain", ImmutableList.of(flags)));
            
            CountDownLatch calledLatch = config().get(OBTAIN_CALLED_LATCH);
            CountDownLatch blockedLatch = config().get(OBTAIN_BLOCKED_LATCH);
            LocationSpec<SshMachineLocation> machineSpec = config().get(MACHINE_SPEC);
            
            if (calledLatch != null) calledLatch.countDown();
            try {
                if (blockedLatch != null) blockedLatch.await();
            } catch (InterruptedException e) {
                throw Exceptions.propagate(e);
            }
            return getManagementContext().getLocationManager().createLocation(machineSpec);
        }

        @Override
        public void release(SshMachineLocation machine) {
            callHistory.add(new CallInfo("release", ImmutableList.of(machine)));
            
            CountDownLatch calledLatch = config().get(RELEASE_CALLED_LATCH);
            CountDownLatch blockedLatch = config().get(RELEASE_BLOCKED_LATCH);
            
            if (calledLatch != null) calledLatch.countDown();
            try {
                if (blockedLatch != null) blockedLatch.await();
            } catch (InterruptedException e) {
                throw Exceptions.propagate(e);
            }
        }

        @Override
        public Map getProvisioningFlags(Collection<String> tags) {
            return Collections.emptyMap();
        }
        
        public List<CallInfo> getCallHistory() {
            synchronized (callHistory) {
                return ImmutableList.copyOf(callHistory);
            }
        }
        
        public CallInfo getLastCall() {
            synchronized (callHistory) {
                return callHistory.get(callHistory.size()-1);
            }
        }
        
        static class CallInfo {
            public final String name;
            public final List<? extends Object> args;
            
            public CallInfo(String name, List<? extends Object> args) {
                this.name = name;
                this.args = args;
            }
        }
    }
    
    /**
     * The AsyncEntity's start leaves it in a "STARTING" state.
     * 
     * It stays like that until {@code clearNotUpIndicator(); setExpected(Lifecycle.RUNNING)} is 
     * called. It should then report "RUNNING" and service.isUp=true.
     */
    @ImplementedBy(AsyncEntityImpl.class)
    public interface AsyncEntity extends Entity, AsyncStartable {
        void setExpected(Lifecycle state);
        void clearNotUpIndicator();
    }
    
    public static class AsyncEntityImpl extends AbstractEntity implements AsyncEntity {

        @Override
        public void start(Collection<? extends Location> locations) {
            ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
            ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, START.getName(), "starting");
        }


        @Override
        public void setExpected(Lifecycle state) {
            ServiceStateLogic.setExpectedState(this, checkNotNull(state, "state"));
        }

        @Override
        public void clearNotUpIndicator() {
            ServiceStateLogic.ServiceNotUpLogic.clearNotUpIndicator(this, START.getName());
        }
        
        @Override
        public void stop() {
        }

        @Override
        public void restart() {
        }
    }
}
