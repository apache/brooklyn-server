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
package org.apache.brooklyn.core.entity;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.FailingEntity;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestApplicationImpl;
import org.apache.brooklyn.core.test.entity.TestApplicationNoEnrichersImpl;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityNoEnrichersImpl;
import org.apache.brooklyn.enricher.stock.AbstractMultipleSensorAggregator;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Test
public class ApplicationLifecycleStateTest extends BrooklynMgmtUnitTestSupport {
    private static final Logger log = LoggerFactory.getLogger(ApplicationLifecycleStateTest.class);

    public void testHappyPathEmptyApp() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);
    }
    
    public void testHappyPathWithChild() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class)));
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);
    }
    
    public void testOnlyChildFailsToStartCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        FailingEntity child = (FailingEntity) Iterables.get(app.getChildren(), 0);
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(child, Lifecycle.ON_FIRE, false);
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }
    
    public static class TestApplicationDoStartFailing extends TestApplicationImpl {
        @Override
        protected void doStart(Collection<? extends Location> locations) {
            super.doStart(locations);
            throw new RuntimeException("deliberate failure");
        }
    }
    public void testAppFailsCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class,
                TestApplicationDoStartFailing.class));
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }
    
    public void testSomeChildFailsOnStartCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }
    
    public void testOnlyChildFailsToStartThenRecoversCausesAppToRecover() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        FailingEntity child = (FailingEntity) Iterables.get(app.getChildren(), 0);
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
        
        child.sensors().set(Attributes.SERVICE_UP, true);
        child.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertUpAndRunningEventually(app);
    }
    
    public void testSomeChildFailsToStartThenRecoversCausesAppToRecover() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        FailingEntity child = (FailingEntity) Iterables.find(app.getChildren(), Predicates.instanceOf(FailingEntity.class));
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
        
        child.sensors().set(Attributes.SERVICE_UP, true);
        child.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertUpAndRunningEventually(app);
    }
    
    public void testStartsThenOnlyChildFailsCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class)));
        TestEntity child = (TestEntity) Iterables.get(app.getChildren(), 0);
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);

        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(child, "myIndicator", "Simulate not-up of child");
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }

    public void testStartsThenSomeChildFailsCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(TestEntity.class)));
        TestEntity child = (TestEntity) Iterables.get(app.getChildren(), 0);
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);

        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(child, "myIndicator", "Simulate not-up of child");
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }

    @Test
    public void testChildFailuresOnStartButWithQuorumCausesAppToSucceed() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .configure(StartableApplication.UP_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .configure(StartableApplication.RUNNING_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);
    }

    @Test
    public void testStartsThenChildFailsButWithQuorumCausesAppToSucceed() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .configure(StartableApplication.UP_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .configure(StartableApplication.RUNNING_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(TestEntity.class)));

        TestEntity child = (TestEntity) Iterables.get(app.getChildren(), 0);
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);
        
        for (Entity childr : app.getChildren()) {
            EntityAsserts.assertAttributeEquals(childr, TestEntity.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        }

        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(child, "myIndicator", "Simulate not-up of child");
        assertHealthContinually(app, Lifecycle.RUNNING, true);
        mgmt.getEntityManager().unmanage(app);
    }

    @Test
    public void testStartsThenChildFailsButWithQuorumCausesAppToStayHealthy() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .configure(StartableApplication.UP_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .configure(StartableApplication.RUNNING_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(TestEntity.class)));
        TestEntity child = (TestEntity) Iterables.get(app.getChildren(), 0);
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);

        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(child, "myIndicator", "Simulate not-up of child");
        assertUpAndRunningEventually(app);
    }

    /**
     * Tests concurrent modifications to a sensor, asserting that the last notification the subscribers 
     * receives equals the last value that sensor has.
     * 
     * Prior to this being fixed (see https://github.com/apache/brooklyn-server/pull/622), it caused 
     * problems in ComputeServiceIndicatorsFromChildrenAndMembers: it saw a child transition 
     * from "running" to "starting", and thus emitted the on-fire event for the parent entity. As asserted
     * by this test, the enricher should now always receive the events in the correct order (e.g. "starting",
     * "running").
     */
    @Test
    public void testSettingSensorFromThreads() {
        final TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        final AttributeSensor<String> TEST_SENSOR = Sensors.newStringSensor("test.sensor");

        final AtomicReference<String> lastSeenState = new AtomicReference<>();
        app.subscriptions().subscribe(app, TEST_SENSOR, new SensorEventListener<String>() {
            @Override
            public void onEvent(SensorEvent<String> event) {
                lastSeenState.set(event.getValue());
                log.debug("seen event=" + event);
            }
        });

        Task<?> first = mgmt.getExecutionManager().submit("setting test sensor", () -> {
                app.sensors().set(TEST_SENSOR, "first");
                log.debug("set first");
            });
        Task<?> second = mgmt.getExecutionManager().submit("setting test sensor", () -> {
                app.sensors().set(TEST_SENSOR, "second");
                log.debug("set second");
            });
        first.blockUntilEnded();
        second.blockUntilEnded();

        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                EntityAsserts.assertAttributeEquals(app, TEST_SENSOR, lastSeenState.get());
            }
        });
    }

    public static class RecodingChildSensorEnricher extends AbstractMultipleSensorAggregator<Void> {
        public static AttributeSensor<String> RECORDED_SENSOR = Sensors.newStringSensor("recorded.sensor");
        List<String> seenValuesAllCollected = Collections.synchronizedList(new ArrayList<>());
        List<String> seenValuesPublished = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected Collection<Sensor<?>> getSourceSensors() {
            return ImmutableList.<Sensor<?>>of(RECORDED_SENSOR);
        }

        @Override
        protected Object compute() {
            throw new UnsupportedOperationException("Not expected to be called since onUpdated is overriden");
        }

        @Override
        protected void setEntityLoadingTargetConfig() {
        }

        @Override
        public void onEvent(SensorEvent<Object> event) {
            super.onEvent(event);
            
            if (event.getSensor().equals(RECORDED_SENSOR)) {
                seenValuesPublished.add((String)event.getValue());
            }
        }
        
        @Override
        protected void onUpdated() {
            // called in onEvent, this is longwinded way to add the event.value we are interested in
            Collection<String> vv = getValues(RECORDED_SENSOR).values();
            if (!vv.isEmpty()) {
                seenValuesAllCollected.add(vv.iterator().next());
            }
        }
    }

    @Test
    public void testSensorInitAndPublishOrderNonStrict() {
        // this mirrors how most code is working - the initial values seen for an added child may be later than other published values
        doTestSensorInitAndPublishOrder(false);
    }
    
    @Test
    public void testSensorInitAndPublishOrder() {
        // this enforces canonical order by waiting on expected events
        doTestSensorInitAndPublishOrder(true);
    }
    
    private void doTestSensorInitAndPublishOrder(boolean strict) {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .impl(TestApplicationNoEnrichersImpl.class)
                .enricher(EnricherSpec.create(RecodingChildSensorEnricher.class))
                .child(EntitySpec.create(TestEntity.class)
                        .impl(TestEntityNoEnrichersImpl.class)));
        
        final RecodingChildSensorEnricher enricher = getFirstEnricher(app, RecodingChildSensorEnricher.class);
        if (strict) {
            Asserts.eventually(Suppliers.ofInstance(enricher.seenValuesAllCollected), CollectionFunctionals.sizeEquals(2));
        }

        Entity child = Iterables.get(app.getChildren(), 0);
        child.sensors().set(RecodingChildSensorEnricher.RECORDED_SENSOR, "first");
        child.sensors().set(RecodingChildSensorEnricher.RECORDED_SENSOR, "second");
        
        // usually:
        //   setEntity -> onUpdate (sees current value)
        //   CHILD_ADDED -> onUpdate (sees current value)
        //   set RECORDED_SENSOR=first -> onUpdate (sees event)
        //   set RECORDED_SENSOR=second -> onUpdate (sees event)
        
        Asserts.eventually(Suppliers.ofInstance(enricher.seenValuesAllCollected), CollectionFunctionals.sizeEquals(4));
        
        Assert.assertEquals(enricher.seenValuesPublished, MutableList.of("first", "second"),
            "Unexpected ordering for vP " + enricher.seenValuesPublished + " (vC "+enricher.seenValuesAllCollected+")");
        
        List<String> vC = MutableList.copyOf(enricher.seenValuesAllCollected); 
        if (!strict) {
            // CHILD_ADDED (and other handlers) are running in parallel to this thread, however,
            // their order is guaranteed to be the above, but it is in parallel to this main thread, thus the current value
            // it takes can be any of null, first, or second, unless we run in strict mode
            vC.set(1, null);
        }
        Assert.assertEquals(vC, MutableList.of(null, null, "first", "second"),
            "Unexpected ordering for vC " + enricher.seenValuesAllCollected + " (vP "+enricher.seenValuesPublished+")");
    }

    public static class EmittingEnricher extends AbstractEnricher {
        @Override
        public void setEntity(@SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity) {
            super.setEntity(entity);
            this.suppressDuplicates = true;
        }

        @Override
        public <T> void emit(Sensor<T> sensor, Object val) {
            super.emit(sensor, val);
        }
    }

    /**
     * The deduplication logic in AbstractEnricher previously did not work for parallel invocations.
     * It used to do a get and then a compare, so another thread could change the value between
     * those two operations.
     */
    @Test
    public void testAbstractEnricherDeduplicationBroken() {
        final TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .enricher(EnricherSpec.create(EmittingEnricher.class)));
        final AttributeSensor<String> TEST_SENSOR = Sensors.newStringSensor("test.sensor");

        final List<String> seenValues = Collections.synchronizedList(new ArrayList<String>());
        app.subscriptions().subscribe(app, TEST_SENSOR, new SensorEventListener<String>() {
            @Override
            public void onEvent(SensorEvent<String> event) {
                seenValues.add(event.getValue());
            }
        });

        app.sensors().set(TEST_SENSOR, "initial");

        final EmittingEnricher enricher = getFirstEnricher(app, EmittingEnricher.class);
        Runnable overrideJob = new Runnable() {
            @Override
            public void run() {
                enricher.emit(TEST_SENSOR, "override");
            }
        };

        // Simulates firing the emit method from event handlers in different threads
        mgmt.getExecutionManager().submit("emitting test sensor", overrideJob);
        mgmt.getExecutionManager().submit("emitting test sensor", overrideJob);

        Asserts.eventually(Suppliers.ofInstance(seenValues), CollectionFunctionals.sizeEquals(2));
        Asserts.succeedsContinually(new Runnable() {
            @Override
            public void run() {
                assertEquals(seenValues, ImmutableList.of("initial", "override"));
            }
        });
    }

    private void assertHealthEventually(Entity entity, Lifecycle expectedState, Boolean expectedUp) {
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, expectedState);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, expectedUp);
    }
    
    private void assertHealthContinually(Entity entity, Lifecycle expectedState, Boolean expectedUp) {
        // short wait, so unit tests don't take ages
        Map<String, ?> flags = ImmutableMap.of("timeout", ValueResolver.REAL_QUICK_WAIT);
        EntityAsserts.assertAttributeEqualsContinually(flags, entity, Attributes.SERVICE_STATE_ACTUAL, expectedState);
        EntityAsserts.assertAttributeEqualsContinually(flags, entity, Attributes.SERVICE_UP, expectedUp);
    }
    
    private void assertUpAndRunningEventually(Entity entity) {
        try {
            EntityAsserts.assertAttributeEventually(entity, Attributes.SERVICE_NOT_UP_INDICATORS, CollectionFunctionals.<String>mapEmptyOrNull());
            EntityAsserts.assertAttributeEventually(entity, ServiceStateLogic.SERVICE_PROBLEMS, CollectionFunctionals.<String>mapEmptyOrNull());
            EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
            EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
        } catch (Throwable t) {
            Dumper.dumpInfo(entity);
            String err = "(Dumped entity info - see log); entity=" + entity + "; " + 
                    "state=" + entity.sensors().get(Attributes.SERVICE_STATE_ACTUAL) + "; " + 
                    "up="+entity.sensors().get(Attributes.SERVICE_UP) + "; " +
                    "notUpIndicators="+entity.sensors().get(Attributes.SERVICE_NOT_UP_INDICATORS) + "; " +
                    "serviceProblems="+entity.sensors().get(Attributes.SERVICE_PROBLEMS);
            throw new AssertionError(err, t);
        }
    }
    
    private void startAndAssertException(TestApplication app, Collection<? extends Location> locs) {
        try {
            app.start(locs);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Error invoking start");
        }
    }

    protected <T> T getFirstEnricher(TestApplication app, Class<T> type) {
        return FluentIterable.from(app.enrichers())
            .filter(type)
            .first()
            .get();
    }

    // for running the tests below many times and stopping when they fail;
    // they seem happy (and this breaks the subclass @Test class annotation)
    // but kept as comment just in case
//    public static void main(String[] args) throws Exception {
//        int i = 0;
//        while (true) {
//            log.info("test "+(++i));
//            ApplicationLifecycleStateTest l = new ApplicationLifecycleStateTest();
//            l.setUp();
//            l.testSensorInitAndPublishOrder();
//            l.testSensorInitAndPublishOrderNonStrict();
//            l.tearDown();
//        }
//    }
}
