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
package org.apache.brooklyn.entity.stock;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collection;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle.Transition;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.FailingEntity;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.QuorumCheck.QuorumChecks;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AsyncApplicationTest extends BrooklynMgmtUnitTestSupport {

    AsyncApplication app;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(AsyncApplication.class));
    }
    
    // FIXME This fails because the enricher is never triggered (there are no child events to trigger it)
    @Test(enabled=false, groups="WIP")
    public void testStartEmptyApp() throws Exception {
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }

    @Test
    public void testStartAndStopWithVanillaChild() throws Exception {
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(AsyncApplication.class)
                .configure(AsyncApplication.DESTROY_ON_STOP, false));
        TestEntity child = app.addChild(EntitySpec.create(TestEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(child, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        
        app.stop();
        
        assertTrue(Entities.isManaged(app));
        assertTrue(Entities.isManaged(child));
        assertStateEventually(child, Lifecycle.STOPPED, Lifecycle.STOPPED, false);
        assertStateEventually(app, Lifecycle.STOPPED, Lifecycle.STOPPED);
    }
    
    @Test
    public void testStopWillUnmanage() throws Exception {
        TestEntity child = app.addChild(EntitySpec.create(TestEntity.class));
        app.start(ImmutableList.of());
        app.stop();
        
        assertFalse(Entities.isManaged(app));
        assertFalse(Entities.isManaged(child));
    }
    
    @Test
    public void testStartWithVanillaFailingChild() throws Exception {
        app.addChild(EntitySpec.create(FailingEntity.class)
                .configure(FailingEntity.FAIL_ON_START, true));
        try {
            app.start(ImmutableList.of());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Simulating entity start failure for test");
        }
        
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
    }

    @Test
    public void testStartWithAsyncChild() throws Exception {
        AsyncEntity child = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        child.clearNotUpIndicators();
        child.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }
    
    @Test
    public void testStartWithAsyncFailingChild() throws Exception {
        AsyncEntity child = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        child.addNotUpIndicator("simulatedFailure", "my failure");
        child.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
    }
    
    @Test
    public void testStartWithAsyncChildren() throws Exception {
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child starts
        child1.clearNotUpIndicators();
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        // Second child starts
        child2.clearNotUpIndicators();
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }
    
    @Test
    public void testStartWithAsyncChildrenFirstChildFails() throws Exception {
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child fails
        child1.addNotUpIndicator("simulatedFailure", "my failure");
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.ON_FIRE, false);
        
        // Second child starts
        child2.clearNotUpIndicators();
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
    }

    @Test
    public void testStartWithAsyncChildrenFirstChildFailsThenRecovers() throws Exception {
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child fails
        child1.addNotUpIndicator("simulatedFailure", "my failure");
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.ON_FIRE, false);
        
        // First child recovers
        child1.clearNotUpIndicators();
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        // Second child starts
        child2.clearNotUpIndicators();
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }

    @Test
    public void testStartWithAsyncChildrenFirstChildFailsThenAfterSecondItRecovers() throws Exception {
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child fails
        child1.addNotUpIndicator("simulatedFailure", "my failure");
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.ON_FIRE, false);
        
        // Second child starts
        child2.clearNotUpIndicators();
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        
        // First child recovers
        child1.clearNotUpIndicators();
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }

    @Test
    public void testStartWithAsyncChildrenFirstChildFailsThenRecoversImmediately() throws Exception {
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child fails
        child1.addNotUpIndicator("simulatedFailure", "my failure");
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.ON_FIRE, false);
        
        // First child recovers
        child1.clearNotUpIndicators();
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        // Second child starts
        child2.clearNotUpIndicators();
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }

    @Test
    public void testStartWithAsyncChildrenLastChildFails() throws Exception {
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child starts
        child1.clearNotUpIndicators();
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        // Seconds child fails
        child2.addNotUpIndicator("simulatedFailure", "my failure");
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
    }

    @Test
    public void testStartWithQuorumOne() throws Exception {
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(AsyncApplication.class)
                .configure(AsyncApplication.RUNNING_QUORUM_CHECK, QuorumChecks.atLeastOne())
                .configure(AsyncApplication.UP_QUORUM_CHECK, QuorumChecks.atLeastOne()));
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child starts
        child1.clearNotUpIndicators();
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.RUNNING, true);
        
        // Seconds child starts
        child2.clearNotUpIndicators();
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }

    @Test
    public void testStartWithQuorumOneFirstChildFails() throws Exception {
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(AsyncApplication.class)
                .configure(AsyncApplication.RUNNING_QUORUM_CHECK, QuorumChecks.atLeastOne())
                .configure(AsyncApplication.UP_QUORUM_CHECK, QuorumChecks.atLeastOne()));
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child starts
        child1.addNotUpIndicator("simulatedFailure", "my failure");
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        // Seconds child starts
        child2.clearNotUpIndicators();
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }

    @Test
    public void testStartWithQuorumOneSecondChildFails() throws Exception {
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(AsyncApplication.class)
                .configure(AsyncApplication.RUNNING_QUORUM_CHECK, QuorumChecks.atLeastOne())
                .configure(AsyncApplication.UP_QUORUM_CHECK, QuorumChecks.atLeastOne()));
        AsyncEntity child1 = app.addChild(EntitySpec.create(AsyncEntity.class));
        AsyncEntity child2 = app.addChild(EntitySpec.create(AsyncEntity.class));
        app.start(ImmutableList.of());
        
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.STARTING, false);

        // First child starts
        child1.clearNotUpIndicators();
        child1.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(app, Lifecycle.STARTING, Lifecycle.RUNNING, true);
        
        // Seconds child starts
        child2.addNotUpIndicator("simulatedFailure", "my failure");
        child2.setExpected(Lifecycle.RUNNING);
        assertStateEventually(child1, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(child2, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(app, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }

    private void assertStateEventually(Entity entity, Lifecycle expectedState, Lifecycle state, Boolean isUp) {
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                assertState(entity, expectedState, state, isUp);
            }});
    }
    
    private void assertState(Entity entity, Lifecycle expectedState, Lifecycle state, Boolean isUp) {
        Transition actualExpectedState = entity.sensors().get(Attributes.SERVICE_STATE_EXPECTED);
        Lifecycle actualState = entity.sensors().get(Attributes.SERVICE_STATE_ACTUAL);
        Boolean actualIsUp = entity.sensors().get(Attributes.SERVICE_UP);
        String msg = "actualExpectedState="+actualExpectedState+", actualState="+actualState+", actualIsUp="+actualIsUp;
        if (expectedState != null) {
            assertEquals(actualExpectedState.getState(), expectedState, msg);
        } else {
            assertTrue(actualExpectedState == null || actualExpectedState.getState() == null, msg);
        }
        assertEquals(actualState, state, msg);
        assertEquals(actualIsUp, isUp, msg);
    }
    
    private void assertStateEventually(Entity entity, Lifecycle expectedState, Lifecycle state) {
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                assertState(entity, expectedState, state);
            }});
    }
    
    private void assertState(Entity entity, Lifecycle expectedState, Lifecycle state) {
        Transition actualExpectedState = entity.sensors().get(Attributes.SERVICE_STATE_EXPECTED);
        Lifecycle actualState = entity.sensors().get(Attributes.SERVICE_STATE_ACTUAL);
        Boolean actualIsUp = entity.sensors().get(Attributes.SERVICE_UP);
        String msg = "actualExpectedState="+actualExpectedState+", actualState="+actualState+", actualIsUp="+actualIsUp;
        if (expectedState != null) {
            assertEquals(actualExpectedState.getState(), expectedState, msg);
        } else {
            assertTrue(actualExpectedState == null || actualExpectedState.getState() == null, msg);
        }
        assertEquals(actualState, state, msg);
    }
    
    /**
     * The AsyncEntity's start leaves it in a "STARTING" state.
     * 
     * It stays like that until {@code clearNotUpIndicator(); setExpected(Lifecycle.RUNNING)} is 
     * called. It should then report "RUNNING" and service.isUp=true.
     */
    @ImplementedBy(AsyncEntityImpl.class)
    public interface AsyncEntity extends Entity, Startable {
        void setExpected(Lifecycle state);
        void addNotUpIndicator(String label, String val);
        void clearNotUpIndicators();
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
        public void clearNotUpIndicators() {
            sensors().set(Attributes.SERVICE_NOT_UP_INDICATORS, ImmutableMap.of());
        }
        
        @Override
        public void addNotUpIndicator(String label, String val) {
            ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, label, val);
        }
        
        @Override
        public void stop() {
        }

        @Override
        public void restart() {
        }
    }
}
