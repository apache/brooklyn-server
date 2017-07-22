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
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle.Transition;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.entity.trait.StartableMethods;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.group.AbstractGroupImpl;
import org.apache.brooklyn.entity.group.Cluster;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Tests a pattern that a user has, for async entities.
 * 
 * Calling start() triggers some asynchronous work. Completion of that work is reported via a
 * callback that indicates success or fail.
 */
public class EntityAsyncTest extends BrooklynAppUnitTestSupport {

    // TODO If the cluster has quorum=all, should the cluster report ON_FIRE as soon as any of the
    // children report a failure to start, or should it keep saying "STARTING" until all callbacks
    // are received?
    
    private static final Logger LOG = LoggerFactory.getLogger(EntityAsyncTest.class);

    @Test
    public void testEntityStartsAsynchronously() throws Exception {
        AsyncEntity entity = app.addChild(EntitySpec.create(AsyncEntity.class));
        
        app.start(ImmutableList.of());
        assertStateEventually(entity, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        entity.onCallback(true);
        assertStateEventually(entity, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }
    
    @Test
    public void testClusterStartsAsynchronously() throws Exception {
        AsyncCluster cluster = app.addChild(EntitySpec.create(AsyncCluster.class)
                .configure(AsyncCluster.INITIAL_SIZE, 2));
        
        app.start(ImmutableList.of());
        List<AsyncEntity> children = cast(cluster.getChildren(), AsyncEntity.class);
        
        // Everything should say "starting"
        assertStateEventually(cluster, Lifecycle.STARTING, Lifecycle.STARTING, false);
        for (AsyncEntity child : children) {
            assertStateEventually(child, Lifecycle.STARTING, Lifecycle.STARTING, false);
        }

        // Indicate that first child is now running successfully
        cluster.onCallback(children.get(0).getId(), true);
        
        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(children.get(1), Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateContinually(cluster, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        // Indicate that second child is now running successfully
        cluster.onCallback(children.get(1).getId(), true);
        
        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(children.get(1), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(cluster, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }
    
    @Test
    public void testClusterFirstChildFails() throws Exception {
        AsyncCluster cluster = app.addChild(EntitySpec.create(AsyncCluster.class)
                .configure(AsyncCluster.INITIAL_SIZE, 2));
        
        app.start(ImmutableList.of());
        List<AsyncEntity> children = cast(cluster.getChildren(), AsyncEntity.class);
        
        // Everything should say "starting"
        assertStateEventually(cluster, Lifecycle.STARTING, Lifecycle.STARTING, false);
        for (AsyncEntity child : children) {
            assertStateEventually(child, Lifecycle.STARTING, Lifecycle.STARTING, false);
        }

        // Indicate that first child failed
        cluster.onCallback(children.get(0).getId(), false);
        
        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(children.get(1), Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateEventually(cluster, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        // Indicate that second child is now running successfully
        cluster.onCallback(children.get(1).getId(), true);
        
        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(children.get(1), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(cluster, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
    }
    
    @Test
    public void testClusterSecondChildFails() throws Exception {
        AsyncCluster cluster = app.addChild(EntitySpec.create(AsyncCluster.class)
                .configure(AsyncCluster.INITIAL_SIZE, 2));
        
        app.start(ImmutableList.of());
        List<AsyncEntity> children = cast(cluster.getChildren(), AsyncEntity.class);
        
        // Everything should say "starting"
        assertStateEventually(cluster, Lifecycle.STARTING, Lifecycle.STARTING, false);
        for (AsyncEntity child : children) {
            assertStateEventually(child, Lifecycle.STARTING, Lifecycle.STARTING, false);
        }

        // Indicate that first child is now running successfully
        cluster.onCallback(children.get(0).getId(), true);
        
        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(children.get(1), Lifecycle.STARTING, Lifecycle.STARTING, false);
        assertStateContinually(cluster, Lifecycle.STARTING, Lifecycle.STARTING, false);
        
        // Indicate that second child failed
        cluster.onCallback(children.get(1).getId(), false);
        
        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(children.get(1), Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(cluster, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
    }
    
    @Test
    public void testClusterStartsThenChildFails() throws Exception {
        AsyncCluster cluster = app.addChild(EntitySpec.create(AsyncCluster.class)
                .configure(AsyncCluster.INITIAL_SIZE, 2));
        
        app.start(ImmutableList.of());
        List<AsyncEntity> children = cast(cluster.getChildren(), AsyncEntity.class);
        
        // Everything should say "starting"
        assertStateEventually(cluster, Lifecycle.STARTING, Lifecycle.STARTING, false);
        for (AsyncEntity child : children) {
            assertStateEventually(child, Lifecycle.STARTING, Lifecycle.STARTING, false);
        }

        // Indicate that children are now running successfully
        cluster.onCallback(children.get(0).getId(), true);
        cluster.onCallback(children.get(1).getId(), true);
        
        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(children.get(1), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(cluster, Lifecycle.RUNNING, Lifecycle.RUNNING, true);

        // First child then fails
        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(children.get(0), "myKey", "simulate failure");

        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        assertStateEventually(children.get(1), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(cluster, Lifecycle.RUNNING, Lifecycle.ON_FIRE, false);
        
        // First child then recovers
        ServiceStateLogic.ServiceNotUpLogic.clearNotUpIndicator(children.get(0), "myKey");
        
        assertStateEventually(children.get(0), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(children.get(1), Lifecycle.RUNNING, Lifecycle.RUNNING, true);
        assertStateEventually(cluster, Lifecycle.RUNNING, Lifecycle.RUNNING, true);
    }
    
    private void assertStateContinually(final Entity entity, final Lifecycle expectedState, final Lifecycle state, final Boolean isUp) {
        Asserts.succeedsContinually(ImmutableMap.of("timeout", Duration.millis(50)), new Runnable() {
            @Override public void run() {
                assertState(entity, expectedState, state, isUp);
            }});
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
    
    private <T> List<T> cast(Iterable<?> vals, Class<T> clazz) {
        List<T> result = Lists.newArrayList();
        for (Object val : vals) {
            result.add(clazz.cast(val));
        }
        return result;
    }

    /**
     * The AsyncEntity's start leaves it in a "STARTING" state.
     * 
     * It stays like that until {@code onCallback(true)} is called. It should then report 
     * expected="RUNNING", service.state="RUNNING" and service.isUp=true. Alternatively, 
     * if {@code onCallback(true)} is called, it should then report expected="RUNNING", 
     * service.state="ON_FIRE" and service.isUp=false.
     */
    @ImplementedBy(AsyncEntityImpl.class)
    public interface AsyncEntity extends Entity, Startable {
        void onCallback(boolean success);
    }
    
    public static class AsyncEntityImpl extends AbstractEntity implements AsyncEntity {

        @Override
        public void start(Collection<? extends Location> locations) {
            ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
            ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, START.getName(), "starting");
        }


        @Override
        public void onCallback(boolean success) {
            if (success) {
                ServiceStateLogic.ServiceNotUpLogic.clearNotUpIndicator(this, START.getName());
            } else {
                ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, START.getName(), "callback reported failure");
            }
            
            Transition expectedState = sensors().get(Attributes.SERVICE_STATE_EXPECTED);
            if (expectedState != null && expectedState.getState() == Lifecycle.STARTING) {
                ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
            }
        }

        @Override
        public void stop() {
        }

        @Override
        public void restart() {
        }
    }
    
    /**
     * The AsyncCluster's start leaves it in a "STARTING" state, having created the children
     * and called start() on each.
     * 
     * It expects an explicit call to {@code clearNotUpIndicator(); setExpected(Lifecycle.RUNNING)},
     * after which its service.isUp will be inferred from its children (they must all be running).
     */
    @ImplementedBy(AsyncClusterImpl.class)
    public interface AsyncCluster extends Cluster, Startable {
        void onCallback(String childId, boolean success);
    }
    
    public static class AsyncClusterImpl extends AbstractGroupImpl implements AsyncCluster {

        @Override
        protected void initEnrichers() {
            super.initEnrichers();
            
            // all children must be up, for ourselves to be up
            enrichers().add(ServiceStateLogic.newEnricherFromChildrenUp()
                    .checkChildrenOnly()
                    .requireUpChildren(QuorumCheck.QuorumChecks.all()));
        }

        @Override
        public void start(Collection<? extends Location> locations) {
            ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
            ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, START.getName(), "starting");
            
            try {
                for (int i = 0; i < config().get(INITIAL_SIZE); i++) {
                    addChild(EntitySpec.create(AsyncEntity.class));
                }
                StartableMethods.start(this, locations);
                
            } catch (Throwable t) {
                ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(this, START.getName(), Exceptions.collapseText(t));
                ServiceStateLogic.setExpectedState(this, Lifecycle.ON_FIRE);
                throw Exceptions.propagate(t);
            }
        }

        @Override
        public void onCallback(String childId, boolean success) {
            Optional<Entity> child = Iterables.tryFind(getChildren(), EntityPredicates.idEqualTo(childId));
            if (child.isPresent()) {
                ((AsyncEntity)child.get()).onCallback(success);
            } else {
                LOG.warn("Child not found with resourceId '"+childId+"'; not injecting state from callback");
            }
            
            Optional<Entity> unstartedVm = Iterables.tryFind(getChildren(), EntityPredicates.attributeSatisfies(Attributes.SERVICE_STATE_EXPECTED, 
                    new Predicate<Lifecycle.Transition>() {
                        @Override public boolean apply(Transition input) {
                            return input == null || input.getState() == Lifecycle.STARTING;
                        }}));
            
            if (!unstartedVm.isPresent()) {
                // No VMs are still starting; we are finished starting
                ServiceStateLogic.ServiceNotUpLogic.clearNotUpIndicator(this, START.getName());
                ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
            }
        }
        
        @Override
        public void stop() {
        }

        @Override
        public void restart() {
        }

        @Override
        public Integer resize(Integer desiredSize) {
            throw new UnsupportedOperationException();
        }
    }
}
