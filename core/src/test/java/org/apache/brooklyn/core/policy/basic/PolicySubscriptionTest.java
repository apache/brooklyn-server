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
package org.apache.brooklyn.core.policy.basic;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class PolicySubscriptionTest extends BrooklynAppUnitTestSupport {

    // TODO Duplication between this and EntitySubscriptionTest
    
    private static final long SHORT_WAIT_MS = 100;
    
    private SimulatedLocation loc;
    private TestEntity entity;
    private TestEntity otherEntity;
    private MyPolicy policy;
    private RecordingSensorEventListener<Object> listener;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = app.newSimulatedLocation();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        otherEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        listener = new RecordingSensorEventListener<>();
        policy = entity.policies().add(PolicySpec.create(MyPolicy.class));
        app.start(ImmutableList.of(loc));
    }

    @Test
    public void testSubscriptionReceivesEvents() throws Exception {
        policy.subscriptions().subscribe(entity, TestEntity.SEQUENCE, listener);
        policy.subscriptions().subscribe(entity, TestEntity.NAME, listener);
        policy.subscriptions().subscribe(entity, TestEntity.MY_NOTIF, listener);
        
        otherEntity.sensors().set(TestEntity.SEQUENCE, 456);
        entity.sensors().set(TestEntity.SEQUENCE, 123);
        entity.sensors().set(TestEntity.NAME, "myname");
        entity.sensors().emit(TestEntity.MY_NOTIF, 789);
        
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                assertEquals(listener.getEvents(), ImmutableList.of(
                        new BasicSensorEvent<Integer>(TestEntity.SEQUENCE, entity, 123),
                        new BasicSensorEvent<String>(TestEntity.NAME, entity, "myname"),
                        new BasicSensorEvent<Integer>(TestEntity.MY_NOTIF, entity, 789)));
            }});
    }
    
    @Test
    public void testUnsubscribeRemovesAllSubscriptionsForThatEntity() throws Exception {
        policy.subscriptions().subscribe(entity, TestEntity.SEQUENCE, listener);
        policy.subscriptions().subscribe(entity, TestEntity.NAME, listener);
        policy.subscriptions().subscribe(entity, TestEntity.MY_NOTIF, listener);
        policy.subscriptions().subscribe(otherEntity, TestEntity.SEQUENCE, listener);
        policy.subscriptions().unsubscribe(entity);
        
        entity.sensors().set(TestEntity.SEQUENCE, 123);
        entity.sensors().set(TestEntity.NAME, "myname");
        entity.sensors().emit(TestEntity.MY_NOTIF, 456);
        otherEntity.sensors().set(TestEntity.SEQUENCE, 789);
        
        Thread.sleep(SHORT_WAIT_MS);
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                assertEquals(listener.getEvents(), ImmutableList.of(
                        new BasicSensorEvent<Integer>(TestEntity.SEQUENCE, otherEntity, 789)));
            }});
    }
    
    @Test
    @SuppressWarnings("unused")
    public void testUnsubscribeUsingHandleStopsEvents() throws Exception {
        SubscriptionHandle handle1 = policy.subscriptions().subscribe(entity, TestEntity.SEQUENCE, listener);
        SubscriptionHandle handle2 = policy.subscriptions().subscribe(entity, TestEntity.NAME, listener);
        SubscriptionHandle handle3 = policy.subscriptions().subscribe(otherEntity, TestEntity.SEQUENCE, listener);
        
        policy.subscriptions().unsubscribe(entity, handle2);
        
        entity.sensors().set(TestEntity.SEQUENCE, 123);
        entity.sensors().set(TestEntity.NAME, "myname");
        otherEntity.sensors().set(TestEntity.SEQUENCE, 456);
        
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                assertEquals(listener.getEvents(), ImmutableList.of(
                        new BasicSensorEvent<Integer>(TestEntity.SEQUENCE, entity, 123),
                        new BasicSensorEvent<Integer>(TestEntity.SEQUENCE, otherEntity, 456)));
            }});
    }

    @Test
    public void testSubscriptionReceivesInitialValueEventsInOrder() {
        entity.sensors().set(TestEntity.NAME, "myname");
        entity.sensors().set(TestEntity.SEQUENCE, 123);
        entity.sensors().emit(TestEntity.MY_NOTIF, -1);

        // delivery should be in subscription order, so 123 then 456
        policy.subscriptions().subscribe(ImmutableMap.of("notifyOfInitialValue", true), entity, TestEntity.SEQUENCE, listener);
        // wait for the above delivery - otherwise it might get dropped
        Asserts.succeedsEventually(MutableMap.of("timeout", Duration.seconds(5)), () -> { 
            Asserts.assertSize(listener.getEvents(), 1); });
        entity.sensors().set(TestEntity.SEQUENCE, 456);
        
        // notifications don't have "initial value" so don't get -1
        policy.subscriptions().subscribe(ImmutableMap.of("notifyOfInitialValue", true), entity, TestEntity.MY_NOTIF, listener);
        // but do get 1, after 456
        entity.sensors().emit(TestEntity.MY_NOTIF, 1);
        
        // STOPPING and myname received, in subscription order, after everything else
        entity.sensors().set(TestEntity.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);
        policy.subscriptions().subscribe(ImmutableMap.of("notifyOfInitialValue", true), entity, TestEntity.SERVICE_STATE_ACTUAL, listener);
        policy.subscriptions().subscribe(ImmutableMap.of("notifyOfInitialValue", true), entity, TestEntity.NAME, listener);
        
        Asserts.succeedsEventually(MutableMap.of("timeout", Duration.seconds(5)), new Runnable() {
            @Override public void run() {
                assertEquals(listener.getEvents(), ImmutableList.of(
                        new BasicSensorEvent<Integer>(TestEntity.SEQUENCE, entity, 123),
                        new BasicSensorEvent<Integer>(TestEntity.SEQUENCE, entity, 456),
                        new BasicSensorEvent<Integer>(TestEntity.MY_NOTIF, entity, 1),
                        new BasicSensorEvent<Lifecycle>(TestEntity.SERVICE_STATE_ACTUAL, entity, Lifecycle.STOPPING),
                        new BasicSensorEvent<String>(TestEntity.NAME, entity, "myname")),
                    "actually got: "+listener.getEvents());
            }});
    }
    
    @Test
    public void testSubscriptionNotReceivesInitialValueEventsByDefault() {
        entity.sensors().set(TestEntity.SEQUENCE, 123);
        entity.sensors().set(TestEntity.NAME, "myname");
        
        policy.subscriptions().subscribe(entity, TestEntity.SEQUENCE, listener);
        policy.subscriptions().subscribe(entity, TestEntity.NAME, listener);
        
        Asserts.succeedsContinually(ImmutableMap.of("timeout", SHORT_WAIT_MS), new Runnable() {
            @Override public void run() {
                Asserts.assertSize(listener.getEvents(), 0);
            }});
    }
    
    public static class MyPolicy extends AbstractPolicy {
    }
}
