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
import static org.testng.Assert.fail;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.SubscriptionManager;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.BasicGroup;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * testing the {@link SubscriptionManager} and associated classes.
 */
public class LocalSubscriptionManagerTest extends BrooklynAppUnitTestSupport {
    
    private static final int TIMEOUT_MS = 5000;
    
    private TestEntity entity;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
    }

    @Test
    public void testSubscribeToEntityAttributeChange() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        app.subscriptions().subscribe(entity, TestEntity.SEQUENCE, new SensorEventListener<Object>() {
                @Override public void onEvent(SensorEvent<Object> event) {
                    latch.countDown();
                }});
        entity.setSequenceValue(1234);
        if (!latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            fail("Timeout waiting for Event on TestEntity listener");
        }
    }
    
    @Test
    public void testSubscribeToEntityWithAttributeWildcard() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        app.subscriptions().subscribe(entity, null, new SensorEventListener<Object>() {
            @Override public void onEvent(SensorEvent<Object> event) {
                latch.countDown();
            }});
        entity.setSequenceValue(1234);
        if (!latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            fail("Timeout waiting for Event on TestEntity listener");
        }
    }
    
    @Test
    public void testSubscribeToAttributeChangeWithEntityWildcard() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        app.subscriptions().subscribe(null, TestEntity.SEQUENCE, new SensorEventListener<Object>() {
                @Override public void onEvent(SensorEvent<Object> event) {
                    latch.countDown();
                }});
        entity.setSequenceValue(1234);
        if (!latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            fail("Timeout waiting for Event on TestEntity listener");
        }
    }
    
    @Test
    public void testSubscribeToChildAttributeChange() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        app.subscriptions().subscribeToChildren(app, TestEntity.SEQUENCE, new SensorEventListener<Object>() {
            @Override public void onEvent(SensorEvent<Object> event) {
                latch.countDown();
            }});
        entity.setSequenceValue(1234);
        if (!latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            fail("Timeout waiting for Event on child TestEntity listener");
        }
    }
    
    @Test
    public void testSubscribeToMemberAttributeChange() throws Exception {
        BasicGroup group = app.createAndManageChild(EntitySpec.create(BasicGroup.class));
        TestEntity member = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        group.addMember(member);

        final List<SensorEvent<Integer>> events = new CopyOnWriteArrayList<SensorEvent<Integer>>();
        final CountDownLatch latch = new CountDownLatch(1);
        app.subscriptions().subscribeToMembers(group, TestEntity.SEQUENCE, new SensorEventListener<Integer>() {
            @Override public void onEvent(SensorEvent<Integer> event) {
                events.add(event);
                latch.countDown();
            }});
        member.sensors().set(TestEntity.SEQUENCE, 123);

        if (!latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            fail("Timeout waiting for Event on parent TestEntity listener");
        }
        assertEquals(events.size(), 1);
        assertEquals(events.get(0).getValue(), (Integer)123);
        assertEquals(events.get(0).getSensor(), TestEntity.SEQUENCE);
        assertEquals(events.get(0).getSource().getId(), member.getId());
    }
    
    // Regression test for ConcurrentModificationException in issue #327
    @Test(groups="Integration")
    public void testConcurrentSubscribingAndPublishing() throws Exception {
        final AtomicReference<Exception> threadException = new AtomicReference<Exception>();
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        // Repeatedly subscribe and unsubscribe, so listener-set constantly changing while publishing to it.
        // First create a stable listener so it is always the same listener-set object.
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    SensorEventListener<Object> noopListener = new SensorEventListener<Object>() {
                        @Override public void onEvent(SensorEvent<Object> event) {
                        }
                    };
                    app.subscriptions().subscribe(null, TestEntity.SEQUENCE, noopListener);
                    while (!Thread.currentThread().isInterrupted()) {
                        SubscriptionHandle handle = app.subscriptions().subscribe(null, TestEntity.SEQUENCE, noopListener);
                        app.subscriptions().unsubscribe(null, handle);
                    }
                } catch (Exception e) {
                    threadException.set(e);
                }
            }
        };
        
        try {
            thread.start();
            for (int i = 0; i < 10000; i++) {
                entity.sensors().set(TestEntity.SEQUENCE, i);
            }
        } finally {
            thread.interrupt();
        }

        if (threadException.get() != null) throw threadException.get();
    }

    @Test
    // same test as in PolicySubscriptionTest, but for entities / simpler
    public void testSubscriptionReceivesInitialValueEventsInOrder() {
        RecordingSensorEventListener<Object> listener = new RecordingSensorEventListener<>();
        
        entity.sensors().set(TestEntity.NAME, "myname");
        entity.sensors().set(TestEntity.SEQUENCE, 123);
        entity.sensors().emit(TestEntity.MY_NOTIF, -1);

        // delivery should be in subscription order, so 123 then 456
        entity.subscriptions().subscribe(ImmutableMap.of("notifyOfInitialValue", true), entity, TestEntity.SEQUENCE, listener);
        // wait for the above delivery - otherwise it might get dropped
        Asserts.succeedsEventually(MutableMap.of("timeout", Duration.seconds(5)), () -> { 
            Asserts.assertSize(listener.getEvents(), 1); });
        entity.sensors().set(TestEntity.SEQUENCE, 456);
        
        // notifications don't have "initial value" so don't get -1
        entity.subscriptions().subscribe(ImmutableMap.of("notifyOfInitialValue", true), entity, TestEntity.MY_NOTIF, listener);
        // but do get 1, after 456
        entity.sensors().emit(TestEntity.MY_NOTIF, 1);
        
        // STOPPING and myname received, in subscription order, after everything else
        entity.sensors().set(TestEntity.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);
        entity.subscriptions().subscribe(ImmutableMap.of("notifyOfInitialValue", true), entity, TestEntity.SERVICE_STATE_ACTUAL, listener);
        entity.subscriptions().subscribe(ImmutableMap.of("notifyOfInitialValue", true), entity, TestEntity.NAME, listener);
        
        Asserts.succeedsEventually(MutableMap.of("timeout", Duration.seconds(5)), new Runnable() {
            @Override public void run() {
                Asserts.assertEquals(listener.getEvents(), ImmutableList.of(
                        new BasicSensorEvent<Integer>(TestEntity.SEQUENCE, entity, 123),
                        new BasicSensorEvent<Integer>(TestEntity.SEQUENCE, entity, 456),
                        new BasicSensorEvent<Integer>(TestEntity.MY_NOTIF, entity, 1),
                        new BasicSensorEvent<Lifecycle>(TestEntity.SERVICE_STATE_ACTUAL, entity, Lifecycle.STOPPING),
                        new BasicSensorEvent<String>(TestEntity.NAME, entity, "myname")),
                    "actually got: "+listener.getEvents());
            }});
    }
    
    @Test
    public void testNotificationOrderMatchesSetValueOrderWhenSynched() {
        RecordingSensorEventListener<Object> listener = new RecordingSensorEventListener<>();
        
        AtomicInteger count = new AtomicInteger();
        Runnable set = () -> { 
            synchronized (count) { 
                entity.sensors().set(TestEntity.SEQUENCE, count.incrementAndGet()); 
            } 
        };
        entity.subscriptions().subscribe(ImmutableMap.of(), entity, TestEntity.SEQUENCE, listener);
        for (int i=0; i<10; i++) {
            new Thread(set).start();
        }
        
        Asserts.succeedsEventually(MutableMap.of("timeout", Duration.seconds(5)), () -> { 
            Asserts.assertSize(listener.getEvents(), 10); });
        for (int i=0; i<10; i++) {
            Assert.assertEquals(listener.getEvents().get(i).getValue(), i+1);
        }
    }

    @Test
    public void testNotificationOrderMatchesSetValueOrderWhenNotSynched() {
        RecordingSensorEventListener<Object> listener = new RecordingSensorEventListener<>();
        
        AtomicInteger count = new AtomicInteger();
        Runnable set = () -> { 
            // as this is not synched, the sets may interleave
            entity.sensors().set(TestEntity.SEQUENCE, count.incrementAndGet()); 
        };
        entity.subscriptions().subscribe(ImmutableMap.of(), entity, TestEntity.SEQUENCE, listener);
        for (int i=0; i<10; i++) {
            new Thread(set).start();
        }
        
        Asserts.succeedsEventually(MutableMap.of("timeout", Duration.seconds(5)), () -> { 
            Asserts.assertSize(listener.getEvents(), 10); });
        // all we expect for sure is that the last value is whatever the sensor is at the end - internal update and publish is mutexed
        Assert.assertEquals(listener.getEvents().get(9).getValue(), entity.sensors().get(TestEntity.SEQUENCE));
    }

}
