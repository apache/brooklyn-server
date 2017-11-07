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
package org.apache.brooklyn.enricher.stock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.Test;

import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

public class UpdatingMapTest extends BrooklynAppUnitTestSupport {

    protected AttributeSensor<Object> mySensor = Sensors.newSensor(Object.class, "mySensor");
    
    @SuppressWarnings("serial")
    protected AttributeSensor<Map<String,Object>> mapSensor = Sensors.newSensor(
            new TypeToken<Map<String,Object>>() {},
            "mapSensor");

    @Test
    public void testUpdateServiceNotUpIndicator() throws Exception {
        AttributeSensor<Object> extraIsUpSensor = Sensors.newSensor(Object.class, "extraIsUp");
        
        Entity entity = app.createAndManageChild(EntitySpec.create(BasicApplication.class)
                .enricher(EnricherSpec.create(UpdatingMap.class)
                        .configure(UpdatingMap.SOURCE_SENSOR.getName(), extraIsUpSensor.getName())
                        .configure(UpdatingMap.TARGET_SENSOR, ServiceStateLogic.SERVICE_NOT_UP_INDICATORS)
                        .configure(UpdatingMap.COMPUTING, Functions.forMap(MutableMap.of(true, null, false, "valIsFalse"), "myDefault"))));

        assertMapSensorContainsEventually(entity, ServiceStateLogic.SERVICE_NOT_UP_INDICATORS, ImmutableMap.of("extraIsUp", "myDefault"));
        
        entity.sensors().set(extraIsUpSensor, true);
        assertMapSensorNotContainsKeysEventually(entity, ServiceStateLogic.SERVICE_NOT_UP_INDICATORS, ImmutableList.of("extraIsUp"));

        app.start(ImmutableList.<Location>of());
        
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEqualsContinually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        
        entity.sensors().set(extraIsUpSensor, false);
        EntityAsserts.assertAttributeEqualsEventually(entity, ServiceStateLogic.SERVICE_NOT_UP_INDICATORS, ImmutableMap.of("extraIsUp", "valIsFalse"));
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, false);

        entity.sensors().set(extraIsUpSensor, true);
        EntityAsserts.assertAttributeEqualsEventually(entity, ServiceStateLogic.SERVICE_NOT_UP_INDICATORS, ImmutableMap.of());
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
    }
    
    @Test
    public void testUpdateMapUsingDifferentProducer() throws Exception {
        Entity producer = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Entity entity = app.createAndManageChild(EntitySpec.create(BasicApplication.class)
                .enricher(EnricherSpec.create(UpdatingMap.class)
                        .configure(UpdatingMap.PRODUCER, producer)
                        .configure(UpdatingMap.SOURCE_SENSOR.getName(), mySensor.getName())
                        .configure(UpdatingMap.TARGET_SENSOR, mapSensor)
                        .configure(UpdatingMap.KEY_IN_TARGET_SENSOR, "myKey")
                        .configure(UpdatingMap.COMPUTING, Functions.forMap(MutableMap.of("v1", "valIsV1", "v2", "valIsV2"), "myDefault"))));

        EntityAsserts.assertAttributeEqualsEventually(entity, mapSensor, ImmutableMap.of("myKey", "myDefault"));
        
        producer.sensors().set(mySensor, "v1");
        EntityAsserts.assertAttributeEqualsEventually(entity, mapSensor, ImmutableMap.of("myKey", "valIsV1"));
    }
    
    @Test
    public void testUpdateMapEmitsEventOnChange() throws Exception {
        Entity entity = app.createAndManageChild(EntitySpec.create(BasicApplication.class)
                .enricher(EnricherSpec.create(UpdatingMap.class)
                        .configure(UpdatingMap.SOURCE_SENSOR.getName(), mySensor.getName())
                        .configure(UpdatingMap.TARGET_SENSOR, mapSensor)
                        .configure(UpdatingMap.KEY_IN_TARGET_SENSOR, "myKey")
                        .configure(UpdatingMap.COMPUTING, Functions.forMap(MutableMap.of("v1", "valIsV1", "v2", "valIsV2"), "myDefault"))));

        EntityAsserts.assertAttributeEqualsEventually(entity, mapSensor, ImmutableMap.of("myKey", "myDefault"));
        
        RecordingSensorEventListener<Map<String, Object>> listener = new RecordingSensorEventListener<>();
        app.subscriptions().subscribe(entity, mapSensor, listener);
        
        entity.sensors().set(mySensor, "v1");
        EntityAsserts.assertAttributeEqualsEventually(entity, mapSensor, ImmutableMap.of("myKey", "valIsV1"));
        
        listener.assertHasEventEventually(Predicates.alwaysTrue());
        Map<String, Object> ev = listener.removeEvent(0).getValue();
        if (ev.equals(ImmutableMap.of("myKey", "myDefault"))) {
            // possible on slow machine for this to pick up myDefault computed late; get next
            listener.assertHasEventEventually(Predicates.alwaysTrue());
            ev = listener.removeEvent(0).getValue();
        }
        assertEquals(ev, ImmutableMap.of("myKey", "valIsV1"));
        Asserts.assertSize(listener.getEventValues(), 0);
    }
    
    @Test
    public void testRemovingIfResultIsNull() throws Exception {
        Entity entity = app.createAndManageChild(EntitySpec.create(BasicApplication.class)
                .enricher(EnricherSpec.create(UpdatingMap.class)
                        .configure(UpdatingMap.SOURCE_SENSOR.getName(), mySensor.getName())
                        .configure(UpdatingMap.TARGET_SENSOR, mapSensor)
                        .configure(UpdatingMap.REMOVING_IF_RESULT_IS_NULL, true)
                        .configure(UpdatingMap.COMPUTING, Functions.forMap(MutableMap.of("v1", "valIsV1"), null))));

        
        entity.sensors().set(mySensor, "v1");
        EntityAsserts.assertAttributeEqualsEventually(entity, mapSensor, ImmutableMap.of("mySensor", "valIsV1"));

        entity.sensors().set(mySensor, "different");
        EntityAsserts.assertAttributeEqualsEventually(entity, mapSensor, ImmutableMap.of());
    }

    @Test
    public void testNotRemovingIfResultIsNull() throws Exception {
        Entity entity = app.createAndManageChild(EntitySpec.create(BasicApplication.class)
                .enricher(EnricherSpec.create(UpdatingMap.class)
                        .configure(UpdatingMap.SOURCE_SENSOR.getName(), mySensor.getName())
                        .configure(UpdatingMap.TARGET_SENSOR, mapSensor)
                        .configure(UpdatingMap.REMOVING_IF_RESULT_IS_NULL, false)
                        .configure(UpdatingMap.COMPUTING, Functions.forMap(MutableMap.of("v1", "valIsV1"), null))));

        
        entity.sensors().set(mySensor, "v1");
        EntityAsserts.assertAttributeEqualsEventually(entity, mapSensor, ImmutableMap.of("mySensor", "valIsV1"));

        entity.sensors().set(mySensor, "different");
        EntityAsserts.assertAttributeEqualsEventually(entity, mapSensor, MutableMap.of("mySensor", null));
    }
    
    private void assertMapSensorContainsEventually(Entity entity, AttributeSensor<? extends Map<?, ?>> mapSensor, Map<?, ?> expected) {
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                Map<?, ?> actual = entity.sensors().get(mapSensor);
                String errMsg = "actual="+actual+"; expected="+expected;
                for (Map.Entry<?,?> entry : expected.entrySet()) {
                    assertTrue(actual.containsKey(entry.getKey()), errMsg);
                    assertEquals(actual.get(entry.getKey()), entry.getValue(), errMsg);
                }
            }});
    }
    
    private void assertMapSensorNotContainsKeysEventually(Entity entity, AttributeSensor<? extends Map<?, ?>> mapSensor, Iterable<?> keys) {
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                Map<?, ?> actual = entity.sensors().get(mapSensor);
                String errMsg = "actual="+actual+"; notExpected="+keys;
                for (Object key : keys) {
                    assertFalse(actual.containsKey(key), errMsg);
                }
            }});
    }
}
