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

package org.apache.brooklyn.policy;

import static org.apache.brooklyn.policy.InvokeEffectorOnCollectionSensorChange.PARAMETER_NAME;
import static org.testng.Assert.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

public class InvokeEffectorOnCollectionSensorChangeTest extends BrooklynAppUnitTestSupport {

    private static final AttributeSensor<Collection<Integer>> DEFAULT_SENSOR = Sensors.newSensor(new TypeToken<Collection<Integer>>() {},
            "invokeeffectoronsetchangetest.sensor");

    private static final AttributeSensor<Boolean> IS_BUSY_SENSOR = Sensors.newBooleanSensor(
            "invokeeffectoronsetchangetest.isBusy");

    LinkedBlockingQueue<ConfigBag> onAddedParameters;
    LinkedBlockingQueue<ConfigBag> onRemovedParameters;
    Effector<Void> onAddedEffector;
    Effector<Void> onRemovedEffector;
    TestEntity testEntity;

    @Override
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
        onAddedParameters = new LinkedBlockingQueue<>();
        onRemovedParameters = new LinkedBlockingQueue<>();
        onAddedEffector = Effectors.effector(Void.class, "on-added-effector")
                .impl(new RecordingEffector(onAddedParameters))
                .build();
        onRemovedEffector = Effectors.effector(Void.class, "on-removed-effector")
                .impl(new RecordingEffector(onRemovedParameters))
                .build();
        testEntity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .addInitializer(new AddEffector(onAddedEffector))
                .addInitializer(new AddEffector(onRemovedEffector)));
    }

    @Test
    public void testOnAddedEffectorCalledWhenItemsAdded() throws Exception {
        addSetChangePolicy(true, false);
        final Set<Integer> values = ImmutableSet.of(1);
        testEntity.sensors().set(DEFAULT_SENSOR, values);
        ConfigBag params = onAddedParameters.poll(10, TimeUnit.SECONDS);
        assertEquals(params.getStringKey(PARAMETER_NAME.getDefaultValue()), 1);
    }

    @Test
    public void testOnRemovedEffectorNotCalledWhenItemsAdded() throws Exception {
        addSetChangePolicy(false, true);
        final Set<Integer> values = ImmutableSet.of(1);
        testEntity.sensors().set(DEFAULT_SENSOR, values);
        Asserts.continually(CollectionFunctionals.sizeSupplier(onRemovedParameters), Predicates.equalTo(0));
    }

    @Test
    public void testOnRemovedEffectorCalledWhenItemRemoved() throws Exception {
        testEntity.sensors().set(DEFAULT_SENSOR, ImmutableSet.of(1, 2));
        addSetChangePolicy(false, true);
        final Set<Integer> values = ImmutableSet.of(1);
        testEntity.sensors().set(DEFAULT_SENSOR, values);
        ConfigBag params = onRemovedParameters.poll(10, TimeUnit.SECONDS);
        assertEquals(params.getStringKey(PARAMETER_NAME.getDefaultValue()), 2);
    }

    @Test
    public void testOnAddedEffectorNotCalledWhenItemRemoved() throws Exception {
        testEntity.sensors().set(DEFAULT_SENSOR, ImmutableSet.of(1));
        addSetChangePolicy(true, false);
        testEntity.sensors().set(DEFAULT_SENSOR, ImmutableSet.<Integer>of());
        Asserts.continually(CollectionFunctionals.sizeSupplier(onRemovedParameters), Predicates.equalTo(0));
    }

    @Test
    public void testSeveralItemsAddedAndRemovedAtOnce() throws Exception {
        testEntity.sensors().set(DEFAULT_SENSOR, ImmutableSet.of(1, 2, 3));
        addSetChangePolicy(true, true);
        testEntity.sensors().set(DEFAULT_SENSOR, ImmutableSet.of(3, 4, 5));

        // 1 and 2 were removed, 4 and 5 were added.
        Asserts.eventually(new ConfigBagValueKeySupplier(onRemovedParameters),
                Predicates.<Collection<Object>>equalTo(ImmutableSet.<Object>of(1, 2)));
        Asserts.eventually(new ConfigBagValueKeySupplier(onAddedParameters),
                Predicates.<Collection<Object>>equalTo(ImmutableSet.<Object>of(4, 5)));
    }

    @Test
    public void testNothingHappensWhenSensorRepublishedUnchanged() {
        final ImmutableSet<Integer> input1 = ImmutableSet.of(1, 2, 3);
        testEntity.sensors().set(DEFAULT_SENSOR, input1);
        addSetChangePolicy(true, true);
        testEntity.sensors().set(DEFAULT_SENSOR, input1);
        // Neither effector should be invoked.
        Asserts.continually(CollectionFunctionals.sizeSupplier(onAddedParameters), Predicates.equalTo(0));
        Asserts.continually(CollectionFunctionals.sizeSupplier(onRemovedParameters), Predicates.equalTo(0));
    }

    @Test
    public void testCollectionsAreConvertedToSets() {
        final List<Integer> input1 = ImmutableList.of(
                1, 1,
                2, 3, 4, 5,
                2, 3, 4, 5);
        final List<Integer> input2 = ImmutableList.of(6, 5, 4, 3, 3);

        addSetChangePolicy(true, true);

        testEntity.sensors().set(DEFAULT_SENSOR, input1);
        Asserts.eventually(new ConfigBagValueKeySupplier(onAddedParameters),
                Predicates.<Collection<Object>>equalTo(ImmutableSet.<Object>of(1, 2, 3, 4, 5)));
        Asserts.continually(CollectionFunctionals.sizeSupplier(onRemovedParameters), Predicates.equalTo(0));

        onAddedParameters.clear();

        testEntity.sensors().set(DEFAULT_SENSOR, input2);
        Asserts.eventually(new ConfigBagValueKeySupplier(onAddedParameters),
                Predicates.<Collection<Object>>equalTo(ImmutableSet.<Object>of(6)));
        Asserts.eventually(new ConfigBagValueKeySupplier(onRemovedParameters),
                Predicates.<Collection<Object>>equalTo(ImmutableSet.<Object>of(1, 2)));
    }

    @Test
    public void testMapValueUsedAsArgumentDirectly() {
        AttributeSensor<Collection<Map<String, String>>> sensor = Sensors.newSensor(new TypeToken<Collection<Map<String, String>>>() {},
                "testMapValueUsedAsArgumentDirectly");
        final Set<Map<String, String>> input1 = ImmutableSet.<Map<String, String>>of(
                ImmutableMap.of("a", "1"),
                ImmutableMap.of("b", "2"));
        final Set<Map<String, String>> input2 = ImmutableSet.<Map<String, String>>of(
                ImmutableMap.of("b", "2"),
                ImmutableMap.of("c", "3"),
                ImmutableMap.of("d", "4"));

        testEntity.sensors().set(sensor, input1);
        addSetChangePolicy(sensor, true, true);

        testEntity.sensors().set(sensor, input2);
        Asserts.eventually(new ConfigBagMapSupplier(onAddedParameters),
                Predicates.<Collection<Map<String, Object>>>equalTo(ImmutableSet.<Map<String, Object>>of(
                        ImmutableMap.<String, Object>of("c", "3"),
                        ImmutableMap.<String, Object>of("d", "4"))));
        Asserts.eventually(new ConfigBagMapSupplier(onRemovedParameters),
                Predicates.<Collection<Map<String, Object>>>equalTo(ImmutableSet.<Map<String, Object>>of(
                        ImmutableMap.<String, Object>of("a", "1"))));

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testErrorIfNeitherOnAddedNorOnRemovedAreSet() {
        addSetChangePolicy(false, false);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testErrorIfTriggerSensorNotSet() {
        testEntity.policies().add(PolicySpec.create(InvokeEffectorOnCollectionSensorChange.class)
                .configure(InvokeEffectorOnCollectionSensorChange.ON_ADDED_EFFECTOR_NAME, onAddedEffector.getName())
                .configure(InvokeEffectorOnCollectionSensorChange.ON_REMOVED_EFFECTOR_NAME, onRemovedEffector.getName()));
    }

    @Test
    public void testPublishesIsBusySensor() {
        final List<Boolean> isBusyValues = new CopyOnWriteArrayList<>();
        testEntity.subscriptions().subscribe(testEntity, IS_BUSY_SENSOR, new SensorEventListener<Boolean>() {
            @Override
            public void onEvent(SensorEvent<Boolean> event) {
                isBusyValues.add(event.getValue());
            }
        });
        addSetChangePolicy(true, false);
        testEntity.sensors().set(DEFAULT_SENSOR, ImmutableSet.of(1));
        List<Boolean> expected = ImmutableList.of(false, true, false);
        Asserts.eventually(Suppliers.ofInstance(isBusyValues), Predicates.equalTo(expected));
    }

    private void addSetChangePolicy(boolean includeOnAdded, boolean includeOnRemoved) {
        addSetChangePolicy(DEFAULT_SENSOR, includeOnAdded, includeOnRemoved);
    }

    private void addSetChangePolicy(AttributeSensor<? extends Collection<?>> sensor, boolean includeOnAdded, boolean includeOnRemoved) {
        PolicySpec<InvokeEffectorOnCollectionSensorChange> policySpec = PolicySpec.create(InvokeEffectorOnCollectionSensorChange.class)
                .configure(InvokeEffectorOnCollectionSensorChange.TRIGGER_SENSOR, sensor)
                .configure(InvokeEffectorOnCollectionSensorChange.IS_BUSY_SENSOR_NAME, IS_BUSY_SENSOR.getName());
        if (includeOnAdded) {
            policySpec.configure(InvokeEffectorOnCollectionSensorChange.ON_ADDED_EFFECTOR_NAME, onAddedEffector.getName());
        }
        if (includeOnRemoved) {
            policySpec.configure(InvokeEffectorOnCollectionSensorChange.ON_REMOVED_EFFECTOR_NAME, onRemovedEffector.getName());
        }
        testEntity.policies().add(policySpec);
    }

    private static class RecordingEffector extends EffectorBody<Void> {
        final Collection<ConfigBag> callParameters;

        private RecordingEffector(Collection<ConfigBag> callParameters) {
            this.callParameters = callParameters;
        }

        @Override
        public Void call(ConfigBag config) {
            callParameters.add(config);
            return null;
        }
    }

    private static class ConfigBagValueKeySupplier implements Supplier<Collection<Object>> {
        private final Collection<ConfigBag> collection;

        private ConfigBagValueKeySupplier(Collection<ConfigBag> collection) {
            this.collection = collection;
        }

        @Override
        public Collection<Object> get() {
            Set<Object> set = new HashSet<>();
            for (ConfigBag bag : collection) {
                set.add(bag.getStringKey(PARAMETER_NAME.getDefaultValue()));
            }
            return set;
        }
    }

    private static class ConfigBagMapSupplier implements Supplier<Collection<Map<String, Object>>> {
        private final Collection<ConfigBag> collection;


        private ConfigBagMapSupplier(Collection<ConfigBag> collection) {
            this.collection = collection;
        }

        @Override
        public Collection<Map<String, Object>> get() {
            Set<Map<String, Object>> values = new HashSet<>(collection.size());
            for (ConfigBag bag : collection) {
                values.add(bag.getAllConfigRaw());
            }
            return values;
        }
    }

}