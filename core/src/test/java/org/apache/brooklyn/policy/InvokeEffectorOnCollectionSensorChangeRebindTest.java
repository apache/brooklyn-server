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

import java.util.Collection;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

public class InvokeEffectorOnCollectionSensorChangeRebindTest extends RebindTestFixtureWithApp {

    private static final AttributeSensor<Collection<Integer>> SENSOR = Sensors.newSensor(new TypeToken<Collection<Integer>>() {},
            "invokeeffectoronsetchangerebindtest.sensor");

    private static final AttributeSensor<Collection<Object>> REMOVED_EFFECTOR_VALUES = Sensors.newSensor(new TypeToken<Collection<Object>>() {},
            "invokeeffectoronsetchangerebindtest.removedvalues");

    @Test
    public void testEffectorMaintainsPreviousCollectionThroughRebind() throws Exception {
        final Set<Integer> input1 = ImmutableSet.of(1, 2);
        final Set<Integer> input2 = ImmutableSet.of(2, 3);
        final Set<Integer> input3 = ImmutableSet.of(3, 4);

        Entity testEntity = app().createAndManageChild(EntitySpec.create(TestEntity.class)
                .policy(PolicySpec.create(InvokeEffectorOnCollectionSensorChange.class)
                        .configure(InvokeEffectorOnCollectionSensorChange.TRIGGER_SENSOR, SENSOR)
                        .configure(InvokeEffectorOnCollectionSensorChange.ON_REMOVED_EFFECTOR_NAME, "on-removed-effector"))
                .addInitializer(new AddEffector(Effectors.effector(Void.class, "on-removed-effector")
                        .impl(new PublishingEffector())
                        .build())));
        testEntity.sensors().set(SENSOR, input1);
        testEntity.sensors().set(SENSOR, input2);
        EntityAsserts.assertAttributeEqualsEventually(testEntity, REMOVED_EFFECTOR_VALUES, ImmutableSet.<Object>of(1));

        newApp = rebind();

        testEntity = Iterables.getOnlyElement(newApp.getChildren());
        testEntity.sensors().set(SENSOR, input3);
        EntityAsserts.assertAttributeEqualsEventually(testEntity, REMOVED_EFFECTOR_VALUES, ImmutableSet.<Object>of(1, 2));
    }


    private static class PublishingEffector extends EffectorBody<Void> {
        @Override
        public Void call(ConfigBag parameters) {
            synchronized (PublishingEffector.class) {
                Collection<Object> values = entity().sensors().get(REMOVED_EFFECTOR_VALUES);
                if (values == null) {
                    values = Sets.newHashSet();
                }
                final Object v = parameters.getStringKey("value");
                values.add(v);
                entity().sensors().set(REMOVED_EFFECTOR_VALUES, values);
                return null;
            }
        }
    }

}
