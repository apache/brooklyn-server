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
package org.apache.brooklyn.entity.group;

import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

import static org.apache.brooklyn.test.Asserts.assertEqualsIgnoringOrder;
import static org.testng.Assert.assertEquals;


// TODO these tests fail as no osgi when running tests

public class GroupsChangePolicyTest extends BrooklynAppLiveTestSupport {

    @Test(enabled = false)
    public void testAddInitializers() {

        DynamicGroup dynamicGroup = app.addChild(EntitySpec.create(DynamicGroup.class));
        PolicySpec<GroupsChangePolicy> policySpec =
                PolicySpec.create(GroupsChangePolicy.class)
                        .configure(GroupsChangePolicy.GROUP, dynamicGroup)
                        .configure(GroupsChangePolicy.INITIALIZERS,
                                ImmutableList.of(
                                        ImmutableMap.of(
                                                "type", "org.apache.brooklyn.core.sensor.StaticSensor",
                                                "brooklyn.config", ImmutableMap.of(
                                                        "name", "mytestsensor",
                                                        "target.type", "string",
                                                        "static.value",  "$brooklyn:formatString(\"%s%s\",\"test\",\"sensor\")")
)));
        TestEntity myTestEntity = app.addChild(EntitySpec.create(TestEntity.class).configure(LocationConfigKeys.DISPLAY_NAME, "mytestentity"));
        dynamicGroup.policies().add(policySpec);
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(myTestEntity.getId()));

        assertEqualsIgnoringOrder(dynamicGroup.getMembers(), ImmutableList.of(myTestEntity));
        assertEquals(myTestEntity.policies().size(), 1);
    }

    @Test(enabled = false)
    public void testAddPolicies() {

        DynamicGroup dynamicGroup = app.addChild(EntitySpec.create(DynamicGroup.class));
        PolicySpec<GroupsChangePolicy> policySpec =
                PolicySpec.create(GroupsChangePolicy.class)
                        .configure(GroupsChangePolicy.GROUP, dynamicGroup)
                        .configure(GroupsChangePolicy.POLICIES,
                                ImmutableList.of(
                                        ImmutableMap.of(
                                                        "type", "org.apache.brooklyn.policy.action.PeriodicEffectorPolicy",
                                                        "brooklyn.config", ImmutableMap.of(
                                                        "period", "5s",
                                                        "effector", "$brooklyn:formatString(\"%s%s\",\"res\",\"tart\")",
                                                        "myconfig", "$brooklyn:formatString(\"%s%s\",\"res\", attributeWhenReady(\"tf.resource.type\"))"
                                                ))
                                        ));


        TestEntity myTestEntity = app.addChild(EntitySpec.create(TestEntity.class));
        dynamicGroup.policies().add(policySpec);
        dynamicGroup.setEntityFilter(EntityPredicates.idEqualTo(myTestEntity.getId()));

        assertEqualsIgnoringOrder(dynamicGroup.getMembers(), ImmutableList.of(myTestEntity));
        Asserts.eventually(new Supplier<String>() {
            @Override
            public String get() {
                return myTestEntity.getAttribute(Sensors.newStringSensor("mytestsensor"));
            }
        }, Predicates.<String> equalTo("testsensor"));
    }
}