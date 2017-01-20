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

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.find;
import static org.apache.brooklyn.core.entity.EntityPredicates.displayNameEqualTo;
import static org.apache.brooklyn.entity.group.DynamicMultiGroup.BUCKET_FUNCTION;
import static org.apache.brooklyn.entity.group.DynamicMultiGroupImpl.bucketFromAttribute;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.List;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class DynamicMultiGroupRebindTest extends RebindTestFixtureWithApp {

    private static final AttributeSensor<String> SENSOR = Sensors.newSensor(String.class, "multigroup.test");

    // Previously there was a bug on rebind. The entity's rebind would immediately connec the 
    // rescan, which would start executing in another thread. If there were any empty buckets
    // (i.e. groups) that child would be removed. But the rebind-manager would still be executing
    // concurrently. The empty group that was being removed might not have been reconstituted yet.
    // When we then tried to reconstitute it, the abstractEntity.setParent would fail with an 
    // error about being previouslyOwned, causing rebind to fail.
    //
    // To recreate this error, need to have several apps concurrently so that the rebind order
    // of the entities will be interleaved.
    @Test(groups="Integration", invocationCount=10)
    public void testRebindWhenGroupDisappeared() throws Exception {
        int NUM_ITERATIONS = 10;
        List<DynamicMultiGroup> dmgs = Lists.newArrayList();
        List<TestEntity> childs = Lists.newArrayList();
        
        // Create lots of DynamicMultiGroups - one entity for each
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            Group group = origApp.createAndManageChild(EntitySpec.create(BasicGroup.class));
            DynamicMultiGroup dmg = origApp.createAndManageChild(EntitySpec.create(DynamicMultiGroup.class)
                    .displayName("dmg"+i)
                    .configure(DynamicMultiGroup.ENTITY_FILTER, Predicates.and(EntityPredicates.displayNameEqualTo("child"+i), instanceOf(TestEntity.class)))
                    .configure(DynamicMultiGroup.RESCAN_INTERVAL, 5L)
                    .configure(BUCKET_FUNCTION, bucketFromAttribute(SENSOR))
                    .configure(DynamicMultiGroup.BUCKET_SPEC, EntitySpec.create(BasicGroup.class)));
            dmgs.add(dmg);
            
            TestEntity child = group.addChild(EntitySpec.create(TestEntity.class).displayName("child"+i));
            child.sensors().set(SENSOR, "bucketA");
            childs.add(child);
        }
        
        // Wait for all DynamicMultiGroups to have initialised correctly
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            final DynamicMultiGroup dmg = dmgs.get(i);
            final TestEntity child = childs.get(i);
            Asserts.succeedsEventually(new Runnable() {
                @Override
                public void run() {
                    Group bucketA = (Group) find(dmg.getChildren(), displayNameEqualTo("bucketA"), null);
                    assertNotNull(bucketA);
                    assertEquals(ImmutableSet.copyOf(bucketA.getMembers()), ImmutableSet.of(child));
                }
            });
        }

        // Quickly change the child sensors, and shutdown immediately before the DynamicMultiGroups 
        // rescan and update themselves.
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            childs.get(i).sensors().set(SENSOR, "bucketB");
        }
        rebind(RebindOptions.create().terminateOrigManagementContext(true));
        
        // Check that all entities are in the new expected groups
        for (int i = 0; i < NUM_ITERATIONS; i++) {
            final DynamicMultiGroup dmg = (DynamicMultiGroup) newManagementContext.getEntityManager().getEntity(dmgs.get(i).getId());
            final TestEntity child = (TestEntity) newManagementContext.getEntityManager().getEntity(childs.get(i).getId());
            // FIXME Remove timeout; use default
            Asserts.succeedsEventually(new Runnable() {
                @Override
                public void run() {
                    Group bucketA = (Group) find(dmg.getChildren(), displayNameEqualTo("bucketA"), null);
                    Group bucketB = (Group) find(dmg.getChildren(), displayNameEqualTo("bucketB"), null);
                    assertNull(bucketA);
                    assertNotNull(bucketB);
                    assertEquals(ImmutableSet.copyOf(bucketB.getMembers()), ImmutableSet.of(child));
                }
            });
        }
    }
}
