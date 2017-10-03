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
package org.apache.brooklyn.launcher;

import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.testng.annotations.Test;

/**
 * When entity that the adjunct is associated with is deleted, the adjunct can safely be deleted.
 */
public class CleanOrphanedAdjunctsTest extends AbstractCleanOrphanedStateTest {

    @Test
    public void testDeletesOrphanedEnricher() throws Exception {
        Entity entity = origApp.addChild(EntitySpec.create(TestEntity.class).impl(MyEntity.class));
        MyEnricher enricher = entity.enrichers().add(EnricherSpec.create(MyEnricher.class));
        MementoTweaker tweaker = new MementoTweaker(new Deletions().entities(entity.getId()));
        assertTransformDeletes(new Deletions().enrichers(enricher.getId()), tweaker);
    }
    
    @Test
    public void testDeletesOrphanedPolicies() throws Exception {
        Entity entity = origApp.addChild(EntitySpec.create(TestEntity.class).impl(MyEntity.class));
        MyPolicy policy = entity.policies().add(PolicySpec.create(MyPolicy.class));
        MementoTweaker tweaker = new MementoTweaker(new Deletions().entities(entity.getId()));
        assertTransformDeletes(new Deletions().policies(policy.getId()), tweaker);
    }
    
    @Test
    public void testDeletesOrphanedFeeds() throws Exception {
        EntityInternal entity = origApp.addChild(EntitySpec.create(TestEntity.class).impl(MyEntity.class));
        Feed feed = new MyFeed();
        entity.feeds().add(feed);
        MementoTweaker tweaker = new MementoTweaker(new Deletions().entities(entity.getId()));
        assertTransformDeletes(new Deletions().feeds(feed.getId()), tweaker);
    }
    
    @Test
    public void testKeepsReachableAdjuncts() throws Exception {
        MyPolicy policy = origApp.policies().add(PolicySpec.create(MyPolicy.class));
        MyEnricher enricher = origApp.enrichers().add(EnricherSpec.create(MyEnricher.class));
        Feed feed = new MyFeed();
        origApp.feeds().add(feed);
        
        // Double-check we have the state we expected for the assertions that it is unmodified!
        BrooklynMementoRawData origData = getRawData();
        assertTrue(origData.getPolicies().containsKey(policy.getId()));
        assertTrue(origData.getEnrichers().containsKey(enricher.getId()));
        assertTrue(origData.getFeeds().containsKey(feed.getId()));
        
        assertTransformIsNoop(origData);
    }
}
