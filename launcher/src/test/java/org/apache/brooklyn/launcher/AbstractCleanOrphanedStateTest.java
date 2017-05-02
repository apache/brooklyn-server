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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Set;

import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.feed.AbstractFeed;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.core.mgmt.rebind.transformer.impl.DeleteOrphanedStateTransformer;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.commons.lang.builder.EqualsBuilder;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public abstract class AbstractCleanOrphanedStateTest extends RebindTestFixtureWithApp {
    
    public static class MementoTweaker implements Function<BrooklynMementoRawData, BrooklynMementoRawData> {
        private Deletions deletions;
        
        public MementoTweaker(Deletions deletions) {
            this.deletions = deletions;
        }
        @Override
        public BrooklynMementoRawData apply(BrooklynMementoRawData input) {
            return BrooklynMementoRawData.builder()
                    .planeId(input.getPlaneId())
                    .brooklynVersion(input.getBrooklynVersion())
                    .catalogItems(input.getCatalogItems())
                    .bundles(input.getBundles())
                    .entities(MutableMap.<String, String>builder().putAll(input.getEntities()).removeAll(deletions.entities).build())
                    .locations(MutableMap.<String, String>builder().putAll(input.getLocations()).removeAll(deletions.locations).build())
                    .feeds(MutableMap.<String, String>builder().putAll(input.getFeeds()).removeAll(deletions.feeds).build())
                    .enrichers(MutableMap.<String, String>builder().putAll(input.getEnrichers()).removeAll(deletions.enrichers).build())
                    .policies(MutableMap.<String, String>builder().putAll(input.getPolicies()).removeAll(deletions.policies).build())
                    .build();

        }
    }

    protected BrooklynMementoRawData assertTransformIsNoop() throws Exception {
        return assertTransformIsNoop(getRawData());
    }

    protected BrooklynMementoRawData assertTransformIsNoop(BrooklynMementoRawData origData) throws Exception {
        return assertTransformIsNoop(origData, Functions.<BrooklynMementoRawData>identity());
    }

    protected BrooklynMementoRawData assertTransformIsNoop(Function<? super BrooklynMementoRawData, ? extends BrooklynMementoRawData> origDataTweaker) throws Exception {
        return assertTransformIsNoop(getRawData(), origDataTweaker);
    }
    
    protected BrooklynMementoRawData assertTransformIsNoop(BrooklynMementoRawData origData, Function<? super BrooklynMementoRawData, ? extends BrooklynMementoRawData> origDataTweaker) throws Exception {
        BrooklynMementoRawData origDataTweaked = origDataTweaker.apply(origData);
        BrooklynMementoRawData transformedData = transformRawData(origDataTweaked);
        assertRawData(transformedData, origDataTweaked);
        return transformedData;
    }
    
    protected BrooklynMementoRawData assertTransformDeletes(Deletions deletions) throws Exception {
        return assertTransformDeletes(deletions, Functions.<BrooklynMementoRawData>identity());
    }

    protected BrooklynMementoRawData assertTransformDeletes(Deletions deletions, BrooklynMementoRawData origData) throws Exception {
        return assertTransformDeletes(deletions, origData, Functions.<BrooklynMementoRawData>identity());
    }

    protected BrooklynMementoRawData assertTransformDeletes(Deletions deletions, Function<? super BrooklynMementoRawData, ? extends BrooklynMementoRawData> origDataTweaker) throws Exception {
        return assertTransformDeletes(deletions, getRawData(), origDataTweaker);
    }
    
    protected BrooklynMementoRawData assertTransformDeletes(Deletions deletions, BrooklynMementoRawData origData, Function<? super BrooklynMementoRawData, ? extends BrooklynMementoRawData> origDataTweaker) throws Exception {
        BrooklynMementoRawData origDataTweaked = origDataTweaker.apply(origData);
        BrooklynMementoRawData transformedData = transformRawData(origDataTweaked);
        assertRawData(transformedData, origDataTweaked, deletions);
        return transformedData;
    }

    protected BrooklynMementoRawData getRawData() throws Exception {
        RebindTestUtils.waitForPersisted(origApp);
        return mgmt().getRebindManager().retrieveMementoRawData();
    }

    protected BrooklynMementoRawData transformRawData(BrooklynMementoRawData rawData) throws Exception {
        DeleteOrphanedStateTransformer transformer = DeleteOrphanedStateTransformer.builder().build();
        return transformer.transform(rawData);
    }

    protected void assertRawData(BrooklynMementoRawData actual, BrooklynMementoRawData expected) {
        // asserting lots of times, to ensure we get a more specific error message!
        assertEquals(actual.getCatalogItems().keySet(), expected.getCatalogItems().keySet(), "catalog ids differ");
        assertEquals(actual.getCatalogItems(), expected.getCatalogItems());
        assertEquals(actual.getEntities().keySet(), expected.getEntities().keySet(), "entity ids differ");
        assertEquals(actual.getEntities(), expected.getEntities());
        assertEquals(actual.getLocations().keySet(), expected.getLocations().keySet(), "location ids differ");
        assertEquals(actual.getLocations(), expected.getLocations());
        assertEquals(actual.getEnrichers().keySet(), expected.getEnrichers().keySet(), "enricher ids differ");
        assertEquals(actual.getEnrichers(), expected.getEnrichers());
        assertEquals(actual.getPolicies().keySet(), expected.getPolicies().keySet(), "policy ids differ");
        assertEquals(actual.getPolicies(), expected.getPolicies());
        assertEquals(actual.getFeeds().keySet(), expected.getFeeds().keySet(), "feed ids differ");
        assertEquals(actual.getFeeds(), expected.getFeeds());
        assertTrue(EqualsBuilder.reflectionEquals(actual, expected));
    }
    
    protected void assertRawData(BrooklynMementoRawData actual, BrooklynMementoRawData orig, Deletions deletions) {
        BrooklynMementoRawData expected = new MementoTweaker(deletions).apply(orig);
        assertRawData(actual, expected);
        
        // double-check that the orig data contains what we think we are deleting!
        // otherwise we could get a lot of false positive tests.
        assertTrue(orig.getEntities().keySet().containsAll(deletions.entities));
        assertTrue(orig.getLocations().keySet().containsAll(deletions.locations));
        assertTrue(orig.getEnrichers().keySet().containsAll(deletions.enrichers));
        assertTrue(orig.getPolicies().keySet().containsAll(deletions.policies));
        assertTrue(orig.getFeeds().keySet().containsAll(deletions.feeds));
    }
    
    protected static class Deletions {
        final Set<String> entities = Sets.newLinkedHashSet();
        final Set<String> locations = Sets.newLinkedHashSet();
        final Set<String> feeds = Sets.newLinkedHashSet();
        final Set<String> enrichers = Sets.newLinkedHashSet();
        final Set<String> policies = Sets.newLinkedHashSet();
        
        protected Deletions entities(String... vals) {
            if (vals != null) entities.addAll(Arrays.asList(vals));
            return this;
        }
        protected Deletions locations(String... vals) {
            if (vals != null) locations.addAll(Arrays.asList(vals));
            return this;
        }
        protected Deletions locations(Iterable<String> vals) {
            if (vals != null) Iterables.addAll(locations, vals);
            return this;
        }
        protected Deletions feeds(String... vals) {
            if (vals != null) feeds.addAll(Arrays.asList(vals));
            return this;
        }
        protected Deletions enrichers(String... vals) {
            if (vals != null) enrichers.addAll(Arrays.asList(vals));
            return this;
        }
        protected Deletions policies(String... vals) {
            if (vals != null) policies.addAll(Arrays.asList(vals));
            return this;
        }
    }
    
    public static class MyEnricher extends AbstractEnricher {
        @SetFromFlag("myobj") Object obj;
    }
    
    public static class MyPolicy extends AbstractPolicy {
    }
    
    public static class MyFeed extends AbstractFeed {
    }
    
    public static class MyEntity extends TestEntityImpl {
        @Override protected void initEnrichers() {
            // no stock enrichers
        }
    }
}
