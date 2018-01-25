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

import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.location.cloud.AvailabilityZoneExtension;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.DynamicCluster.ZoneFailureDetector;
import org.apache.brooklyn.entity.group.DynamicClusterWithAvailabilityZonesTest.SimulatedAvailabilityZoneExtension;
import org.apache.brooklyn.entity.group.zoneaware.ProportionalZoneFailureDetector;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

public class DynamicClusterWithAvailabilityZonesRebindTest extends RebindTestFixtureWithApp {
    
    // Previously, when there was a mutable default value for ZONE_FAILURE_DETECTOR, the second cluster
    // would include failure info for the first cluster's zone-locations. When these were unmanaged,
    // the zone-locations would become dangling referenes within the ZoneFailureDetector. This caused
    // NPE on rebind.
    //
    // Config key persistence was then changed so that statically defined keys (such as ZONE_FAILURE_DETECTOR,
    // declared on the DynamicCluster class) would not be persisted. This meant that the ZoneFailureDetector
    // state was no longer persisted for either location. The NPE on rebind went away, but the config key
    // default value instance was still shared between all clusters.
    //
    // Now the detector has a null default value. We implicitly set a transient field for the zoneFailureDetector
    // so if using defaults then nothing is persisted. But if a custom ZoneFailureDetector is supplied, then
    // that will be persisted and used.
    @Test
    public void testRebindWithDefaultZoneFailureDetector() throws Exception {
        // Start and then unmanage a cluster (so it's detector was populated).
        // Do this in case the ZoneFailureDetector is shared!
        SimulatedLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SimulatedLocationWithZoneExtension.class)
                .configure(SimulatedLocationWithZoneExtension.ZONE_NAMES, ImmutableList.of("zone1", "zone2"))
                .configure(SimulatedLocationWithZoneExtension.ZONE_FAIL_CONDITIONS, ImmutableMap.of("zone1", Predicates.alwaysTrue())));

        DynamicCluster cluster = app().addChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.ENABLE_AVAILABILITY_ZONES, true)
                .configure(DynamicCluster.INITIAL_SIZE, 2)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));
        
        cluster.start(ImmutableList.of(loc));
        
        Entities.unmanage(cluster);
        Locations.unmanage(loc);

        // Start a second cluster
        SimulatedLocation locUnrelated = mgmt().getLocationManager().createLocation(LocationSpec.create(SimulatedLocationWithZoneExtension.class)
                .configure(SimulatedLocationWithZoneExtension.ZONE_NAMES, ImmutableList.of("zone3", "zone4"))
                .configure(SimulatedLocationWithZoneExtension.ZONE_FAIL_CONDITIONS, ImmutableMap.of("zone3", Predicates.alwaysTrue())));
        
        DynamicCluster clusterUnrelated = app().addChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.ENABLE_AVAILABILITY_ZONES, true)
                .configure(DynamicCluster.INITIAL_SIZE, 2)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));
        clusterUnrelated.start(ImmutableList.of(locUnrelated));
        
        rebind();
        
        // Confirm that cluster is usable
        DynamicCluster newClusterUnrelated = (DynamicCluster) mgmt().getEntityManager().getEntity(clusterUnrelated.getId());
        newClusterUnrelated.resize(4);
    }
    
    @Test
    public void testRebindWithCustomZoneFailureDetector() throws Exception {
        // Start and then unmanage a cluster (so it's detector was populated).
        // Do this in case the ZoneFailureDetector is shared!
        SimulatedLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SimulatedLocationWithZoneExtension.class)
                .configure(SimulatedLocationWithZoneExtension.ZONE_NAMES, ImmutableList.of("zone1", "zone2"))
                .configure(SimulatedLocationWithZoneExtension.ZONE_FAIL_CONDITIONS, ImmutableMap.of("zone1", Predicates.alwaysTrue())));

        DynamicCluster cluster = app().addChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.ENABLE_AVAILABILITY_ZONES, true)
                .configure(DynamicCluster.INITIAL_SIZE, 2)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class))
                .configure(DynamicCluster.ZONE_FAILURE_DETECTOR, new CustomZoneFailureDetector(2, Duration.ONE_HOUR, 0.9)));
        
        cluster.start(ImmutableList.of(loc));
        ZoneFailureDetector detector = ((DynamicClusterImpl)Entities.deproxy(cluster)).getZoneFailureDetector();
        assertTrue(detector instanceof CustomZoneFailureDetector, "detector="+detector);
        
        rebind();
        
        // Confirm that has custom detector, and is usable
        DynamicCluster newCluster = (DynamicCluster) mgmt().getEntityManager().getEntity(cluster.getId());
        ZoneFailureDetector newDetector = ((DynamicClusterImpl)Entities.deproxy(newCluster)).getZoneFailureDetector();
        assertTrue(newDetector instanceof CustomZoneFailureDetector, "detector="+newDetector);
        
        cluster.resize(4);
    }
    
    public static class CustomZoneFailureDetector extends ProportionalZoneFailureDetector {
        public CustomZoneFailureDetector(int minDatapoints, Duration timeToConsider, double maxProportionFailures) {
            super(minDatapoints, timeToConsider, maxProportionFailures);
        }
    }
    
    public static class SimulatedLocationWithZoneExtension extends SimulatedLocation {
        @SuppressWarnings("serial")
        public static final ConfigKey<List<String>> ZONE_NAMES = ConfigKeys.newConfigKey(
                new TypeToken<List<String>>() {},
                "zoneNames");
        
        @SuppressWarnings("serial")
        public static final ConfigKey<Map<String, ? extends Predicate<? super SimulatedLocation>>> ZONE_FAIL_CONDITIONS = ConfigKeys.newConfigKey(
                new TypeToken<Map<String, ? extends Predicate<? super SimulatedLocation>>>() {},
                "zoneFailConditions");
        
        @Override
        public void init() {
            super.init();
            addExtension(AvailabilityZoneExtension.class, new SimulatedAvailabilityZoneExtension(getManagementContext(), this, 
                    config().get(ZONE_NAMES), config().get(ZONE_FAIL_CONDITIONS)));
        }
    }
}
