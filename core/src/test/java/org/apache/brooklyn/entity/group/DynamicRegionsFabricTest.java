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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DynamicRegionsFabricTest extends BrooklynAppUnitTestSupport {

    DynamicRegionsFabric fabric;
    private Location loc1;
    private Location loc2;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc1 = new SimulatedLocation();
        loc2 = new SimulatedLocation();
        
        fabric = app.createAndManageChild(EntitySpec.create(DynamicRegionsFabric.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));
    }

    @Test
    public void testUsesInitialLocationsFromStartEffectorArgs() throws Exception {
        app.start(ImmutableList.of(loc1, loc2));

        assertFabricChildren(fabric, 2, ImmutableList.of(loc1, loc2));
    }
    
    @Test
    @SuppressWarnings("deprecation")
    public void testUsesInitialLocationsFromAppSpec() throws Exception {
        TestApplication app2 = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
            .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
            .child(EntitySpec.create(DynamicRegionsFabric.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)))
            .locations(ImmutableList.of(loc1, loc2)));
        DynamicRegionsFabric fabric2 = (DynamicRegionsFabric) Iterables.getOnlyElement(app2.getChildren());
        app2.start(ImmutableList.<Location>of());
        
        assertFabricChildren(fabric2, 2, ImmutableList.of(loc1, loc2));
    }
    
    @Test
    @SuppressWarnings("deprecation")
    public void testUsesInitialLocationsFromEntitySpec() throws Exception {
        TestApplication app2 = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
            .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
            .child(EntitySpec.create(DynamicRegionsFabric.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .locations(ImmutableList.of(loc1, loc2))));
        DynamicRegionsFabric fabric2 = (DynamicRegionsFabric) Iterables.getOnlyElement(app2.getChildren());
        app2.start(ImmutableList.<Location>of());
        
        assertFabricChildren(fabric2, 2, ImmutableList.of(loc1, loc2));
    }
    
    @Test
    public void testUsesInitialLocationsForExistingChildCreatingExtras() throws Exception {
        // Expect existing child to be used for one location, and a second child to be created 
        // automatically for the second location
        runUsesInitialLocationsForExistingChildren(1);
    }

    @Test
    public void testUsesInitialLocationsForExistingChildren() throws Exception {
        // Expect the two existing children to be used for the two locations; no additional
        // children should be created automatically.
        runUsesInitialLocationsForExistingChildren(2);
    }

    @Test
    public void testUsesInitialLocationsRoundRobinForExistingChildren() throws Exception {
        // Expect the two locations to be used round-robin for the three existing children;
        // no additional children should be created automatically.
        runUsesInitialLocationsForExistingChildren(3);
    }

    @Test
    public void testIgnoredNonStartableChildren() throws Exception {
        // Add a basic entity; this will not count as a "member" because it is not startable.
        // Therefore expect two new children to be created.
        BasicEntity existingChild = fabric.addChild(EntitySpec.create(BasicEntity.class));
        app.start(ImmutableList.of(loc1, loc2));
        assertFabricMembers(fabric, 2, ImmutableList.of(loc1, loc2));
        assertFalse(fabric.getMembers().contains(existingChild));
    }

    @Test
    public void testTurnOffIncludeInitialChildren() throws Exception {
        // Turning off "include initial children" means they will be ignored in the member-count.
        fabric.config().set(DynamicFabric.INCLUDE_INITIAL_CHILDREN, false);
        TestEntity existingChild = fabric.addChild(EntitySpec.create(TestEntity.class));
        app.start(ImmutableList.of(loc1, loc2));
        assertFabricMembers(fabric, 2, ImmutableList.of(loc1, loc2));
        
        assertFalse(fabric.getMembers().contains(existingChild));
        assertEqualsIgnoringOrder(existingChild.getLocations(), ImmutableList.of(loc1));
    }

    protected void runUsesInitialLocationsForExistingChildren(int numChildren) throws Exception {
        List<TestEntity> existingChildren = Lists.newArrayList();
        for (int i = 0; i < numChildren; i++) {
            existingChildren.add(fabric.addChild(EntitySpec.create(TestEntity.class)));
        }
        app.start(ImmutableList.of(loc1, loc2));

        int expectedNumChildren = Math.max(numChildren, 2);
        Iterable<Location> expectedChildLocations = Iterables.limit(Iterables.cycle(ImmutableList.of(loc1, loc2)), expectedNumChildren);
        
        assertFabricChildren(fabric, expectedNumChildren, ImmutableList.copyOf(expectedChildLocations));
        assertTrue(fabric.getChildren().containsAll(existingChildren));
    }

    @Test
    public void testDynamicallyAddLocations() throws Exception {
        app.start(ImmutableList.of(loc1));
        Set<Entity> initialChildren = ImmutableSet.copyOf(fabric.getChildren());
        Collection<Location> initialLocations = fabric.getLocations();
        assertEquals(initialChildren.size(), 1, "children="+initialChildren);
        assertEqualsIgnoringOrder(initialLocations, ImmutableSet.of(loc1));

        fabric.addRegion("localhost:(name=newloc1)");
        
        Set<Entity> newChildren = Sets.difference(ImmutableSet.copyOf(fabric.getChildren()), initialChildren);
        Collection<Location> newLocations = Iterables.getOnlyElement(newChildren).getLocations();
        assertEquals(Iterables.getOnlyElement(newLocations).getDisplayName(), "newloc1");
        
        Set<Location> newLocations2 = Sets.difference(ImmutableSet.copyOf(fabric.getLocations()), ImmutableSet.copyOf(initialLocations));
        assertEquals(newLocations2, ImmutableSet.copyOf(newLocations));
    }
    
    @Test
    public void testDynamicallyRemoveInitialLocations() throws Exception {
        app.start(ImmutableList.of(loc1, loc2));
        Set<Entity> initialChildren = ImmutableSet.copyOf(fabric.getChildren());

        Entity childToRemove1 = Iterables.get(initialChildren, 0);
        Entity childToRemove2 = Iterables.get(initialChildren, 1);
        
        // remove first child (leaving one)
        fabric.removeRegion(childToRemove1.getId());
        
        Set<Entity> removedChildren = Sets.difference(initialChildren, ImmutableSet.copyOf(fabric.getChildren()));
        Set<Entity> removedMembers = Sets.difference(initialChildren, ImmutableSet.copyOf(fabric.getMembers()));

        assertEqualsIgnoringOrder(removedChildren, ImmutableSet.of(childToRemove1));
        assertEqualsIgnoringOrder(removedMembers, ImmutableSet.of(childToRemove1));
        assertEqualsIgnoringOrder(fabric.getLocations(), childToRemove2.getLocations());
        
        // remove second child (leaving none)
        fabric.removeRegion(childToRemove2.getId());
        
        assertEqualsIgnoringOrder(fabric.getChildren(), ImmutableSet.of());
        assertEqualsIgnoringOrder(fabric.getMembers(), ImmutableSet.of());
        assertEqualsIgnoringOrder(fabric.getLocations(), ImmutableSet.of());
    }
    
    @Test
    public void testDynamicallyRemoveDynamicallyAddedLocation() throws Exception {
        app.start(ImmutableList.of(loc1));
        Set<Entity> initialChildren = ImmutableSet.copyOf(fabric.getChildren());
        Collection<Location> initialLocations = fabric.getLocations();
        assertEquals(initialChildren.size(), 1, "children="+initialChildren);
        assertEqualsIgnoringOrder(initialLocations, ImmutableSet.of(loc1));

        fabric.addRegion("localhost:(name=newloc1)");
        Entity childToRemove = Iterables.getOnlyElement(Sets.difference(ImmutableSet.copyOf(fabric.getChildren()), initialChildren));
        
        // remove dynamically added child
        fabric.removeRegion(childToRemove.getId());
        
        assertEqualsIgnoringOrder(fabric.getChildren(), initialChildren);
        assertEqualsIgnoringOrder(fabric.getLocations(), initialLocations);
    }
    
    @Test
    public void testRemoveRegionWithNonChild() throws Exception {
        app.start(ImmutableList.of(loc1));

        try {
            fabric.removeRegion(app.getId());
        } catch (Exception e) {
            IllegalArgumentException cause = Exceptions.getFirstThrowableOfType(e, IllegalArgumentException.class);
            if (cause == null && !e.toString().contains("Wrong parent")) throw e;
        }
    }
    
    @Test
    public void testRemoveRegionWithNonEntityId() throws Exception {
        app.start(ImmutableList.of(loc1));

        try {
            fabric.removeRegion("thisIsNotAnEntityId");
        } catch (Exception e) {
            IllegalArgumentException cause = Exceptions.getFirstThrowableOfType(e, IllegalArgumentException.class);
            if (cause == null && !e.toString().contains("No entity found")) throw e;
        }
    }
    
    private List<Location> getLocationsOfChildren(DynamicRegionsFabric fabric) {
        return getLocationsOf(fabric.getChildren());
    }

    private List<Location> getLocationsOfMembers(DynamicRegionsFabric fabric) {
        return getLocationsOf(fabric.getMembers());
    }

    private List<Location> getLocationsOf(Iterable<? extends Entity> entities) {
        List<Location> result = Lists.newArrayList();
        for (Entity entity : entities) {
            result.addAll(entity.getLocations());
        }
        return result;
    }

    private void assertEqualsIgnoringOrder(Iterable<? extends Object> col1, Iterable<? extends Object> col2) {
        assertEquals(Iterables.size(col1), Iterables.size(col2), "col2="+col1+"; col2="+col2);
        assertEquals(MutableSet.copyOf(col1), MutableSet.copyOf(col2), "col1="+col1+"; col2="+col2);
    }
    
    private void assertFabricChildren(DynamicRegionsFabric fabric, int expectedSize, List<? extends Location> expectedLocs) {
        assertEquals(fabric.getChildren().size(), expectedSize, "children="+fabric.getChildren());
        assertEquals(fabric.getMembers().size(), expectedSize, "members="+fabric.getMembers());
        assertEqualsIgnoringOrder(fabric.getChildren(), fabric.getMembers());
        assertEqualsIgnoringOrder(getLocationsOfChildren(fabric), expectedLocs);
    }
    
    private void assertFabricMembers(DynamicRegionsFabric fabric, int expectedSize, List<? extends Location> expectedLocs) {
        assertEquals(fabric.getMembers().size(), expectedSize, "members="+fabric.getMembers());
        assertTrue(fabric.getChildren().containsAll(fabric.getMembers()));
        assertEqualsIgnoringOrder(getLocationsOfMembers(fabric), expectedLocs);
    }
}
