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

import static org.apache.brooklyn.test.Asserts.assertEqualsIgnoringOrder;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class SequenceGroupTest extends BrooklynAppUnitTestSupport {
    
    private TestApplication app;
    private SequenceGroup group;
    private TestEntity e1;
    private TestEntity e2;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        app = TestApplication.Factory.newManagedInstanceForTests();
        group = app.createAndManageChild(EntitySpec.create(SequenceGroup.class));
        e1 = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        e2 = app.createAndManageChild(EntitySpec.create(TestEntity.class));
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        if (app != null) Entities.destroyAll(app.getManagementContext());

        super.tearDown();
    }

    @Test
    public void testGroupWithMatchingFilterReturnsOnlyMatchingMembers() throws Exception {
        group.setEntityFilter(EntityPredicates.idEqualTo(e1.getId()));

        assertEqualsIgnoringOrder(group.getMembers(), ImmutableList.of(e1));
        EntityAsserts.assertAttributeEquals(e1, SequenceGroup.SEQUENCE_VALUE, 1);
        EntityAsserts.assertAttributeEquals(group, SequenceGroup.SEQUENCE_NEXT, 2);
    }

    @Test
    public void testGroupWithMatchingFilterReturnsEverythingThatMatches() throws Exception {
        group.setEntityFilter(Predicates.alwaysTrue());

        assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e1, e2, app, group));
        EntityAsserts.assertAttributeEquals(app, SequenceGroup.SEQUENCE_VALUE, 1);
        EntityAsserts.assertAttributeEquals(group, SequenceGroup.SEQUENCE_VALUE, 2);
        EntityAsserts.assertAttributeEquals(e1, SequenceGroup.SEQUENCE_VALUE, 3);
        EntityAsserts.assertAttributeEquals(e2, SequenceGroup.SEQUENCE_VALUE, 4);
        EntityAsserts.assertAttributeEquals(group, SequenceGroup.SEQUENCE_NEXT, 5);
    }
    
    @Test
    public void testGroupDetectsNewlyManagedMatchingMember() throws Exception {
        group.setEntityFilter(EntityPredicates.displayNameEqualTo("myname"));
        final Entity e3 = app.addChild(EntitySpec.create(TestEntity.class).displayName("myname"));
        
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e3));
                EntityAsserts.assertAttributeEquals(e3, SequenceGroup.SEQUENCE_VALUE, 1);
                EntityAsserts.assertAttributeEquals(group, SequenceGroup.SEQUENCE_NEXT, 2);
            }});
    }

    @Test
    public void testGroupUsesNewFilter() throws Exception {
        group.setEntityFilter(EntityPredicates.hasInterfaceMatching(".*TestEntity"));

        assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e1, e2));
        EntityAsserts.assertAttributeEquals(e1, SequenceGroup.SEQUENCE_VALUE, 1);
        EntityAsserts.assertAttributeEquals(e2, SequenceGroup.SEQUENCE_VALUE, 2);
        EntityAsserts.assertAttributeEquals(group, SequenceGroup.SEQUENCE_NEXT, 3);

        final Entity e3 = app.addChild(EntitySpec.create(TestEntity.class));

        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e1, e2, e3));
                EntityAsserts.assertAttributeEquals(e3, SequenceGroup.SEQUENCE_VALUE, 3);
                EntityAsserts.assertAttributeEquals(group, SequenceGroup.SEQUENCE_NEXT, 4);
            }});
    }
}
