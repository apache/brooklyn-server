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

package org.apache.brooklyn.location.jclouds;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Collection;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class BasicJcloudsLocationCustomizerTest extends BrooklynAppUnitTestSupport {

    private BasicJcloudsLocationCustomizer testCustomizer;

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testCustomizer = new BasicJcloudsLocationCustomizer();
    }

    @Test
    public void testCustomiserIncluded() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .addInitializer(testCustomizer));
        assertCustomizers(entity, 1);
    }

    @Test
    public void testCustomizersMerged() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .addInitializer(testCustomizer)
                .configure(
                        BrooklynConfigKeys.PROVISIONING_PROPERTIES.subKey(JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZERS.getName()),
                        ImmutableList.of(new BasicJcloudsLocationCustomizer())));
        assertCustomizers(entity, 2);
    }

    @Test
    public void testInitialiserAppliedToMembersOfCluster() {
        final EntitySpec<TestEntity> memberSpec = EntitySpec.create(TestEntity.class)
                .addInitializer(testCustomizer);
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.INITIAL_SIZE, 3)
                .configure(DynamicCluster.MEMBER_SPEC, memberSpec));
        app.start(ImmutableList.of(app.newSimulatedLocation()));
        for (Entity member : cluster.getMembers()) {
            assertCustomizers(member, 1);
        }
    }

    @Test
    public void testApplyToEntity() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        assertNoCustomizers(entity);
        testCustomizer.apply(entity);
        assertCustomizers(entity, 1);
    }

    @SuppressWarnings("unchecked")
    private void assertCustomizers(Entity entity, int numberCustomizers) {
        Object object = entity.config().get(BrooklynConfigKeys.PROVISIONING_PROPERTIES.subKey(
                JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZERS.getName()));
        assertNotNull(object, "expected value for customizers in " + entity.config().get(BrooklynConfigKeys.PROVISIONING_PROPERTIES));
        assertTrue(object instanceof Collection, "expected collection, got " + object.getClass());
        Collection<JcloudsLocationCustomizer> customizers = (Collection<JcloudsLocationCustomizer>) object;
        assertEquals(customizers.size(), numberCustomizers,
                "expected " + numberCustomizers + " customizer" + Strings.s(numberCustomizers) + " in " + Iterables.toString(customizers));
        assertTrue(customizers.contains(testCustomizer), "expected to find testCustomizer in " + Iterables.toString(customizers));
    }

    @SuppressWarnings("unchecked")
    private void assertNoCustomizers(Entity entity) {
        Object object = entity.config().get(BrooklynConfigKeys.PROVISIONING_PROPERTIES.subKey(
                JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZERS.getName()));
        if (object != null) {
            assertTrue(object instanceof Collection, "expected collection, got " + object.getClass());
            Collection<JcloudsLocationCustomizer> customizers = (Collection<JcloudsLocationCustomizer>) object;
            assertEquals(customizers.size(), 0, "expected no entries in " + Iterables.toString(customizers));
        }
    }

}
