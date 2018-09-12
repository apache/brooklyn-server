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
package org.apache.brooklyn.core.mgmt;

import static org.testng.Assert.assertFalse;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConstraintViolationException;
import org.apache.brooklyn.core.mgmt.internal.LocalEntityManager;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;

public class DeployFailureTest extends BrooklynAppUnitTestSupport {

    private static final Set<Entity> constructedEntities = Collections.synchronizedSet(new LinkedHashSet<>());
    
    private LocalEntityManager entityManager;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        constructedEntities.clear();
        super.setUp();
        entityManager = (LocalEntityManager) mgmt.getEntityManager();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            constructedEntities.clear();
        }
    }
    
    @Test
    public void testConfigConstraintViolation() throws Exception {
        try {
            mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class)
                    .child(EntitySpec.create(TestEntityWithConfigConstraint.class)));
            Asserts.shouldHaveFailedPreviously();
        } catch (ConstraintViolationException e) {
            // Check gives nice exception
            Asserts.expectedFailureContains(e, "myNonNullKey", "Predicates.notNull()");
        }

        // Should have disposed of entities that failed to be created
        assertEntitiesNotKnown(constructedEntities);
    }

    @Test
    public void testFailedInit() throws Exception {
        try {
            mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class)
                    .child(EntitySpec.create(TestEntity.class)
                            .impl(TestEntityFailingInitImpl.class)));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            // Check gives nice exception
            Asserts.expectedFailureContains(e, "Simulating failure in entity init");
        }

        // Should have disposed of entities that failed to be created
        assertEntitiesNotKnown(constructedEntities);
    }

    @Test
    public void testFailedGetParent() throws Exception {
        entityManager.getAllEntitiesInApplication(app);

        try {
            mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class)
                    .child(EntitySpec.create(TestEntity.class)
                            .impl(TestEntityFailingGetParentImpl.class)));
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassCastException e) {
            // TODO Should give nicer exception
            Asserts.expectedFailureContains(e, "cannot be cast", "WrongParentEntity");
        }

        // This should continue to work, after the failed entity-deploy above
        // See https://issues.apache.org/jira/browse/BROOKLYN-599
        entityManager.getAllEntitiesInApplication(app);

        // Should have disposed of entities that failed to be created
        assertEntitiesNotKnown(constructedEntities);
    }

    private void assertEntitiesNotKnown(Iterable<Entity> entities) {
        for (Entity entity : constructedEntities) {
            if (entity.getId() != null) {
                assertFalse(entityManager.isKnownEntityId(entity.getId()), "entity="+entity);
            }
        }
    }

    public static class TestEntityRecordingConstructionImpl extends TestEntityImpl {
        public TestEntityRecordingConstructionImpl() {
            constructedEntities.add(this);
        }
    }
    
    @ImplementedBy(TestEntityWithConfigConstraintImpl.class)
    public static interface TestEntityWithConfigConstraint extends TestEntity {
        ConfigKey<String> NON_NULL_KEY = ConfigKeys.builder(String.class)
                .name("myNonNullKey")
                .constraint(Predicates.notNull())
                .build();
    }
    
    public static class TestEntityWithConfigConstraintImpl extends TestEntityRecordingConstructionImpl implements TestEntityWithConfigConstraint {
    }
    
    public static class TestEntityFailingInitImpl extends TestEntityRecordingConstructionImpl {
        @Override
        public void init() {
            throw new RuntimeException("Simulating failure in entity init()");
        }
    }
    
    public static class TestEntityFailingGetParentImpl extends TestEntityRecordingConstructionImpl {
        private static interface WrongParentEntity extends Entity {}
        
        @Override
        public WrongParentEntity getParent() {
            return (WrongParentEntity) super.getParent();
        }
    }
}
