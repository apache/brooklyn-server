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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.mgmt.internal.LocalEntityManager;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeployFailureTest extends BrooklynAppUnitTestSupport {

    private LocalEntityManager entityManager;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entityManager = (LocalEntityManager) mgmt.getEntityManager();
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
            Asserts.expectedFailureContains(e, "cannot be cast", "WrongParentEntity");
        }

        // This should continue to work, after the failed entity-deploy above
        // See https://issues.apache.org/jira/browse/BROOKLYN-599
        entityManager.getAllEntitiesInApplication(app);
    }

    public static class TestEntityFailingGetParentImpl extends TestEntityImpl {
        private static interface WrongParentEntity extends Entity {}
        
        @Override
        public WrongParentEntity getParent() {
            return (WrongParentEntity) super.getParent();
        }
    }
}
