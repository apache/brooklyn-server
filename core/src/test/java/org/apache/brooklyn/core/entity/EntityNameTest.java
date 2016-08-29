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
package org.apache.brooklyn.core.entity;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.factory.ApplicationBuilder;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.testng.annotations.Test;

public class EntityNameTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testDisplayNameWhenNothingSupplied() {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class));
        assertEquals(entity.getDisplayName(), "TestEntity:"+entity.getId().substring(0, 4));
    }
    
    @Test
    public void testExplicitDisplayName() {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .displayName("myDisplayName"));
        assertEquals(entity.getDisplayName(), "myDisplayName");
    }
    
    @Test
    public void testExplicitDefaultDisplayName() {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .configure(AbstractEntity.DEFAULT_DISPLAY_NAME, "myDefaultName"));
        assertEquals(entity.getDisplayName(), "myDefaultName");
    }
    
    @Test
    public void testExplicitDefaultDisplayNameOverriddenByRealName() {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .configure(AbstractEntity.DEFAULT_DISPLAY_NAME, "myDefaultName")
                .displayName("myDisplayName"));
        assertEquals(entity.getDisplayName(), "myDisplayName");
    }
    
    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testDefaultDisplayNameUsesDeferredSupplier() {
        EntitySpec<TestEntity> spec = EntitySpec.create(TestEntity.class);
        spec.configure((ConfigKey)AbstractEntity.DEFAULT_DISPLAY_NAME, new DeferredSupplier<String>() {
            @Override public String get() {
                return "myDefaultName";
            }});
        TestEntity entity = app.addChild(spec);
        assertEquals(entity.getDisplayName(), "myDefaultName");
    }

    @Test
    public void testAppUsesDefaultDisplayName() {
        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class)
                .configure(AbstractApplication.DEFAULT_DISPLAY_NAME, "myDefaultName");
        TestApplication app2 = ApplicationBuilder.newManagedApp(appSpec, mgmt);

        assertEquals(app2.getDisplayName(), "myDefaultName");
    }

    @Test
    public void testAppUsesEntityName() {
        EntitySpec<BasicApplication> appSpec = EntitySpec.create(BasicApplication.class)
                .configure(AbstractApplication.DEFAULT_DISPLAY_NAME, "myDefaultName").child(EntitySpec.create(TestApplication.class));
        BasicApplication app2 = ApplicationBuilder.newManagedApp(appSpec, mgmt);

        assertEquals(app2.getDisplayName(), "myDefaultName");
        Entity childEntity = Iterables.getFirst(app2.getChildren(), null);
        assertEquals(childEntity.getDisplayName(), TestApplication.class.getSimpleName() + ":" + childEntity.getId().substring(0,4));
    }

    @Test
    public void testAppUsesDisplayNameOverDefaultName() {
        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class)
                .displayName("myName")
                .configure(AbstractApplication.DEFAULT_DISPLAY_NAME, "myDefaultName");
        TestApplication app2 = ApplicationBuilder.newManagedApp(appSpec, mgmt);
        
        assertEquals(app2.getDisplayName(), "myName");
    }
}
