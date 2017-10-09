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

import static org.testng.Assert.assertEquals
import static org.testng.Assert.assertFalse
import static org.testng.Assert.assertTrue

import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.Entities
import org.apache.brooklyn.core.entity.EntityConfigTest.MyOtherEntity
import org.apache.brooklyn.core.entity.EntityConfigTest.MySubEntity
import org.testng.annotations.AfterMethod
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test

import com.google.common.base.Predicate;

public class EntityConfigGroovyTest extends BrooklynAppUnitTestSupport {

    private MySubEntity entity;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() {
        super.setUp();
        entity = app.addChild(EntitySpec.create(MySubEntity.class));
    }

    @Test
    public void testGetConfigOfTypeClosureReturnsClosure() throws Exception {
        MyOtherEntity entity2 = app.addChild(EntitySpec.create(MyOtherEntity.class));
        entity2.config().set(MyOtherEntity.CLOSURE_KEY, { return "abc" } );
        
        Closure configVal = entity2.getConfig(MyOtherEntity.CLOSURE_KEY);
        assertTrue(configVal instanceof Closure, "configVal="+configVal);
        assertEquals(configVal.call(), "abc");
    }

    @Test
    public void testGetConfigOfPredicateTaskReturnsCoercedClosure() throws Exception {
        MyOtherEntity entity2 = app.addChild(EntitySpec.create(MyOtherEntity.class));
        entity2.config().set(MyOtherEntity.PREDICATE_KEY, { return it != null } );

        Predicate<?> predicate = entity2.getConfig(MyOtherEntity.PREDICATE_KEY);
        assertTrue(predicate instanceof Predicate, "predicate="+predicate);
        assertTrue(predicate.apply(1));
        assertFalse(predicate.apply(null));
    }
}
