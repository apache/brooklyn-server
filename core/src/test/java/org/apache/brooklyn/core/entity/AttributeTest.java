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
import static org.testng.Assert.assertNull;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AttributeTest extends BrooklynAppUnitTestSupport {
    static AttributeSensor<String> COLOR = new BasicAttributeSensor<String>(String.class, "my.color");

    TestEntity entity;
    TestEntityImpl entityImpl;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.addChild(EntitySpec.create(TestEntity.class));
        entityImpl = (TestEntityImpl) Entities.deproxy(entity);
    }

    @Test
    public void canGetAndSetAttribute() {
        entity.sensors().set(COLOR, "red");
        assertEquals(entity.getAttribute(COLOR), "red");
    }
    
    @Test
    public void missingAttributeIsNull() {
        assertEquals(entity.getAttribute(COLOR), null);
    }
    
    @Test
    public void canGetAttributeByNameParts() {
        // Initially null
        assertNull(entityImpl.getAttributeByNameParts(COLOR.getNameParts()));
        
        // Once set, returns val
        entity.sensors().set(COLOR, "red");
        assertEquals(entityImpl.getAttributeByNameParts(COLOR.getNameParts()), "red");
    }
}
