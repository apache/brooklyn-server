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

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.Test;

public class EntitySetFromFlagTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testSetIconUrl() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("iconUrl", "/img/myicon.gif"));
        assertEquals(entity.getIconUrl(), "/img/myicon.gif");
    }

    private MyEntity newDeproxiedEntity(Map<?, ?> config) {
        Entity result = app.addChild(EntitySpec.create(Entity.class).impl(MyEntity.class)
                .configure(config));
        return (MyEntity) Entities.deproxy(result);
    }
    
    public static class MyEntity extends AbstractEntity {
    }
}
