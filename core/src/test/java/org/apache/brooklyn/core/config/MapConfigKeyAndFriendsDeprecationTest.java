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
package org.apache.brooklyn.core.config;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class MapConfigKeyAndFriendsDeprecationTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testUsingDeprecatedName() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MyEntity.class)
                .configure("oldConfMapDeepMerge", ImmutableMap.of("mykey", "myval")));
        assertEquals(entity.config().get(MyEntity.CONF_MAP_DEEP_MERGE), ImmutableMap.of("mykey", "myval"));
    }

    @Test
    public void testUsingDeprecatedNameSubkey() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MyEntity.class)
                .configure("confMapDeepMerge.mykey", "myval"));
        assertEquals(entity.config().get(MyEntity.CONF_MAP_DEEP_MERGE), ImmutableMap.of("mykey", "myval"));
    }

    @Test
    public void testPrefersNonDeprecatedName() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MyEntity.class)
                .configure("confMapDeepMerge", ImmutableMap.of("mykey", "myval"))
                .configure("oldConfMapDeepMerge", ImmutableMap.of("wrongkey", "wrongval")));
        assertEquals(entity.config().get(MyEntity.CONF_MAP_DEEP_MERGE), ImmutableMap.of("mykey", "myval"));
    }
    
    @Test
    public void testInheritsDeprecatedKeyFromRuntimeParent() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(TestEntity.class)
                .configure("oldConfMapDeepMerge", ImmutableMap.of("mykey", "myval"))
                .configure("oldConfMapNotReinherited", ImmutableMap.of("mykey", "myval")));
        EntityInternal child = entity.addChild(EntitySpec.create(MyEntity.class));
        assertEquals(child.config().get(MyEntity.CONF_MAP_DEEP_MERGE), ImmutableMap.of("mykey", "myval"));
        assertEquals(child.config().get(MyEntity.CONF_MAP_NOT_REINHERITED), ImmutableMap.of("mykey", "myval"));
    }

    @Test
    public void testMergesDeprecatedKeyFromRuntimeParent() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(TestEntity.class)
                .configure("oldConfMapDeepMerge", ImmutableMap.of("mykey", "myval")));
        EntityInternal child = entity.addChild(EntitySpec.create(MyEntity.class)
                .configure("oldConfMapDeepMerge", ImmutableMap.of("mykey2", "myval2")));
        assertEquals(child.config().get(MyEntity.CONF_MAP_DEEP_MERGE), ImmutableMap.of("mykey", "myval", "mykey2", "myval2"));
    }

    @Test
    public void testMergesDeprecatedKeyFromRuntimeParentWithOwn() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(TestEntity.class)
                .configure("oldConfMapDeepMerge", ImmutableMap.of("mykey", "myval")));
        EntityInternal child = entity.addChild(EntitySpec.create(MyEntity.class)
                .configure("confMapDeepMerge", ImmutableMap.of("mykey2", "myval2")));
        assertEquals(child.config().get(MyEntity.CONF_MAP_DEEP_MERGE), ImmutableMap.of("mykey", "myval", "mykey2", "myval2"));
    }

    @Test
    public void testMergesKeyFromRuntimeParentWithOwnDeprecated() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(TestEntity.class)
                .configure("confMapDeepMerge", ImmutableMap.of("mykey", "myval")));
        EntityInternal child = entity.addChild(EntitySpec.create(MyEntity.class)
                .configure("oldConfMapDeepMerge", ImmutableMap.of("mykey2", "myval2")));
        assertEquals(child.config().get(MyEntity.CONF_MAP_DEEP_MERGE), ImmutableMap.of("mykey", "myval", "mykey2", "myval2"));
    }
    
    @Test
    public void testDeprecatedKeyNotReinheritedIfNotSupposedToBe() throws Exception {
        EntityInternal entity = app.addChild(EntitySpec.create(MyEntity.class)
                .configure("oldConfMapNotReinherited", ImmutableMap.of("mykey", "myval")));
        EntityInternal child = entity.addChild(EntitySpec.create(MyEntity.class));
        assertEquals(entity.config().get(MyEntity.CONF_MAP_NOT_REINHERITED), ImmutableMap.of("mykey", "myval"));
        assertEquals(child.config().get(MyEntity.CONF_MAP_NOT_REINHERITED), null);
    }
    
    @ImplementedBy(MyEntityImpl.class)
    public interface MyEntity extends EntityInternal {
        MapConfigKey<String> CONF_MAP_DEEP_MERGE = new MapConfigKey.Builder<String>(String.class, "confMapDeepMerge")
                .deprecatedNames("oldConfMapDeepMerge")
                .runtimeInheritance(BasicConfigInheritance.DEEP_MERGE)
                .build();
        
        MapConfigKey<Object> CONF_MAP_NOT_REINHERITED = new MapConfigKey.Builder<Object>(Object.class, "confMapNotReinherited")
                .deprecatedNames("oldConfMapNotReinherited")
                .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED)
                .build();
        
        // FIXME Need to support deprecatedNames for ListConfigKey and SetConfigKey?
        ListConfigKey<String> CONF_LIST_THING = new ListConfigKey<String>(String.class, "test.confListThing", "Configuration key that's a list thing");
        ListConfigKey<Object> CONF_LIST_OBJ_THING = new ListConfigKey<Object>(Object.class, "test.confListObjThing", "Configuration key that's a list thing, of objects");
        SetConfigKey<String> CONF_SET_THING = new SetConfigKey<String>(String.class, "test.confSetThing", "Configuration key that's a set thing");
        SetConfigKey<Object> CONF_SET_OBJ_THING = new SetConfigKey<Object>(Object.class, "test.confSetObjThing", "Configuration key that's a set thing, of objects");
    }
    
    public static class MyEntityImpl extends AbstractEntity implements MyEntity {
    }
}
