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

import java.util.List;
import org.apache.brooklyn.api.entity.*;
import org.apache.brooklyn.util.collections.MutableList;
import org.testng.Assert;
import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class EntityInitializersTest extends BrooklynAppUnitTestSupport {

    private ConfigKey<String> keyWithDefault = ConfigKeys.newStringConfigKey("mykey", "mydescription", "mydefault");

    @Test
    public void testResolveSimple() throws Exception {
        // Should return config val
        assertEquals(resolve(ImmutableMap.of("mykey", "myval"), keyWithDefault), "myval");
        
        // Should return explicit null val
        assertEquals(resolve(MutableMap.of("mykey", null), keyWithDefault), null);
    }
    
    @Test
    public void testResolveDeferredSupplier() throws Exception {
        DeferredSupplier<String> supplier = new DeferredSupplier<String>() {
            @Override public String get() {
                return "myval";
            }
        };
        assertEquals(resolve(ImmutableMap.of("mykey", supplier), keyWithDefault), "myval");
    }
    
    @Test
    public void testResolveUsesDefaultIfAbsent() throws Exception {
        // No config value so should return default
        assertEquals(resolve(ImmutableMap.of(), keyWithDefault), "mydefault");
    }
    
    private Object resolve(Map<?,?> config, ConfigKey<?> key) {
        return EntityInitializers.resolve(ConfigBag.newInstance(config), key, app.getExecutionContext()); 
    }

    static List<String> sequenceMessages = MutableList.of();

    @ImplementedBy(InitSequenceTestEntityImpl.class)
    public static interface InitSequenceTestEntity extends Entity, EntityPostInitializable {
        final ConfigKey<String> NAME = ConfigKeys.newStringConfigKey("name");
    }
    public static class InitSequenceTestEntityImpl extends AbstractEntity implements InitSequenceTestEntity {

        @Override
        public void init() {
            super.init();
            sequenceMessages.add(config().get(NAME)+":"+"init");
        }

        @Override
        public void postInit() {
            sequenceMessages.add(config().get(NAME)+":"+"postInit");
        }
    }

    public static class InitSequenceEntityInitializer implements EntityInitializer {
        @Override
        public void apply(EntityLocal entity) {
            sequenceMessages.add(entity.config().get(InitSequenceTestEntity.NAME)+":"+"initializer");
        }
    }

    @Test
    public void testSequence() {
        sequenceMessages.clear();
        try {
            mgmt.getEntityManager().createEntity(EntitySpec.create(InitSequenceTestEntity.class)
                    .addInitializer(InitSequenceEntityInitializer.class)
                    .configure(InitSequenceTestEntity.NAME, "A")
                    .child(EntitySpec.create(InitSequenceTestEntity.class)
                            .addInitializer(InitSequenceEntityInitializer.class)
                            .configure(InitSequenceTestEntity.NAME, "B")));
            Assert.assertEquals(MutableList.of("A:init", "A:initializer", "B:init", "B:initializer", "B:postInit", "A:postInit"), sequenceMessages);
        } finally {
            sequenceMessages.clear();
        }
    }
}
