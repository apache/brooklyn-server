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
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ListConfigKey.ListModifications;
import org.apache.brooklyn.core.config.MapConfigKey.MapModifications;
import org.apache.brooklyn.core.config.SetConfigKey.SetModifications;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Callables;

public class MapListAndOtherStructuredConfigKeyTest extends BrooklynAppUnitTestSupport {

    private List<SimulatedLocation> locs;
    private TestApplication app;
    private TestEntity entity;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() {
        locs = ImmutableList.of(new SimulatedLocation());
        app = TestApplication.Factory.newManagedInstanceForTests();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (app != null) Entities.destroyAll(app.getManagementContext());
    }
    
    @Test    
    public void testMapConfigKeyCanStoreAndRetrieveVals() throws Exception {
        entity.config().set(TestEntity.CONF_MAP_THING.subKey("akey"), "aval");
        entity.config().set(TestEntity.CONF_MAP_THING.subKey("bkey"), "bval");
        app.start(locs);
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("akey","aval","bkey","bval"));
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING.subKey("akey")), "aval");
    }
    
    @Test
    public void testMapConfigKeyCanStoreAndRetrieveFutureValsPutByKeys() throws Exception {
        final AtomicReference<String> bval = new AtomicReference<String>("bval-too-early");
        entity.config().set(TestEntity.CONF_MAP_THING.subKey("akey"), DependentConfiguration.whenDone(Callables.returning("aval")));
        entity.config().set(TestEntity.CONF_MAP_THING.subKey("bkey"), DependentConfiguration.whenDone(new Callable<String>() {
                public String call() {
                    return bval.get();
                }}));
        app.start(locs);
        bval.set("bval");
        
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("akey","aval","bkey","bval"));
    }

    @Test
    public void testMapConfigKeyCanStoreAndRetrieveFutureValsPutAsMap() throws Exception {
        final AtomicReference<String> bval = new AtomicReference<String>("bval-too-early");
        entity.config().set(TestEntity.CONF_MAP_THING, (Map) MutableMap.of(
                "akey", DependentConfiguration.whenDone(Callables.returning("aval")),
                "bkey", DependentConfiguration.whenDone(new Callable<String>() {
                    public String call() {
                        return bval.get();
                    }})));
        app.start(locs);
        bval.set("bval");
        
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("akey","aval","bkey","bval"));
    }

    @Test
    public void testUnstructuredConfigKeyCanStoreAndRetrieveFutureValsPutAsMap() throws Exception {
        final AtomicReference<String> bval = new AtomicReference<String>("bval-too-early");
        final AtomicInteger bref = new AtomicInteger(0);
        
        entity.config().set(ConfigKeys.newConfigKey(Object.class, TestEntity.CONF_MAP_THING.getName()), 
            MutableMap.of("akey", DependentConfiguration.whenDone(Callables.returning("aval")),
                "bkey", new DeferredSupplier<String>() {
                    @Override public String get() {
                        bref.incrementAndGet();
                        return bval.get();
                    }}));
        app.start(locs);
        assertEquals(bref.get(), 0);
        bval.set("bval");
  
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING.subKey("akey")), "aval");
        assertEquals(bref.get(), 0);
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING.subKey("bkey")), "bval");
        assertEquals(bref.get(), 1);
        
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("akey","aval","bkey","bval"));
        assertEquals(bref.get(), 2);
        
        // and changes are also visible
        bval.set("bval2");
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("akey","aval","bkey","bval2"));
        assertEquals(bref.get(), 3);
    }

    @Test
    public void testResolvesMapKeysOnGetNotPut() throws Exception {
        class DeferredSupplierConstant implements DeferredSupplier<String> {
            private final String val;
            DeferredSupplierConstant(String val) {
                this.val = val;
            }
            @Override
            public String get() {
                return val;
            }
        }
        entity.config().set((ConfigKey)TestEntity.CONF_MAP_THING,
                MutableMap.of(new DeferredSupplierConstant("akey"), new DeferredSupplierConstant("aval")));
        app.start(locs);
  
        // subkey is not resolvable in this way
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING.subKey("akey")), null);
        // deferred supplier keys are only resolved when map is gotten
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("akey","aval"));
    }

    @Test
    public void testConfigKeyStringWontStoreAndRetrieveMaps() throws Exception {
        Map<String, String> v1 = ImmutableMap.of("a", "1", "b", "bb");
        //it only allows strings
        try {
            entity.config().set((ConfigKey)TestEntity.CONF_MAP_THING.subKey("akey"), v1);
            fail();
        } catch (Exception e) {
            ClassCastException cce = Exceptions.getFirstThrowableOfType(e, ClassCastException.class);
            if (cce == null) throw e;
            if (!cce.getMessage().contains("Cannot coerce type")) throw e;
        }
    }
    
    @Test
    public void testConfigKeyCanStoreAndRetrieveMaps() throws Exception {
        Map<String, String> v1 = ImmutableMap.of("a", "1", "b", "bb");
        entity.config().set(TestEntity.CONF_MAP_PLAIN, v1);
        app.start(locs);
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_PLAIN), v1);
    }

    // TODO getConfig returns null; it iterated over the set to add each value so setting it to a null set was like a no-op
    @Test(enabled=false)
    public void testSetConfigKeyAsEmptySet() throws Exception {
        Entity entity2 = app.createAndManageChild(EntitySpec.create(TestEntity.class)
            .configure(TestEntity.CONF_SET_THING.getName(), ImmutableSet.<String>of()));

        Entity entity3 = app.createAndManageChild(EntitySpec.create(TestEntity.class)
            .configure(TestEntity.CONF_SET_THING, ImmutableSet.<String>of()));

        app.start(locs);
        
        assertEquals(entity3.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of());
        assertEquals(entity2.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of());
    }
    
    @Test
    public void testSetConfigKeyCanStoreAndRetrieveVals() throws Exception {
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), "aval");
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), "bval");
        app.start(locs);
        
        assertEquals(entity.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of("aval","bval"));
    }
    
    @Test
    public void testSetConfigKeyCanStoreAndRetrieveFutureVals() throws Exception {
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), DependentConfiguration.whenDone(Callables.returning("aval")));
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), DependentConfiguration.whenDone(Callables.returning("bval")));
        app.start(locs);
        
        assertEquals(entity.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of("aval","bval"));
    }

    @Test
    public void testSetConfigKeyAddDirect() throws Exception {
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), "aval");
        entity.config().set((ConfigKey)TestEntity.CONF_SET_THING, "bval");
        assertEquals(entity.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of("aval","bval"));
    }

    @Test
    public void testSetConfigKeyClear() throws Exception {
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), "aval");
        entity.config().set((ConfigKey)TestEntity.CONF_SET_THING, (Object) SetModifications.clearing());
        // for now defaults to null, but empty list might be better? or whatever the default is?
        assertEquals(entity.getConfig(TestEntity.CONF_SET_THING), null);
    }

    @Test
    public void testSetConfigKeyAddMod() throws Exception {
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), "aval");
        entity.config().set(TestEntity.CONF_SET_THING, SetModifications.add("bval", "cval"));
        assertEquals(entity.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of("aval","bval","cval"));
    }
    @Test
    public void testSetConfigKeyAddAllMod() throws Exception {
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), "aval");
        entity.config().set(TestEntity.CONF_SET_THING, SetModifications.addAll(ImmutableList.of("bval", "cval")));
        assertEquals(entity.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of("aval","bval","cval"));
    }
    @Test
    public void testSetConfigKeyAddItemMod() throws Exception {
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), "aval");
        entity.config().set((ConfigKey)TestEntity.CONF_SET_THING, SetModifications.addItem(ImmutableList.of("bval", "cval")));
        assertEquals(entity.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of("aval",ImmutableList.of("bval","cval")));
    }
    @Test
    public void testSetConfigKeyListMod() throws Exception {
        entity.config().set(TestEntity.CONF_SET_THING.subKey(), "aval");
        entity.config().set(TestEntity.CONF_SET_THING, SetModifications.set(ImmutableList.of("bval", "cval")));
        assertEquals(entity.getConfig(TestEntity.CONF_SET_THING), ImmutableSet.of("bval","cval"));
    }
    
    @Test // ListConfigKey deprecated, as order no longer guaranteed
    public void testListConfigKeyCanStoreAndRetrieveVals() throws Exception {
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), "aval");
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), "bval");
        app.start(locs);
        
        //assertEquals(entity.getConfig(TestEntity.CONF_LIST_THING), ["aval","bval"])
        assertEquals(ImmutableSet.copyOf(entity.getConfig(TestEntity.CONF_LIST_THING)), ImmutableSet.of("aval","bval"));
    }
    
    @Test // ListConfigKey deprecated, as order no longer guaranteed
    public void testListConfigKeyCanStoreAndRetrieveFutureVals() throws Exception {
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), DependentConfiguration.whenDone(Callables.returning("aval")));
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), DependentConfiguration.whenDone(Callables.returning("bval")));
        app.start(locs);
        
        //assertEquals(entity.getConfig(TestEntity.CONF_LIST_THING), ["aval","bval"])
        assertEquals(ImmutableSet.copyOf(entity.getConfig(TestEntity.CONF_LIST_THING)), ImmutableSet.of("aval","bval"));
    }

    @Test // ListConfigKey deprecated, as order no longer guaranteed
    public void testListConfigKeyAddDirect() throws Exception {
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), "aval");
        entity.config().set((ConfigKey)TestEntity.CONF_LIST_THING, "bval");
        //assertEquals(entity.getConfig(TestEntity.CONF_LIST_THING), ["aval","bval"])
        assertEquals(ImmutableSet.copyOf(entity.getConfig(TestEntity.CONF_LIST_THING)), ImmutableSet.of("aval","bval"));
    }

    @Test // ListConfigKey deprecated, as order no longer guaranteed
    public void testListConfigKeyClear() throws Exception {
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), "aval");
        entity.config().set((ConfigKey)TestEntity.CONF_LIST_THING, (Object) ListModifications.clearing());
        // for now defaults to null, but empty list might be better? or whatever the default is?
        assertEquals(entity.getConfig(TestEntity.CONF_LIST_THING), null);
    }

    @Test // ListConfigKey deprecated, as order no longer guaranteed
    public void testListConfigKeyAddMod() throws Exception {
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), "aval");
        entity.config().set(TestEntity.CONF_LIST_THING, ListModifications.add("bval", "cval"));
        //assertEquals(entity.getConfig(TestEntity.CONF_LIST_THING), ["aval","bval","cval"])
        assertEquals(ImmutableSet.copyOf(entity.getConfig(TestEntity.CONF_LIST_THING)), ImmutableSet.of("aval","bval","cval"));
    }

    @Test // ListConfigKey deprecated, as order no longer guaranteed
    public void testListConfigKeyAddAllMod() throws Exception {
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), "aval");
        entity.config().set(TestEntity.CONF_LIST_THING, ListModifications.addAll(ImmutableList.of("bval", "cval")));
        //assertEquals(entity.getConfig(TestEntity.CONF_LIST_THING), ["aval","bval","cval"])
        assertEquals(ImmutableSet.copyOf(entity.getConfig(TestEntity.CONF_LIST_THING)), ImmutableSet.of("aval","bval","cval"));
    }
    
    @Test // ListConfigKey deprecated, as order no longer guaranteed
    public void testListConfigKeyAddItemMod() throws Exception {
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), "aval");
        entity.config().set((ConfigKey)TestEntity.CONF_LIST_THING, ListModifications.addItem(ImmutableList.of("bval", "cval")));
        //assertEquals(entity.getConfig(TestEntity.CONF_LIST_THING), ["aval",["bval","cval"]])
        assertEquals(ImmutableSet.copyOf(entity.getConfig(TestEntity.CONF_LIST_THING)), ImmutableSet.of("aval",ImmutableList.of("bval","cval")));
    }
    
    @Test // ListConfigKey deprecated, as order no longer guaranteed
    public void testListConfigKeyListMod() throws Exception {
        entity.config().set(TestEntity.CONF_LIST_THING.subKey(), "aval");
        entity.config().set(TestEntity.CONF_LIST_THING, ListModifications.set(ImmutableList.of("bval", "cval")));
        //assertEquals(entity.getConfig(TestEntity.CONF_LIST_THING), ["bval","cval"])
        assertEquals(ImmutableSet.copyOf(entity.getConfig(TestEntity.CONF_LIST_THING)), ImmutableSet.of("bval","cval"));
    }

    @Test
    public void testMapConfigPutDirect() throws Exception {
        entity.config().set(TestEntity.CONF_MAP_THING.subKey("akey"), "aval");
        entity.config().set(TestEntity.CONF_MAP_THING, ImmutableMap.of("bkey","bval"));;
        //assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), [akey:"aval",bkey:"bval"])
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("akey","aval","bkey","bval"));
    }

    @Test
    public void testMapConfigPutAllMod() throws Exception {
        entity.config().set(TestEntity.CONF_MAP_THING.subKey("akey"), "aval");
        entity.config().set(TestEntity.CONF_MAP_THING, MapModifications.put(ImmutableMap.of("bkey","bval")));
        //assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), [akey:"aval",bkey:"bval"])
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("akey","aval","bkey","bval"));
    }

    @Test
    public void testMapConfigClearMod() throws Exception {
        entity.config().set(TestEntity.CONF_MAP_THING.subKey("akey"), "aval");
        entity.config().set((ConfigKey)TestEntity.CONF_MAP_THING, (Object) MapModifications.clearing());
        // for now defaults to null, but empty map might be better? or whatever the default is?
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), null);
    }
    
    @Test
    public void testMapConfigSetMod() throws Exception {
        entity.config().set(TestEntity.CONF_MAP_THING.subKey("akey"), "aval");
        entity.config().set(TestEntity.CONF_MAP_THING, MapModifications.set(ImmutableMap.of("bkey","bval")));
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_THING), ImmutableMap.of("bkey","bval"));
    }
    
    @Test
    public void testMapConfigDeepSetFromMap() throws Exception {
        entity.config().set(TestEntity.CONF_MAP_OBJ_THING, (Map)ImmutableMap.of("akey", ImmutableMap.of("aa","AA","a2","A2"), "bkey", "b"));
        
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_OBJ_THING.subKey("akey")), ImmutableMap.of("aa", "AA", "a2", "A2"));
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_OBJ_THING.subKey("bkey")), "b");
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_OBJ_THING), 
                ImmutableMap.of("akey", ImmutableMap.of("aa","AA","a2","A2"), "bkey", "b"));
    }
    
    @Test
    public void testMapConfigDeepSetFromSubkeys() throws Exception {
        entity.config().set(TestEntity.CONF_MAP_OBJ_THING.subKey("akey"), ImmutableMap.of("aa", "AA", "a2", "A2"));
        entity.config().set(TestEntity.CONF_MAP_OBJ_THING.subKey("bkey"), "b");
        
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_OBJ_THING.subKey("akey")), ImmutableMap.of("aa", "AA", "a2", "A2"));
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_OBJ_THING.subKey("bkey")), "b");
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_OBJ_THING), 
                ImmutableMap.of("akey", ImmutableMap.of("aa", "AA", "a2", "A2"), "bkey", "b"));
    }
    
    @Test
    public void testMapConfigAdd() throws Exception {
        entity.config().set(TestEntity.CONF_MAP_OBJ_THING.subKey("0key"), 0);
        entity.config().set(TestEntity.CONF_MAP_OBJ_THING.subKey("akey"), MutableMap.of("aa", "AA", "a2", "A2"));
        entity.config().set(TestEntity.CONF_MAP_OBJ_THING.subKey("bkey"), MutableList.of("b"));
        entity.config().set((ConfigKey)TestEntity.CONF_MAP_OBJ_THING, MapModifications.add(ImmutableMap.of("akey", ImmutableMap.of("a3",3), "bkey", "b2", "ckey", "cc")));
        
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_OBJ_THING), 
                ImmutableMap.of("0key", 0, "akey", ImmutableMap.of("aa","AA","a2","A2","a3",3), "bkey", ImmutableList.of("b","b2"), "ckey", "cc"));
        assertEquals(entity.getConfig(TestEntity.CONF_MAP_OBJ_THING.subKey("akey")), ImmutableMap.of("aa", "AA", "a2", "A2", "a3", 3));
    }
}
