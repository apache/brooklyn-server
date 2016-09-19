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
package org.apache.brooklyn.camp.brooklyn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class ConfigYamlTest extends AbstractYamlTest {
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ConfigYamlTest.class);

    private ExecutorService executor;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        executor = Executors.newCachedThreadPool();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (executor != null) executor.shutdownNow();
    }

    @Test
    public void testPlainCollections() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confMapPlain:",
                "      mykey: myval",
                "    test.confListPlain:",
                "    - myval",
                "    test.confSetPlain:",
                "    - myval");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        // Task that resolves quickly
        assertEquals(entity.config().get(TestEntity.CONF_MAP_PLAIN), ImmutableMap.of("mykey", "myval"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_PLAIN), ImmutableList.of("myval"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_PLAIN), ImmutableSet.of("myval"));
    }
    
    /**
     * This tests config keys of type {@link org.apache.brooklyn.core.config.MapConfigKey}, etc.
     * It sets the value all in one go (as opposed to explicit sub-keys).
     */
    @Test
    public void testSpecialTypeCollections() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confMapThing:",
                "      mykey: myval",
                "    test.confMapObjThing:",
                "      mykey: myval",
                "    test.confListThing:",
                "    - myval",
                "    test.confListObjThing:",
                "    - myval",
                "    test.confSetThing:",
                "    - myval",
                "    test.confSetObjThing:",
                "    - myval");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        // Task that resolves quickly
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mykey", "myval"));
        assertEquals(entity.config().get(TestEntity.CONF_MAP_OBJ_THING), ImmutableMap.of("mykey", "myval"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_THING), ImmutableList.of("myval"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_OBJ_THING), ImmutableList.of("myval"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_THING), ImmutableSet.of("myval"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_OBJ_THING), ImmutableSet.of("myval"));
    }
    
    /**
     * This tests config keys of type {@link org.apache.brooklyn.core.config.MapConfigKey}, etc.
     * It sets the value of each sub-key explicitly, rather than all in one go.
     */
    @Test
    public void testSpecialTypeCollectionsWithExplicitSubkeys() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confMapThing.mykey: myval",
                "    test.confMapObjThing.mykey: myval",
                "    test.confListThing.mysubkey: myval",
                "    test.confListObjThing.mysubkey: myval",
                "    test.confSetThing.mysubkey: myval",
                "    test.confSetObjThing.mysubkey: myval");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        // Task that resolves quickly
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mykey", "myval"));
        assertEquals(entity.config().get(TestEntity.CONF_MAP_OBJ_THING), ImmutableMap.of("mykey", "myval"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_THING), ImmutableList.of("myval"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_OBJ_THING), ImmutableList.of("myval"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_THING), ImmutableSet.of("myval"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_OBJ_THING), ImmutableSet.of("myval"));
    }
    
    @Test
    public void testDeferredSupplierToConfig() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:config(\"myOtherConf\")",
                "    test.confMapThing:",
                "      mykey: $brooklyn:config(\"myOtherConf\")",
                "    myOtherConf: myOther",
                "    test.confMapPlain:",
                "      mykey: $brooklyn:config(\"myOtherConf\")",
                "    test.confListThing:",
                "    - $brooklyn:config(\"myOtherConf\")",
                "    test.confListPlain:",
                "    - $brooklyn:config(\"myOtherConf\")",
                "    test.confSetThing:",
                "    - $brooklyn:config(\"myOtherConf\")",
                "    test.confSetPlain:",
                "    - $brooklyn:config(\"myOtherConf\")",
                "    myOtherConf: myOther");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        assertEquals(entity.config().get(TestEntity.CONF_NAME), "myOther");
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mykey", "myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_THING), ImmutableList.of("myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_THING), ImmutableSet.of("myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_MAP_PLAIN), ImmutableMap.of("mykey", "myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_PLAIN), ImmutableList.of("myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_PLAIN), ImmutableSet.of("myOther"));
    }
    
    /**
     * TODO The {@code entity.config().getNonBlocking()} can return absent. When it's called with 
     * a deferred supplier value, it will kick off a task and then wait just a few millis for that 
     * task to execute deferredSupplier.get(). If it times out, then it returns Maybe.absent. 
     * However, on apache jenkins the machine is often slow so the task doesn't complete in the 
     * given number of millis (even though deferredSupplier.get() doesn't need to block for anything).
     * Same for {@link #testDeferredSupplierToAttributeWhenReadyInSpecialTypes()}.
     * See https://issues.apache.org/jira/browse/BROOKLYN-272.
     */
    @Test(groups="Broken")
    public void testDeferredSupplierToAttributeWhenReady() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:attributeWhenReady(\"myOtherSensor\")");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Attribute not yet set; non-blocking will return promptly without the value
        assertTrue(entity.config().getNonBlocking(TestEntity.CONF_NAME).isAbsent());

        // Now set the attribute: get will return once that has happened
        executor.submit(new Callable<Object>() {
            public Object call() {
                return entity.sensors().set(Sensors.newStringSensor("myOtherSensor"), "myOther");
            }});
        assertEquals(entity.config().get(TestEntity.CONF_NAME), "myOther");

        // Non-blocking calls will now return with the value
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_NAME).get(), "myOther");
    }
    
    /**
     * This tests config keys of type {@link org.apache.brooklyn.core.config.MapConfigKey}, etc.
     * For plain maps, see {@link #testDeferredSupplierToAttributeWhenReadyInPlainCollections()}.
     * 
     * TODO The {@code entity.config().getNonBlocking()} can return absent. When it's called with 
     * a deferred supplier value, it will kick off a task and then wait just a few millis for that 
     * task to execute deferredSupplier.get(). If it times out, then it returns Maybe.absent. 
     * However, on apache jenkins the machine is often slow so the task doesn't complete in the 
     * given number of millis (even though deferredSupplier.get() doesn't need to block for anything).
     * Same for {@link #testDeferredSupplierToAttributeWhenReady()}.
     * See https://issues.apache.org/jira/browse/BROOKLYN-272.
     */
    @Test(groups="Broken")
    public void testDeferredSupplierToAttributeWhenReadyInSpecialTypes() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:attributeWhenReady(\"myOtherSensor\")",
                "    test.confMapThing:",
                "      mykey: $brooklyn:attributeWhenReady(\"myOtherSensor\")",
                "    test.confListThing:",
                "    - $brooklyn:attributeWhenReady(\"myOtherSensor\")",
                "    test.confSetThing:",
                "    - $brooklyn:attributeWhenReady(\"myOtherSensor\")");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Attribute not yet set; non-blocking will return promptly without the value
        assertTrue(entity.config().getNonBlocking(TestEntity.CONF_NAME).isAbsent());
        assertTrue(entity.config().getNonBlocking(TestEntity.CONF_MAP_THING).isAbsent());
        assertTrue(entity.config().getNonBlocking(TestEntity.CONF_LIST_THING).isAbsent());
        assertTrue(entity.config().getNonBlocking(TestEntity.CONF_SET_THING).isAbsent());

        // Now set the attribute: get will return once that has happened
        executor.submit(new Callable<Object>() {
            public Object call() {
                return entity.sensors().set(Sensors.newStringSensor("myOtherSensor"), "myOther");
            }});
        assertEquals(entity.config().get(TestEntity.CONF_NAME), "myOther");
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mykey", "myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_THING), ImmutableList.of("myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_THING), ImmutableSet.of("myOther"));

        // Non-blocking calls will now return with the value
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_NAME).get(), "myOther");
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_MAP_THING).get(), ImmutableMap.of("mykey", "myOther"));
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_LIST_THING).get(), ImmutableList.of("myOther"));
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_SET_THING).get(), ImmutableSet.of("myOther"));
    }
    
    /**
     * This tests config keys of type {@link java.util.Map}, etc.
     * For special types (e.g. {@link org.apache.brooklyn.core.config.MapConfigKey}), see 
     * {@link #testDeferredSupplierToAttributeWhenReadyInPlainCollections()}.
     * 
     * TODO test doesn't work because getNonBlocking returns even when no value.
     *      For example, we get back: Present[value={mykey=attributeWhenReady("myOtherSensor")}].
     *      However, the `config().get()` does behave as desired.
     * 
     * Including the "WIP" group because this test would presumably have never worked!
     * Added to demonstrate the short-coming.
     */
    @Test(groups={"Broken", "WIP"})
    public void testDeferredSupplierToAttributeWhenReadyInPlainCollections() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confMapPlain:",
                "      mykey: $brooklyn:attributeWhenReady(\"myOtherSensor\")",
                "    test.confListPlain:",
                "    - $brooklyn:attributeWhenReady(\"myOtherSensor\")",
                "    test.confSetPlain:",
                "    - $brooklyn:attributeWhenReady(\"myOtherSensor\")");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Attribute not yet set; non-blocking will return promptly without the value
        assertTrue(entity.config().getNonBlocking(TestEntity.CONF_MAP_PLAIN).isAbsent());
        assertTrue(entity.config().getNonBlocking(TestEntity.CONF_LIST_PLAIN).isAbsent());
        assertTrue(entity.config().getNonBlocking(TestEntity.CONF_SET_PLAIN).isAbsent());

        // Now set the attribute: get will return once that has happened
        executor.submit(new Callable<Object>() {
            public Object call() {
                return entity.sensors().set(Sensors.newStringSensor("myOtherSensor"), "myOther");
            }});
        assertEquals(entity.config().get(TestEntity.CONF_MAP_PLAIN), ImmutableMap.of("mykey", "myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_LIST_PLAIN), ImmutableList.of("myOther"));
        assertEquals(entity.config().get(TestEntity.CONF_SET_PLAIN), ImmutableSet.of("myOther"));

        // Non-blocking calls will now return with the value
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_MAP_PLAIN).get(), ImmutableMap.of("mykey", "myOther"));
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_LIST_PLAIN).get(), ImmutableList.of("myOther"));
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_SET_PLAIN).get(), ImmutableSet.of("myOther"));
    }
}
