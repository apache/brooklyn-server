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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class ConfigYamlTest extends AbstractYamlTest {
    
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
    public void testConfigInConfigBlock() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: myName",
                "    test.confObject: myObj",
                "    confStringAlias: myString",
                "    test.confDynamic: myDynamic",
                "    myField: myFieldVal",
                "    myField2Alias: myField2Val");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        assertEquals(entity.config().get(TestEntity.CONF_NAME), "myName"); // confName has @SetFromFlag("confName"); using full name
        assertEquals(entity.config().get(TestEntity.CONF_OBJECT), "myObj"); // confObject does not have @SetFromFlag
        assertEquals(entity.config().get(TestEntity.CONF_STRING), "myString"); // set using the @SetFromFlag alias
        assertEquals(entity.config().get(ConfigKeys.newStringConfigKey("test.confDynamic")), "myDynamic"); // not defined on entity

        // This isn't exactly desired behaviour, just a demonstration of what happens!
        // The names used in YAML correspond to fields with @SetFromFlag. The values end up in the
        // {@link EntitySpec#config} rather than {@link EntitySpec#flags}. The field is not set.
        assertNull(entity.getMyField()); // field with @SetFromFlag
        assertNull(entity.getMyField2()); // field with @SetFromFlag("myField2Alias"), set using alias
    }
    

    @Test
    public void testRecursiveConfigFailsGracefully() throws Exception {
        doTestRecursiveConfigFailsGracefully(false);
    }
    
    @Test
    public void testRecursiveConfigImmediateFailsGracefully() throws Exception {
        doTestRecursiveConfigFailsGracefully(true);
    }
    
    protected void doTestRecursiveConfigFailsGracefully(boolean immediate) throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    infinite_loop: $brooklyn:config(\"infinite_loop\")");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Time.sleep(Duration.FIVE_SECONDS);
                    // error, loop wasn't interrupted or detected
                    LOG.warn("Timeout elapsed, destroying items; usage: "+
                            ((LocalManagementContext)mgmt()).getGarbageCollector().getUsageString());
                    Entities.destroy(app);
                } catch (RuntimeInterruptedException e) {
                    // expected on normal execution; clear the interrupted flag to prevent ugly further warnings being logged
                    Thread.interrupted();
                }
            }
        });
        t.start();
        try {
            String c;
            if (immediate) {
                // this should throw rather than return "absent", because the error is definitive (absent means couldn't resolve in time)
                c = entity.config().getNonBlocking(ConfigKeys.newStringConfigKey("infinite_loop")).or("FAILED");
            } else {
                c = entity.config().get(ConfigKeys.newStringConfigKey("infinite_loop"));
            }
            Asserts.shouldHaveFailedPreviously("Expected recursive error, instead got: "+c);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "infinite_loop", "recursive");
        } finally {
            if (!Entities.isManaged(app)) {
                t.interrupt();
            }
        }
    }

    @Test
    public void testConfigAtTopLevel() throws Exception {
        // This style is discouraged - instead use a "brooklyn.config:" block.
        // However, it's important we don't break this as blueprints in the wild rely on it!
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  test.confName: myName",
                "  test.confObject: myObj",
                "  confStringAlias: myString",
                "  test.confDynamic: myDynamic",
                "  myField: myFieldVal",
                "  myField2Alias: myField2Val");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());
     
        assertEquals(entity.config().get(TestEntity.CONF_NAME), "myName"); // confName has @SetFromFlag("confName"); using full name
        assertEquals(entity.config().get(TestEntity.CONF_OBJECT), "myObj"); // confObject does not have @SetFromFlag
        assertEquals(entity.config().get(TestEntity.CONF_STRING), "myString"); // set using the @SetFromFlag alias
        
        // The "dynamic" config key (i.e. not defined on the entity's type) is not picked up to 
        // be set on the entity if it's not inside the "brooklyn.config" block. This isn't exactly
        // desired behaviour, but it is what happens! This test is more to demonstrate the behaviour
        // than to say it is definitely what we want! But like the comment at the start of the 
        // method says, this style is discouraged so we don't really care.
        assertNull(entity.config().get(ConfigKeys.newStringConfigKey("test.confDynamic"))); // not defined on entity
        
        // Again this isn't exactly desired behaviour, just a demonstration of what happens!
        // The names used in YAML correspond to fields with @SetFromFlag. The values end up in the
        // {@link EntitySpec#config} rather than {@link EntitySpec#flags}. The field is not set.
        assertNull(entity.getMyField()); // field with @SetFromFlag
        assertNull(entity.getMyField2()); // field with @SetFromFlag("myField2Alias"), set using alias
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
            @Override
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
            @Override
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
            @Override
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
