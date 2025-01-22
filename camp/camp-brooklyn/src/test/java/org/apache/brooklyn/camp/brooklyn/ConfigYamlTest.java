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

import com.google.common.annotations.Beta;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.*;

public class ConfigYamlTest extends AbstractYamlTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(ConfigYamlTest.class);

    final static String DOUBLE_MAX_VALUE_TIMES_TEN = "" + Double.MAX_VALUE + "0";

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
                    Entities.destroy(app, true);
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
    
    @Test
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
     * For plain maps, see {@link #testDeferredSupplierToAttributeWhenReadyInPlainCollections()}
     */
    @Test
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
        Asserts.assertNotPresent(entity.config().getNonBlocking(TestEntity.CONF_NAME));
        Asserts.assertNotPresent(entity.config().getNonBlocking(TestEntity.CONF_MAP_THING));
        Asserts.assertNotPresent(entity.config().getNonBlocking(TestEntity.CONF_LIST_THING));
        Asserts.assertNotPresent(entity.config().getNonBlocking(TestEntity.CONF_SET_THING));

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
     */
    @Test
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

    @Test
    public void testAttributeWhenReadyOptionsBasic() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: { $brooklyn:attributeWhenReady: [ \"test.name\", { timeout: 10ms } ] }");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Attribute not yet set; non-blocking will return promptly without the value
        Stopwatch sw = Stopwatch.createStarted();
        Asserts.assertFailsWith(() -> entity.config().get(TestEntity.CONF_NAME),
                Asserts.expectedFailureContainsIgnoreCase("Cannot resolve", "$brooklyn:attributeWhenReady", "test.name", "10ms", "Resolving config test.confName", "Unsatisfied after "));
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isLongerThan(Duration.millis(9)));

        entity.sensors().set(TestEntity.NAME, "x");
        EntityAsserts.assertConfigEquals(entity, TestEntity.CONF_NAME, "x");
    }

    @Test
    public void testAttributeWhenReadyOptionsBasicOnOtherEntity() throws Exception {
        String v0 = "{ $brooklyn:chain: [ $brooklyn:entity(\"entity2\"), { attributeWhenReady: [ \"test.name\", { timeout: 10ms } ] } ] }";
        String v1 = "{ $brooklyn:chain: [ $brooklyn:entity(\"entity2\"), { attributeWhenReady: [ \"test.name\", { \"timeout\": \"10ms\" } ] } ] }";

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: "+v0,
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  id: entity2");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity1 = (TestEntity) Iterables.get(app.getChildren(), 0);
        final TestEntity entity2 = (TestEntity) Iterables.get(app.getChildren(), 1);

        // Attribute not yet set; non-blocking will return promptly without the value
        Stopwatch sw = Stopwatch.createStarted();
        Asserts.assertFailsWith(() -> entity1.config().get(TestEntity.CONF_NAME),
                Asserts.expectedFailureContainsIgnoreCase("Cannot resolve", "$brooklyn:chain", " attributeWhenReady", "test.name", "10ms", "Resolving config test.confName", "Unsatisfied after "));
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isLongerThan(Duration.millis(9)));

        entity2.sensors().set(TestEntity.NAME, "x");
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, "x");

        // and on fire
        entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, "x");
        entity2.sensors().remove(TestEntity.NAME);
        sw = Stopwatch.createStarted();
        Asserts.assertFailsWith(() -> entity1.config().get(TestEntity.CONF_NAME),
                Asserts.expectedFailureContainsIgnoreCase("Cannot resolve", "$brooklyn:chain", " attributeWhenReady", "test.name", "0", "Resolving config test.confName",
//                        "Unsatisfied after ",
                        "Abort due to", "on-fire"));
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isShorterThan(Duration.millis(999)));

        // and source code
        Maybe<Object> rawV = entity1.config().getRaw(TestEntity.CONF_NAME);
        Asserts.assertEquals(rawV.get().toString(), v1);
    }

    @Test
    public void testAttributeWhenReadyOptionsTimeoutZero() throws Exception {
        String v0 = "{ $brooklyn:chain: [ $brooklyn:entity(\"entity2\"), { attributeWhenReady: [ \"test.name\", { timeout: 0 } ] } ] }";

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: "+v0,
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  id: entity2");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity1 = (TestEntity) Iterables.get(app.getChildren(), 0);
        final TestEntity entity2 = (TestEntity) Iterables.get(app.getChildren(), 1);

        entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        Stopwatch sw = Stopwatch.createStarted();
        Asserts.assertFailsWith(() -> entity1.config().get(TestEntity.CONF_NAME),
                Asserts.expectedFailureContainsIgnoreCase("Cannot resolve", "$brooklyn:chain", " attributeWhenReady", "test.name", "0", "Resolving config test.confName",
                        "Waiting not permitted"));
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isShorterThan(Duration.millis(999)));

        entity2.sensors().set(TestEntity.NAME, "");
        sw = Stopwatch.createStarted();
        Asserts.assertFailsWith(() -> entity1.config().get(TestEntity.CONF_NAME),
                Asserts.expectedFailureContainsIgnoreCase("Cannot resolve", "$brooklyn:chain", " attributeWhenReady", "test.name", "0", "Resolving config test.confName",
                        "Waiting not permitted"));
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isShorterThan(Duration.millis(999)));

        entity2.sensors().set(TestEntity.NAME, "x");
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, "x");

        entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, "x");
        entity2.sensors().remove(TestEntity.NAME);
        // not aborted, just no timeout here
        Asserts.assertFailsWith(() -> entity1.config().get(TestEntity.CONF_NAME),
                e -> {
                    Asserts.expectedFailureContainsIgnoreCase(e, "Cannot resolve", "$brooklyn:chain", " attributeWhenReady", "test.name", "0", "Resolving config test.confName",
                        "Waiting not permitted");
                    Asserts.expectedFailureDoesNotContainIgnoreCase(e,
                            "Abort due to", "on-fire",
                            "Unsatisfied after ");
                    return true;
                });
    }

    @Test
    public void testAttributeWhenReadyOptionsTimeoutIfDownTimesOut() throws Exception {
        String v0 = "{ $brooklyn:chain: [ $brooklyn:entity(\"entity2\"), { attributeWhenReady: [ \"test.name\", { timeout: forever, timeout_if_down: 10ms, abort_if_on_fire: false } ] } ] }";

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: "+v0,
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  id: entity2");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity1 = (TestEntity) Iterables.get(app.getChildren(), 0);
        final TestEntity entity2 = (TestEntity) Iterables.get(app.getChildren(), 1);

        new Thread(()->{
            Time.sleep(10);
            entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);
        }).start();

        Stopwatch sw = Stopwatch.createStarted();
        Asserts.assertFailsWith(() -> entity1.config().get(TestEntity.CONF_NAME),
                Asserts.expectedFailureContainsIgnoreCase("Cannot resolve", "$brooklyn:chain", " attributeWhenReady", "test.name", "0", "Resolving config test.confName",
                        "tighter timeout due to", "stopping"));
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isShorterThan(Duration.millis(999)));
    }

    @Test
    public void testAttributeWhenReadyOptionsTimeoutIfDownResetsAndAbortsIfOnFire() throws Exception {
        // was 10ms, but that is too short as there are 10ms sleeps while stopping; 50ms is better
        String v0 = "{ $brooklyn:chain: [ $brooklyn:entity(\"entity2\"), { attributeWhenReady: [ \"test.name\", { timeout: forever, timeout_if_down: 50ms } ] } ] }";

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: "+v0,
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  id: entity2");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity1 = (TestEntity) Iterables.get(app.getChildren(), 0);
        final TestEntity entity2 = (TestEntity) Iterables.get(app.getChildren(), 1);

        entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);
        Stopwatch sw = Stopwatch.createStarted();
        new Thread(()->{
            Time.sleep(Duration.millis(10));
            entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);
            entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);  // will clear the timeout

            Time.sleep(Duration.millis(10));
            entity2.sensors().set(TestEntity.NAME, "x");
        }).start();
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, "x");
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isLongerThan(Duration.millis(19)));

        entity2.sensors().remove(TestEntity.NAME);
        sw = Stopwatch.createStarted();
        new Thread(()->{
            Time.sleep(Duration.millis(10));
            entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        }).start();
        Asserts.assertFailsWith(() -> entity1.config().get(TestEntity.CONF_NAME),
                Asserts.expectedFailureContainsIgnoreCase("Cannot resolve", "$brooklyn:chain", " attributeWhenReady", "test.name", "0", "Resolving config test.confName",
//                        "Unsatisfied after ",
                        "Abort due to", "on-fire"));
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isShorterThan(Duration.millis(999)));
    }

    @Test(groups="Integration")  // because slow
    public void testAttributeWhenReadyOptionsTimeoutIfDownResetsBetter() throws Exception {
        String v0 = "{ $brooklyn:chain: [ $brooklyn:entity(\"entity2\"), { attributeWhenReady: [ \"test.name\", { timeout: forever, timeout_if_down: 1s, abort_if_on_fire: false } ] } ] }";

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: "+v0,
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  id: entity2");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity1 = (TestEntity) Iterables.get(app.getChildren(), 0);
        final TestEntity entity2 = (TestEntity) Iterables.get(app.getChildren(), 1);

        entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);
        Stopwatch sw = Stopwatch.createStarted();
        new Thread(()->{
            Time.sleep(Duration.millis(100));
            entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPING);

            // comment these two lines out and it shoud fail
            Time.sleep(Duration.millis(50));
            entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);  // will clear the timeout

            Time.sleep(Duration.seconds(2));
            entity2.sensors().set(TestEntity.NAME, "x");
        }).start();
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, "x");
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isLongerThan(Duration.millis(19)));
    }

    @Test
    public void testAttributeWhenReadyOptionsAbortIfOnFireAndNoWait() throws Exception {
        String v0 = "{ $brooklyn:chain: [ $brooklyn:entity(\"entity2\"), { attributeWhenReady: [ \"test.name\", { wait_for_truthy: false, " +
                // these have no effect with wait_for_truthy: false
                "timeout: 1s, abort_if_on_fire: true } ] } ] }";

        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confName: "+v0,
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  id: entity2");

        final Entity app = createStartWaitAndLogApplication(yaml);
        final TestEntity entity1 = (TestEntity) Iterables.get(app.getChildren(), 0);
        final TestEntity entity2 = (TestEntity) Iterables.get(app.getChildren(), 1);

        Stopwatch sw = Stopwatch.createStarted();
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, null);
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isShorterThan(Duration.millis(999)));

        entity2.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, null);

        entity2.sensors().set(TestEntity.NAME, "x");
        EntityAsserts.assertConfigEquals(entity1, TestEntity.CONF_NAME, "x");
        Asserts.assertThat(Duration.of(sw.elapsed()), d -> d.isShorterThan(Duration.millis(999)));
    }

    @Test
    public void testConfigGoodNumericCoercions() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confDouble: 1.1",
                "    test.confInteger: 1.0");

        final Entity app = createStartWaitAndLogApplication(yaml);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        assertEquals(entity.config().get(TestEntity.CONF_INTEGER), (Integer)1);
        assertEquals(entity.config().get(TestEntity.CONF_DOUBLE), (Double)1.1);
    }

    @Test
    public void testConfigVeryLargeIntegerCoercionFails() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confInteger: 9999999999999999999999999999999999999332",
                "");

        Asserts.assertFailsWith(() -> createStartWaitAndLogApplication(yaml),
            e -> e.toString().contains("9999332"));
    }

    @Test
    public void testConfigOtherVeryLargeIntegerCoercionFails() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confInteger: 100000000000000000000000000000000000",
                "");

        Asserts.assertFailsWith(() -> createStartWaitAndLogApplication(yaml),
                e -> e.toString().contains("100000000000000000000000000000000000"));
    }

    @Test
    public void testConfigSlightlyLargeIntegerCoercionFails() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confInteger: 2147483648",
                "");

        Asserts.assertFailsWith(() -> createStartWaitAndLogApplication(yaml),
                e -> e.toString().contains("2147483648"));
    }

    @Test
    public void testConfigVeryLargeDoubleCoercionFails() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confDouble: 999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999332",
                "");

        Asserts.assertFailsWith(() -> createStartWaitAndLogApplication(yaml),
                e -> e.toString().contains("9999332"));
    }

    @Test
    public void testConfigSlightlyLargeDoubleParseFails() throws Exception {
        Object xm = null;
        try {
            xm = Yamls.parseAll("x: " + DOUBLE_MAX_VALUE_TIMES_TEN).iterator().next();

//        Object x = ((Map)xm).get("x");
//        LOG.info("x: "+x);
//        if (x instanceof Double) {
//            Asserts.fail("Should not be a double: "+x);
//        }

            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "yaml parser", "out of range", DOUBLE_MAX_VALUE_TIMES_TEN);
        }
    }

    @Test
    public void testConfigSlightlyLargeDoubleCoercionFails() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confDouble: "+DOUBLE_MAX_VALUE_TIMES_TEN,
                "");

        Asserts.assertFailsWith(() -> createStartWaitAndLogApplication(yaml),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "yaml parser", "out of range", DOUBLE_MAX_VALUE_TIMES_TEN));
    }


    @Test
    public void testConfigSlightlyLargeDoubleAsStringCoercionFails() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confDouble: "+ JavaStringEscapes.wrapJavaString(DOUBLE_MAX_VALUE_TIMES_TEN),
                "");

        Asserts.assertFailsWith(() -> createStartWaitAndLogApplication(yaml),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "cannot coerce", DOUBLE_MAX_VALUE_TIMES_TEN));
    }

    @Test
    public void testConfigFloatAsIntegerCoercionFails() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    test.confInteger: 1.5",
                "");

        Asserts.assertFailsWith(() -> createStartWaitAndLogApplication(yaml),
                e -> e.toString().contains("1.5"));
    }

    @Test
    public void testSensitiveConfigFailsIfConfigured() throws Exception {
        Asserts.assertFailsWith(() -> {
            return withSensitiveFieldsBlocked(() -> {
                String yaml = Joiner.on("\n").join(
                        "services:",
                        "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                        "  brooklyn.config:",
                        "    secret1: myval");

                return createStartWaitAndLogApplication(yaml);
            });
        }, e -> {
            Asserts.expectedFailureContainsIgnoreCase(e, "secret1");
            Asserts.expectedFailureDoesNotContain(e, "myval");
            return true;
        });
    }

    @Test
    public void testSensitiveConfigDslWorksOrFailsDependingHowConfigured() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    secret1: $brooklyn:literal(\"myval\")");

        // allowed
        withSensitiveFieldsBlocked(() -> {
            return createStartWaitAndLogApplication(yaml);
        });

        Asserts.assertFailsWith(() -> {
            return withSensitiveFieldsBlocked(() -> {
                String oldValue =
                        //((BrooklynProperties) mgmt().getConfig()).put(BrooklynServerConfig.SENSITIVE_FIELDS_PLAINTEXT_BLOCKED, true);
                        System.setProperty(BrooklynServerConfig.SENSITIVE_FIELDS_EXT_BLOCKED_PHRASES.getName(), "[ \"$brooklyn:literal\" ]");
                Sanitizer.getSensitiveFieldsTokens(true);
                try {
                    return createStartWaitAndLogApplication(yaml);
                } finally {
                    System.setProperty(BrooklynServerConfig.SENSITIVE_FIELDS_EXT_BLOCKED_PHRASES.getName(), oldValue!=null ? oldValue : "");
                    Sanitizer.getSensitiveFieldsTokens(true);
                }
            });
        }, e -> {
            Asserts.expectedFailureContainsIgnoreCase(e, "literal");
            Asserts.expectedFailureDoesNotContain(e, "myval");
            return true;
        });
    }

    @Beta
    public static <T> T withSensitiveFieldsBlocked(Callable<T> r) throws Exception {
        String oldValue =
                //((BrooklynProperties) mgmt().getConfig()).put(BrooklynServerConfig.SENSITIVE_FIELDS_PLAINTEXT_BLOCKED, true);
                System.setProperty(BrooklynServerConfig.SENSITIVE_FIELDS_PLAINTEXT_BLOCKED.getName(), "true");
        Sanitizer.getSensitiveFieldsTokens(true);
        Assert.assertTrue( Sanitizer.isSensitiveFieldsPlaintextBlocked() );

        try {

            return r.call();

        } finally {
            //((BrooklynProperties) mgmt().getConfig()).put(BrooklynServerConfig.SENSITIVE_FIELDS_PLAINTEXT_BLOCKED, oldValue);
            System.setProperty(BrooklynServerConfig.SENSITIVE_FIELDS_PLAINTEXT_BLOCKED.getName(), oldValue!=null ? oldValue : "");
            Sanitizer.getSensitiveFieldsTokens(true);
        }
    }
}
