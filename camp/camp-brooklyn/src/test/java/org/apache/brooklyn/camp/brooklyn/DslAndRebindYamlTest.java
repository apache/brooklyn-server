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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ha.MementoCopyMode;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.persist.BrooklynPersistenceUtils;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RecordingRebindExceptionHandler;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.software.base.DoNothingSoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.policy.ha.ServiceReplacer;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

@Test
public class DslAndRebindYamlTest extends AbstractYamlRebindTest {

    private static final Logger log = LoggerFactory.getLogger(DslAndRebindYamlTest.class);

    protected ExecutorService executor;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newSingleThreadExecutor();
    }
    
    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        if (executor != null) executor.shutdownNow();
        super.tearDown();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    @SuppressWarnings("unchecked")
    protected <T extends Entity> T rebind(T entity) throws Exception {
        rebind();
        Entity result = mgmt().getEntityManager().getEntity(entity.getId());
        assertNotNull(result, "no entity found after rebind with id " + entity.getId());
        return (T) result;
    }

    protected Entity setupAndCheckTestEntityInBasicYamlWith(String... extras) throws Exception {
        Entity app = createAndStartApplication(loadYaml("test-entity-basic-template.yaml", extras));
        waitForApplicationTasks(app);

        Assert.assertEquals(app.getDisplayName(), "test-entity-basic-template");

        log.info("App started:");
        Dumper.dumpInfo(app);

        Assert.assertTrue(app.getChildren().iterator().hasNext(), "Expected app to have child entity");
        Entity entity = app.getChildren().iterator().next();
        Assert.assertTrue(entity instanceof TestEntity, "Expected TestEntity, found " + entity.getClass());

        return entity;
    }

    protected static <T> T getConfigInTask(final Entity entity, final ConfigKey<T> key) {
        return Entities.submit(entity, Tasks.<T>builder().body(new Callable<T>() {
            @Override
            public T call() throws Exception {
                return entity.getConfig(key);
            }
        }).build()).getUnchecked();
    }

    protected <T> Future<T> getConfigInTaskAsync(final Entity entity, final ConfigKey<T> key) {
        // Wait for the attribute to be ready in a new Task
        Callable<T> configGetter = new Callable<T>() {
            @Override
            public T call() throws Exception {
                T s = getConfigInTask(entity, key);
                getLogger().info("getConfig {}={}", key, s);
                return s;
            }
        };
        return executor.submit(configGetter);
    }

    @Test
    public void testDslAttributeWhenReady() throws Exception {
        Entity testEntity = entityWithAttributeWhenReady();
        ((EntityInternal) testEntity).sensors().set(Sensors.newStringSensor("foo"), "bar");
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_NAME), "bar");
    }

    @Test
    public void testDslAttributeWhenReadyRebindWhenResolved() throws Exception {
        Entity testEntity = entityWithAttributeWhenReady();
        ((EntityInternal) testEntity).sensors().set(Sensors.newStringSensor("foo"), "bar");
        
        Entity e2 = rebind(testEntity);

        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_NAME), "bar");
    }

    @Test
    public void testDslAttributeWhenReadyWhenNotYetResolved() throws Exception {
        Entity testEntity = entityWithAttributeWhenReady();
        
        Entity e2 = rebind(testEntity);

        // Wait for the attribute to be ready in a new Task
        Future<String> stringFuture = getConfigInTaskAsync(e2, TestEntity.CONF_NAME);

        // Check that the Task is still waiting for attribute to be ready
        Assert.assertFalse(stringFuture.isDone());

        // Set the sensor; expect that to complete
        e2.sensors().set(Sensors.newStringSensor("foo"), "bar");
        String s = stringFuture.get(10, TimeUnit.SECONDS); // Timeout just for sanity
        Assert.assertEquals(s, "bar");
    }

    @Test
    public void testDslAttributeWhenReadyPersistedAsDeferredSupplier() throws Exception {
        doDslAttributeWhenReadyPersistedAsDeferredSupplier(false);
    }
    
    @Test
    public void testDslAttributeWhenReadyPersistedWithoutLeakingResolvedValue() throws Exception {
        doDslAttributeWhenReadyPersistedAsDeferredSupplier(true);
    }
    
    protected void doDslAttributeWhenReadyPersistedAsDeferredSupplier(boolean resolvedBeforeRebind) throws Exception {
        Entity testEntity = entityWithAttributeWhenReady();
        
        if (resolvedBeforeRebind) {
            testEntity.sensors().set(Sensors.newStringSensor("foo"), "bar");
            Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_NAME), "bar");
        }
        
        // Persist and rebind
        Entity e2 = rebind(testEntity);

        Maybe<Object> maybe = ((EntityInternal) e2).config().getLocalRaw(TestEntity.CONF_NAME);
        Assert.assertTrue(maybe.isPresentAndNonNull());
        Assert.assertTrue(BrooklynDslDeferredSupplier.class.isInstance(maybe.get()));
        BrooklynDslDeferredSupplier<?> deferredSupplier = (BrooklynDslDeferredSupplier<?>) maybe.get();
        Assert.assertEquals(deferredSupplier.toString(), "$brooklyn:entity(\"x\").attributeWhenReady(\"foo\")");

        // Assert the persisted state itself is as expected, and not too big
        BrooklynMementoRawData raw = BrooklynPersistenceUtils.newStateMemento(mgmt(), MementoCopyMode.LOCAL);
        String persistedStateForE2 = raw.getEntities().get(e2.getId());
        Matcher matcher = Pattern.compile(".*\\<test.confName\\>(.*)\\<\\/test.confName\\>.*", Pattern.DOTALL)
                .matcher(persistedStateForE2);
        Assert.assertTrue(matcher.find());
        String testConfNamePersistedState = matcher.group(1);

        Assert.assertNotNull(testConfNamePersistedState);
        // should be about 200 chars long, something like:
        //
        //      <test.confName>
        //        <org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent_-AttributeWhenReady>
        //          <component>
        //            <componentId>x</componentId>
        //            <scope>GLOBAL</scope>
        //          </component>
        //          <sensorName>foo</sensorName>
        //        </org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent_-AttributeWhenReady>
        //      </test.confName>

        Assert.assertTrue(testConfNamePersistedState.length() < 400, "persisted state too long: " + testConfNamePersistedState);
        
        Assert.assertFalse(testConfNamePersistedState.contains("bar"), "value 'bar' leaked in persisted state");
    }

    @Test
    public void testDslAttributeWhenReadyInEntitySpecWhenNotYetResolved() throws Exception {
        doDslAttributeWhenReadyInEntitySpec(false);
    }
    
    @Test
    public void testDslAttributeWhenReadyInEntitySpecWhenAlreadyResolved() throws Exception {
        doDslAttributeWhenReadyInEntitySpec(true);
    }
    
    protected void doDslAttributeWhenReadyInEntitySpec(boolean resolvedBeforeRebind) throws Exception {
        String yaml = "location: localhost\n" +
                "name: Test Cluster\n" +
                "services:\n" +
                "- type: org.apache.brooklyn.entity.group.DynamicCluster\n" +
                "  id: test-cluster\n" +
                "  initialSize: 0\n" +
                "  memberSpec:\n" +
                "    $brooklyn:entitySpec:\n" +
                "      type: org.apache.brooklyn.core.test.entity.TestEntity\n" +
                "      brooklyn.config:\n" +
                "        test.confName: $brooklyn:component(\"test-cluster\").attributeWhenReady(\"sensor\")";

        final Entity testEntity = createAndStartApplication(yaml);
        DynamicCluster cluster = (DynamicCluster) Iterables.getOnlyElement(testEntity.getApplication().getChildren());
        cluster.resize(1);
        Assert.assertEquals(cluster.getMembers().size(), 1);

        if (resolvedBeforeRebind) {
            cluster.sensors().set(Sensors.newStringSensor("sensor"), "bar");
        }

        // Persist and rebind
        DynamicCluster cluster2 = rebind(cluster);

        // Assert the persisted state itself is as expected, and not too big
        BrooklynMementoRawData raw = BrooklynPersistenceUtils.newStateMemento(mgmt(), MementoCopyMode.LOCAL);
        String persistedStateForE2 = raw.getEntities().get(cluster2.getId());
        String expectedTag = "org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent_-AttributeWhenReady";
        Matcher matcher = Pattern.compile(".*\\<"+expectedTag+"\\>(.*)\\<\\/"+expectedTag+"\\>.*", Pattern.DOTALL)
                .matcher(persistedStateForE2);
        Assert.assertTrue(matcher.find(), persistedStateForE2);
        String testConfNamePersistedState = matcher.group(1);
        Assert.assertNotNull(testConfNamePersistedState);

        // Can re-size to create a new member entity
        cluster2.resize(2);
        Assert.assertEquals(cluster2.getMembers().size(), 2);
        
        // Both the existing and the new member should have the DeferredSupplier config
        for (Entity member : Iterables.filter(cluster2.getChildren(), TestEntity.class)) {
            Maybe<Object> maybe = ((EntityInternal)member).config().getLocalRaw(TestEntity.CONF_NAME);
            Assert.assertTrue(maybe.isPresentAndNonNull());
            BrooklynDslDeferredSupplier<?> deferredSupplier = (BrooklynDslDeferredSupplier<?>) maybe.get();
            Assert.assertEquals(deferredSupplier.toString(), "$brooklyn:entity(\"test-cluster\").attributeWhenReady(\"sensor\")");
        }
        
        if (resolvedBeforeRebind) {
            // All members should resolve their config
            for (Entity member : Iterables.filter(cluster2.getChildren(), TestEntity.class)) {
                String val = getConfigInTask(member, TestEntity.CONF_NAME);
                Assert.assertEquals(val, "bar");
            }
        } else {
            List<Future<String>> futures = Lists.newArrayList();
            
            // All members should have unresolved values
            for (Entity member : Iterables.filter(cluster2.getChildren(), TestEntity.class)) {
                // Wait for the attribute to be ready in a new Task
                Future<String> stringFuture = getConfigInTaskAsync(member, TestEntity.CONF_NAME);
                futures.add(stringFuture);
                
                // Check that the Task is still waiting for attribute to be ready
                Thread.sleep(100);
                Assert.assertFalse(stringFuture.isDone());
            }
            
            // After setting the sensor, all those values should now resolve
            cluster2.sensors().set(Sensors.newStringSensor("sensor"), "bar");
            
            for (Future<String> future : futures) {
                String s = future.get(10, TimeUnit.SECONDS); // Timeout just for sanity
                Assert.assertEquals(s, "bar");
            }
        }
    }

    private Entity entityWithAttributeWhenReady() throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:component(\"x\").attributeWhenReady(\"foo\")");
    }

    private void doTestOnEntityWithSensor(Entity testEntity, Sensor<?> expectedSensor) throws Exception {
        doTestOnEntityWithSensor(testEntity, expectedSensor, true);
    }

    private void doTestOnEntityWithSensor(Entity testEntity, Sensor<?> expectedSensor, boolean inTask) throws Exception {
        @SuppressWarnings("rawtypes")
        ConfigKey<Sensor> configKey = ConfigKeys.newConfigKey(Sensor.class, "test.sensor");
        Sensor<?> s;
        s = inTask ? getConfigInTask(testEntity, configKey) : testEntity.getConfig(configKey);
        Assert.assertEquals(s, expectedSensor);
        Entity te2 = rebind(testEntity);
        s = inTask ? getConfigInTask(te2, configKey) : te2.getConfig(configKey);
        Assert.assertEquals(s, expectedSensor);
    }

    @Test
    public void testDslSensorFromClass() throws Exception {
        doTestOnEntityWithSensor(entityWithSensorFromClass(), Attributes.SERVICE_UP);
        switchOriginalToNewManagementContext();
        
        // without context it can still find it
        doTestOnEntityWithSensor(entityWithSensorFromClass(), Attributes.SERVICE_UP, false);
    }

    @Test
    public void testDslSensorLocal() throws Exception {
        doTestOnEntityWithSensor(entityWithSensorLocal(), TestEntity.SEQUENCE);
        // here without context it makes one up, so type info (and description etc) not present;
        // but context is needed to submit the DslDeferredSupplier object, so this would fail
//        doTestOnEntityWithSensor(entityWithSensorAdHoc(), Sensors.newSensor(Object.class, TestEntity.SEQUENCE.getName()), false);
    }

    @Test
    public void testDslSensorAdHoc() throws Exception {
        doTestOnEntityWithSensor(entityWithSensorAdHoc(), Sensors.newSensor(Object.class, "sensor.foo"));
        // here context has no impact, but it is needed to submit the DslDeferredSupplier object so this would fail
//        doTestOnEntityWithSensor(entityWithSensorAdHoc(), Sensors.newSensor(Object.class, "sensor.foo"), false);
    }

    private Entity entityWithSensorFromClass() throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.sensor: $brooklyn:sensor(\"" + Attributes.class.getName() + "\", \"" + Attributes.SERVICE_UP.getName() + "\")");
    }

    private Entity entityWithSensorLocal() throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.sensor: $brooklyn:sensor(\"" + TestEntity.SEQUENCE.getName() + "\")");
    }

    private Entity entityWithSensorAdHoc() throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.sensor: $brooklyn:sensor(\"sensor.foo\")");
    }


    @Test
    public void testDslConfigFromRoot() throws Exception {
        Entity testEntity = entityWithConfigFromRoot();
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_NAME), "bar");
    }

    @Test
    public void testDslConfigFromRootRebind() throws Exception {
        Entity testEntity = entityWithConfigFromRoot();
        Entity e2 = rebind(testEntity);

        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_NAME), "bar");
        Assert.assertEquals(e2.getConfig(TestEntity.CONF_NAME), "bar");
    }

    private Entity entityWithConfigFromRoot() throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:component(\"x\").config(\"foo\")",
                "brooklyn.config:",
                "  foo: bar");
    }

    @Test
    public void testDslConfigWithBrooklynParameterDefault() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.parameters:",
                "  - name: test.param",
                "    type: String",
                "    default: myDefaultVal",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:config(\"test.param\")");
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_NAME), "myDefaultVal");
        Assert.assertEquals(testEntity.config().get(TestEntity.CONF_NAME), "myDefaultVal");
        
        Entity e2 = rebind(testEntity);
        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_NAME), "myDefaultVal");
        Assert.assertEquals(e2.config().get(TestEntity.CONF_NAME), "myDefaultVal");
    }

    @Test
    public void testDslLocation() throws Exception {
        // TODO Doesn't work with `$brooklyn:location(0)`, only works if 0 is in quotes!
        
        String yaml = Joiner.on("\n").join(
                "location: localhost",
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    config1: $brooklyn:location()",
                "    config2: $brooklyn:location(\"0\")",
                "  brooklyn.children:",
                "  - type: " + DoNothingSoftwareProcess.class.getName(),
                "    brooklyn.config:",
                "      config1: $brooklyn:location()",
                "      config2: $brooklyn:location(\"0\")",
                "  - type: " + TestEntity.class.getName(),
                "    brooklyn.config:",
                "      config1: $brooklyn:location()",
                "      config2: $brooklyn:location(\"0\")");

        Application app = (Application) createStartWaitAndLogApplication(yaml);
        DoNothingSoftwareProcess softwareProcess = Iterables.getOnlyElement(Iterables.filter(app.getChildren(), DoNothingSoftwareProcess.class));
        TestEntity testEntity = Iterables.getOnlyElement(Iterables.filter(app.getChildren(), TestEntity.class));

        Location appLoc = Iterables.getOnlyElement(app.getLocations());
        assertEquals(getConfigInTask(app, ConfigKeys.newConfigKey(Object.class, "config1")), appLoc);
        assertEquals(getConfigInTask(app, ConfigKeys.newConfigKey(Object.class, "config2")), appLoc);

        Location softwareProcessLoc = Iterables.getOnlyElement(softwareProcess.getLocations());
        assertEquals(getConfigInTask(softwareProcess, ConfigKeys.newConfigKey(Object.class, "config1")), softwareProcessLoc);
        assertEquals(getConfigInTask(softwareProcess, ConfigKeys.newConfigKey(Object.class, "config2")), softwareProcessLoc);
        
        assertEquals(getConfigInTask(testEntity, ConfigKeys.newConfigKey(Object.class, "config1")), appLoc);
        assertEquals(getConfigInTask(testEntity, ConfigKeys.newConfigKey(Object.class, "config2")), appLoc);
    }

    @Test
    public void testDslLocationIndexOutOfBounds() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: localhost",
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    config1: $brooklyn:location(\"1\")");

        Application app = (Application) createStartWaitAndLogApplication(yaml);

        try {
            getConfigInTask(app, ConfigKeys.newConfigKey(Object.class, "config1"));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "has 1 location", "but requested index 1");
            if (Exceptions.getFirstThrowableOfType(e, IndexOutOfBoundsException.class) == null) {
                throw e;
            }
        }
    }

    @Test
    public void testDslLocationInEntity() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    config1: $brooklyn:location()",
                "    config2: $brooklyn:location(\"0\")",
                "  location: localhost");
        
        Application app = (Application) createStartWaitAndLogApplication(yaml);
        Location loc = Iterables.getOnlyElement(app.getLocations());
        
        assertEquals(getConfigInTask(app, ConfigKeys.newConfigKey(Object.class, "config1")), loc);
        assertEquals(getConfigInTask(app, ConfigKeys.newConfigKey(Object.class, "config2")), loc);
    }

    @Test
    public void testDslFormatString() throws Exception {
        Entity testEntity = entityWithFormatString();
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_NAME), "hello world");
    }

    @Test
    public void testDslFormatStringRebind() throws Exception {
        Entity testEntity = entityWithFormatString();
        Entity e2 = rebind(testEntity);

        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_NAME), "hello world");
    }

    private Entity entityWithFormatString() throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.confName: $brooklyn:formatString(\"hello %s\", \"world\")");
    }

    @Test
    public void testDslFormatStringWithDeferredSupplier() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.config:",
                "    test.confObject: world",
                "    test.confName:",
                "      $brooklyn:formatString:",
                "      - \"hello %s\"",
                "      - $brooklyn:config(\"test.confObject\")");
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_NAME), "hello world");
        
        Entity e2 = rebind(testEntity);
        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_NAME), "hello world");
    }

    @Test
    public void testDslUrlEncode() throws Exception {
        String unescapedVal = "name@domain?!/&:%";
        String escapedVal = "name%40domain%3F%21%2F%26%3A%25";

        String unescapedUrlFormat = "http://%s:password@mydomain.com";
        String escapedUrl = "http://" + escapedVal + ":password@mydomain.com";
        
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.config:",
                "    test.confObject: \"" + unescapedVal + "\"",
                "    test.confName:",
                "      $brooklyn:urlEncode:",
                "      - $brooklyn:config(\"test.confObject\")",
                "    test.confUrl:",
                "      $brooklyn:formatString:",
                "      - " + unescapedUrlFormat,
                "      - $brooklyn:urlEncode:",
                "        - $brooklyn:config(\"test.confObject\")");

        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_NAME), escapedVal);
        Assert.assertEquals(getConfigInTask(testEntity, ConfigKeys.newStringConfigKey("test.confUrl")), escapedUrl);
        
        Entity e2 = rebind(testEntity);
        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_NAME), escapedVal);
        Assert.assertEquals(getConfigInTask(e2, ConfigKeys.newStringConfigKey("test.confUrl")), escapedUrl);
    }

    @Test
    public void testDslEntityById() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.confObject: $brooklyn:entity(\"x\")");
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_OBJECT), testEntity);
        
        Entity e2 = rebind(testEntity);
        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_OBJECT), e2);
    }

    @Test
    public void testDslEntityWhereIdRetrievedFromAttributeWhenReadyDsl() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.confObject: $brooklyn:entity(attributeWhenReady(\"mySensor\"))");
        testEntity.sensors().set(Sensors.newStringSensor("mySensor"), "x");
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_OBJECT), testEntity);
        
        Entity e2 = rebind(testEntity);
        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_OBJECT), e2);
    }

    @Test
    public void testDslEntityWhereAttributeWhenReadyDslReturnsEntity() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.confObject: $brooklyn:entity(attributeWhenReady(\"mySensor\"))");
        testEntity.sensors().set(Sensors.newSensor(Entity.class, "mySensor"), testEntity);
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_OBJECT), testEntity);
        
        Entity e2 = rebind(testEntity);
        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_OBJECT), e2);
    }

    @Test
    public void testDslChildWhereIdRetrievedFromAttributeWhenReadyDsl() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.confObject: $brooklyn:child(attributeWhenReady(\"mySensor\"))",
                "  brooklyn.children:",
                "  - type: " + TestEntity.class.getName(),
                "    id: x");
        Entity childEntity = Iterables.getOnlyElement(testEntity.getChildren());
        testEntity.sensors().set(Sensors.newStringSensor("mySensor"), "x");
        Assert.assertEquals(getConfigInTask(testEntity, TestEntity.CONF_OBJECT), childEntity);
        
        Entity e2 = rebind(testEntity);
        Entity child2 = Iterables.getOnlyElement(e2.getChildren());
        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_OBJECT), child2);
    }

    @Test
    public void testDslChildWhereAttributeWhenReadyDslReturnsEntityOutOfScopeFails() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.confObject: $brooklyn:child(attributeWhenReady(\"mySensor\"))");
        testEntity.sensors().set(Sensors.newSensor(Entity.class, "mySensor"), testEntity);
        try {
            Object val = getConfigInTask(testEntity, TestEntity.CONF_OBJECT);
            Asserts.shouldHaveFailedPreviously("actual="+val);
        } catch (Exception e) {
            IllegalStateException ise = Exceptions.getFirstThrowableOfType(e, IllegalStateException.class);
            if (ise == null || !ise.toString().contains("is not in scope 'child'")) throw e;
        }
    }

    /*
        - type: org.apache.brooklyn.enricher.stock.Transformer
          brooklyn.config:
            enricher.sourceSensor: $brooklyn:sensor("mongodb.server.replicaSet.primary.endpoint")
            enricher.targetSensor: $brooklyn:sensor("justtheport")
            enricher.transformation: $brooklyn:function.regexReplacement("^.*:", "")
        - type: org.apache.brooklyn.enricher.stock.Transformer
          brooklyn.config:
            enricher.sourceSensor: $brooklyn:sensor("mongodb.server.replicaSet.primary.endpoint")
            enricher.targetSensor: $brooklyn:sensor("directport")
            enricher.targetValue: $brooklyn:regexReplacement($brooklyn:attributeWhenReady("mongodb.server.replicaSet.primary.endpoint"), "^.*:", "foo")
     */

    @Test
    public void testRegexReplacementWithStrings() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.config:",
                "    test.regex.config: $brooklyn:regexReplacement(\"somefooname\", \"foo\", \"bar\")"
        );
        Assert.assertEquals("somebarname", testEntity.getConfig(ConfigKeys.newStringConfigKey("test.regex.config")));
    }

    @Test
    public void testRegexReplacementWithAttributeWhenReady() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.config:",
                "    test.regex.config: $brooklyn:regexReplacement($brooklyn:attributeWhenReady(\"test.regex.source\"), $brooklyn:attributeWhenReady(\"test.regex.pattern\"), $brooklyn:attributeWhenReady(\"test.regex.replacement\"))"
        );
        testEntity.sensors().set(Sensors.newStringSensor("test.regex.source"), "somefooname");
        testEntity.sensors().set(Sensors.newStringSensor("test.regex.pattern"), "foo");
        testEntity.sensors().set(Sensors.newStringSensor("test.regex.replacement"), "bar");

        Assert.assertEquals("somebarname", testEntity.getConfig(ConfigKeys.newStringConfigKey("test.regex.config")));
    }

    @Test
    public void testRegexReplacementFunctionWithStrings() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.enrichers:",
                "  - type: org.apache.brooklyn.enricher.stock.Transformer",
                "    brooklyn.config:",
                "      enricher.sourceSensor: $brooklyn:sensor(\"test.name\")",
                "      enricher.targetSensor: $brooklyn:sensor(\"test.name.transformed\")",
                "      enricher.transformation: $brooklyn:function.regexReplacement(\"foo\", \"bar\")"
        );
        testEntity.sensors().set(TestEntity.NAME, "somefooname");
        AttributeSensor<String> transformedSensor = Sensors.newStringSensor("test.name.transformed");
        EntityAsserts.assertAttributeEqualsEventually(testEntity, transformedSensor, "somebarname");
    }

    @Test
    public void testRegexReplacementFunctionWithAttributeWhenReady() throws Exception {
        Entity testEntity = setupAndCheckTestEntityInBasicYamlWith(
                "  brooklyn.enrichers:",
                "  - type: org.apache.brooklyn.enricher.stock.Transformer",
                "    brooklyn.config:",
                "      enricher.sourceSensor: $brooklyn:sensor(\"test.name\")",
                "      enricher.targetSensor: $brooklyn:sensor(\"test.name.transformed\")",
                "      enricher.transformation: $brooklyn:function.regexReplacement($brooklyn:attributeWhenReady(\"test.pattern\"), $brooklyn:attributeWhenReady(\"test.replacement\"))"
        );
        testEntity.sensors().set(Sensors.newStringSensor("test.pattern"), "foo");
        testEntity.sensors().set(Sensors.newStringSensor("test.replacement"), "bar");
        testEntity.sensors().set(TestEntity.NAME, "somefooname");
        AttributeSensor<String> transformedSensor = Sensors.newStringSensor("test.name.transformed");
        EntityAsserts.assertAttributeEqualsEventually(testEntity, transformedSensor, "somebarname");
    }

    @Test
    public void testDslTemplateRebind() throws Exception {
        Entity testEntity = entityWithTemplatedString();
        Application app2 = rebind(testEntity.getApplication());
        Entity e2 = Iterables.getOnlyElement(app2.getChildren());

        Assert.assertEquals(getConfigInTask(e2, TestEntity.CONF_NAME), "hello world");
    }

    protected Entity entityWithTemplatedString() throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                "  id: x",
                "  brooklyn.config:",
                "    test.sourceName: hello world",
                "    test.confName: $brooklyn:template(\"${config['test.sourceName']}\")");
    }

    // See https://issues.apache.org/jira/browse/BROOKLYN-549
    @Test
    public void testDslInServiceReplacerPolicy() throws Exception {
        RecordingRebindExceptionHandler exceptionHandler = new RecordingRebindExceptionHandler(RecordingRebindExceptionHandler.builder()
                .strict());
        
        Entity app = createAndStartApplication(
                "services:",
                "- type: "+DynamicCluster.class.getName(),
                "  brooklyn.config:",
                "    initialSize: 0",
                "  brooklyn.policies:",
                "  - type: "+ServiceReplacer.class.getName(),
                "    brooklyn.config:",
                "      failureSensorToMonitor: $brooklyn:sensor(\"ha.entityFailed\")");
        waitForApplicationTasks(app);
        DynamicCluster cluster = (DynamicCluster) Iterables.getOnlyElement(app.getChildren());
        ServiceReplacer policy = (ServiceReplacer) Iterables.find(cluster.policies(), Predicates.instanceOf(ServiceReplacer.class));
        Sensor<?> sensor = policy.config().get(ServiceReplacer.FAILURE_SENSOR_TO_MONITOR);
        assertEquals(sensor.getName(), "ha.entityFailed");
        
        rebind(RebindOptions.create().exceptionHandler(exceptionHandler));
        
        Entity newApp = mgmt().getEntityManager().getEntity(app.getId());
        DynamicCluster newCluster = (DynamicCluster) Iterables.getOnlyElement(newApp.getChildren());
        ServiceReplacer newPolicy = (ServiceReplacer) Iterables.find(newCluster.policies(), Predicates.instanceOf(ServiceReplacer.class));
        
        Sensor<?> newSensor2 = ((EntityInternal)newCluster).getExecutionContext().submit("get-policy-config", new Callable<Sensor<?>>() {
            public Sensor<?> call() {
                return newPolicy.config().get(ServiceReplacer.FAILURE_SENSOR_TO_MONITOR);
            }})
            .get();
        assertEquals(newSensor2.getName(), "ha.entityFailed");
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-549
    // Similar to testDslInServiceReplacerPolicy, but with a simple test policy (so test will always
    // be here, even if someone changes the implementation of ServiceReplacer).
    @Test
    public void testDslInPolicy() throws Exception {
        RecordingRebindExceptionHandler exceptionHandler = new RecordingRebindExceptionHandler(RecordingRebindExceptionHandler.builder()
                .strict());
        
        Entity app = createAndStartApplication(
                "services:",
                "- type: "+BasicApplication.class.getName(),
                "  brooklyn.config:",
                "    myentity.myconfig: myval",
                "  brooklyn.policies:",
                "  - type: "+MyPolicy.class.getName(),
                "    brooklyn.config:",
                "      mypolicy.myconfig: $brooklyn:config(\"myentity.myconfig\")");
        waitForApplicationTasks(app);
        MyPolicy policy = (MyPolicy) Iterables.find(app.policies(), Predicates.instanceOf(MyPolicy.class));
        assertEquals(policy.resolvedVal, "myval");
        
        rebind(RebindOptions.create().exceptionHandler(exceptionHandler));
        
        Entity newApp = mgmt().getEntityManager().getEntity(app.getId());
        MyPolicy newPolicy = (MyPolicy) Iterables.find(newApp.policies(), Predicates.instanceOf(MyPolicy.class));
        assertEquals(newPolicy.resolvedVal, "myval");
    }
    
    public static class MyPolicy extends AbstractPolicy {
        public static final ConfigKey<String> MY_CONFIG = ConfigKeys.newStringConfigKey("mypolicy.myconfig");
        
        public String resolvedVal;
        
        @Override
        @SuppressWarnings("deprecation")
        public void setEntity(EntityLocal entity) {
            super.setEntity(entity);
            resolvedVal = checkNotNull(getConfig(MY_CONFIG), "myconfig");
        }
    }
}
