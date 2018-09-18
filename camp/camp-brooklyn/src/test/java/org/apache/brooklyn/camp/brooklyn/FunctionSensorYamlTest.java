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
import static org.testng.Assert.fail;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.feed.AttributePollHandler;
import org.apache.brooklyn.core.feed.Poller;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.function.FunctionSensor;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.test.LogWatcher.EventPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;

public class FunctionSensorYamlTest extends AbstractYamlRebindTest {
    private static final Logger log = LoggerFactory.getLogger(FunctionSensorYamlTest.class);

    final static AttributeSensor<String> SENSOR_STRING = Sensors.newStringSensor("aString");
    final static AttributeSensor<Integer> SENSOR_INT = Sensors.newIntegerSensor("anInt");

    public static class MyCallable implements Callable<Object> {
        public static AtomicReference<Object> val = new AtomicReference<>();
        public static AtomicInteger callCounter = new AtomicInteger();

        public static void clear() {
            callCounter.set(0);
            val.set(null);
        }
        @Override public Object call() throws Exception {
            callCounter.incrementAndGet();
            return val.get();
        }
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MyCallable.clear();
    }

    @Test
    public void testFunctionSensor() throws Exception {
        MyCallable.val.set("first");
        
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "  brooklyn.initializers:",
            "  - type: "+FunctionSensor.class.getName(),
            "    brooklyn.config:",
            "      "+FunctionSensor.SENSOR_PERIOD.getName()+": 100ms",
            "      "+FunctionSensor.SENSOR_NAME.getName()+": " + SENSOR_STRING.getName(),
            "      "+FunctionSensor.SENSOR_TYPE.getName()+": String",
            "      "+FunctionSensor.FUNCTION.getName()+":",
            "        $brooklyn:object:",
            "          type: "+MyCallable.class.getName());
        waitForApplicationTasks(app);
        Entity entity = Iterables.getOnlyElement(app.getChildren());

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "first");
        
        MyCallable.val.set("second");
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "second");
        
        // Rebind, and confirm that it resumes polling
        Application newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());

        MyCallable.val.set("third");
        EntityAsserts.assertAttributeEqualsEventually(newEntity, SENSOR_STRING, "third");
    }

    @Test
    public void testFunctionSensorCoerces() throws Exception {
        MyCallable.val.set("1");
        
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + TestEntity.class.getName(),
            "  brooklyn.config:",
            "    onbox.base.dir.skipResolution: true",
            "  brooklyn.initializers:",
            "  - type: "+FunctionSensor.class.getName(),
            "    brooklyn.config:",
            "      "+FunctionSensor.SENSOR_PERIOD.getName()+": 100ms",
            "      "+FunctionSensor.SENSOR_NAME.getName()+": " + SENSOR_INT.getName(),
            "      "+FunctionSensor.SENSOR_TYPE.getName()+": int",
            "      "+FunctionSensor.FUNCTION.getName()+":",
            "        $brooklyn:object:",
            "          type: "+MyCallable.class.getName());
        waitForApplicationTasks(app);
        Entity entity = Iterables.getOnlyElement(app.getChildren());

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_INT, 1);
        
        MyCallable.val.set("1");
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_INT, 1);
        
        // Rebind, and confirm that it resumes polling
        Application newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());

        MyCallable.val.set("3");
        EntityAsserts.assertAttributeEqualsEventually(newEntity, SENSOR_INT, 3);
    }

    @Test
    public void testWarnOnlyOnceOnRepeatedCoercionException() throws Exception {
        MyCallable.val.set("my-not-a-number");
        
        List<String> loggerNames = ImmutableList.of(
                AttributePollHandler.class.getName(), 
                Poller.class.getName());
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.TRACE;
        Predicate<ILoggingEvent> filter = Predicates.alwaysTrue();
        try (LogWatcher watcher = new LogWatcher(loggerNames, logLevel, filter)) {
            Entity app = createAndStartApplication(
                    "services:",
                    "- type: " + TestEntity.class.getName(),
                    "  brooklyn.config:",
                    "    onbox.base.dir.skipResolution: true",
                    "  brooklyn.initializers:",
                    "  - type: "+FunctionSensor.class.getName(),
                    "    brooklyn.config:",
                    "      "+FunctionSensor.SENSOR_PERIOD.getName()+": 1ms",
                    "      "+FunctionSensor.SENSOR_NAME.getName()+": mysensor",
                    "      "+FunctionSensor.SENSOR_TYPE.getName()+": int",
                    "      "+FunctionSensor.LOG_WARNING_GRACE_TIME_ON_STARTUP.getName()+": 0s",
                    "      "+FunctionSensor.SENSOR_TYPE.getName()+": int",
                    "      "+FunctionSensor.FUNCTION.getName()+":",
                    "        $brooklyn:object:",
                    "          type: "+MyCallable.class.getName());
            waitForApplicationTasks(app);

            // Wait until we've polled (and thus presumably tried to handle the response) 3 times, 
            // then shutdown the app so we don't risk flooding the log too much if it's going wrong!
            Asserts.succeedsEventually(() -> assertTrue(MyCallable.callCounter.get() > 3));
            ((BasicApplication)app).stop();
            
            // Ensure we log.warn only once
            Iterable<ILoggingEvent> warnEvents = Iterables.filter(watcher.getEvents(), EventPredicates.levelGeaterOrEqual(Level.WARN));
            assertTrue(Iterables.tryFind(warnEvents, EventPredicates.containsMessages("Read of", "gave exception", "Cannot coerce ")).isPresent(), "warnEvents="+warnEvents);
            assertEquals(Iterables.size(warnEvents), 1, "warnEvents="+warnEvents);

            // Ensure exception logging is acceptable
            Iterable<ILoggingEvent> exceptionEvents = Iterables.filter(watcher.getEvents(), EventPredicates.containsException());

            // Expect exactly one stacktrace in normal feed execution
            // e.g. [DEBUG] Trace for exception reading TestEntityImpl{id=xfnctsdr8k}->Sensor: mysensor (java.lang.Integer): org.apache.brooklyn.util.javalang.coerce.ClassCoercionException: Cannot coerce "my-not-a-number" to java.lang.Integer (my-not-a-number): adapting failed,
            Iterable<ILoggingEvent> activeExceptionEvents = Iterables.filter(exceptionEvents, EventPredicates.containsMessage("Trace for exception reading "));
            assertTrue(Iterables.tryFind(activeExceptionEvents, EventPredicates.containsExceptionMessage("Cannot coerce ")).isPresent(), "exceptionEvents="+exceptionEvents);
            assertEquals(Iterables.size(activeExceptionEvents), 1, "activeExceptionEvents="+activeExceptionEvents);
            
            // After stop, can have the stacktrace logged again (when entity is "inactive")
            // e.g. [DEBUG] unable to compute TestEntityImpl{id=xfnctsdr8k}->Sensor: mysensor (java.lang.Integer); exception=org.apache.brooklyn.util.javalang.coerce.ClassCoercionException: Cannot coerce "my-not-a-number" to java.lang.Integer (my-not-a-number): adapting failed (when inactive)
            // This could be for "interrupted", or potentially the "Cannot coerce" again.
            Iterable<ILoggingEvent> inactiveExceptionEvents = Iterables.filter(watcher.getEvents(), EventPredicates.containsMessage("when inactive"));
            Iterable<ILoggingEvent> inactiveCoercionExceptionEvents = Iterables.filter(inactiveExceptionEvents, EventPredicates.containsExceptionMessage("Cannot coerce "));
            Iterable<ILoggingEvent> inactiveInterruptExceptionEvents = Iterables.filter(inactiveExceptionEvents, EventPredicates.containsExceptionClassname("InterruptedException"));
            if (!Iterables.isEmpty(inactiveCoercionExceptionEvents)) {
                assertTrue(Iterables.size(inactiveCoercionExceptionEvents) <= 1, "inactiveCoercionExceptionEvents="+inactiveCoercionExceptionEvents);
            }

            // But no other exceptions should be logged
            int numAllowedExceptions = Iterables.size(Iterables.concat(activeExceptionEvents, inactiveCoercionExceptionEvents, inactiveInterruptExceptionEvents));
            if (Iterables.size(exceptionEvents) != numAllowedExceptions) {
                fail("exceptionEvents="+watcher.printEventsToString(exceptionEvents));
            }

        }
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
