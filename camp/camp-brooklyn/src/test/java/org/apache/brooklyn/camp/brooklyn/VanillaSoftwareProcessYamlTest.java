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
import static org.testng.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.function.FunctionSensor;
import org.apache.brooklyn.enricher.stock.UpdatingMap;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.internal.ssh.ExecCmdAsserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Test
public class VanillaSoftwareProcessYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(VanillaSoftwareProcessYamlTest.class);

    public static class MyCallable implements Callable<Object> {
        public static AtomicReference<Object> val = new AtomicReference<>();
        public static AtomicReference<CountDownLatch> latch = new AtomicReference<>();

        public static void clear() {
            val.set(null);
            latch.set(null);
        }
        @Override public Object call() throws Exception {
            if (latch.get() != null) latch.get().await();
            return val.get();
        }
    }

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MyCallable.clear();
        RecordingSshTool.clear();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        MyCallable.clear();
        RecordingSshTool.clear();
    }
    
    @Test
    public void testSshPolling() throws Exception {
        Entity app = createAndStartApplication(
            "location:",
            "  localhost:",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "services:",
            "- type: "+VanillaSoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    softwareProcess.serviceProcessIsRunningPollPeriod: 10ms",
            "    checkRunning.command: myCheckRunning",
            "    launch.command: myLaunch");
        waitForApplicationTasks(app);

        log.info("App started:");
        Dumper.dumpInfo(app);
        
        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
        
        RecordingSshTool.setCustomResponse(".*myCheckRunning.*", new CustomResponse(1, "simulating not running", ""));
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, false);
        
        RecordingSshTool.clear();
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
    }
    
    @Test
    public void testDisableSshPolling() throws Exception {
        // driver.isRunning will report failure
        RecordingSshTool.setCustomResponse(".*myCheckRunning.*", new CustomResponse(1, "simulating not running", ""));
        
        Entity app = createApplicationUnstarted(
            "location:",
            "  localhost:",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "services:",
            "- type: "+VanillaSoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    softwareProcess.serviceProcessIsRunningPollPeriod: 10ms",
            "    sshMonitoring.enabled: false",
            "    checkRunning.command: myCheckRunning",
            "    launch.command: myLaunch");
        
        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        
        RecordingSensorEventListener<Object> serviceUpListener = subscribe(entity, Attributes.SERVICE_UP);
        RecordingSensorEventListener<Object> serviceStateListener = subscribe(entity, Attributes.SERVICE_STATE_ACTUAL);

        // ensure these get the initial values because test relies on that later
        assertEventsEqualEventually(serviceStateListener, ImmutableList.of(Lifecycle.CREATED), true);
        assertEventsEqualEventually(serviceUpListener, ImmutableList.of(false), true);

        Task<Void> task = app.invoke(Startable.START, ImmutableMap.of());
        
        // Should eventually poll for 'checkRunning', before reporting 'up'
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                ExecCmdAsserts.assertExecHasAtLeastOnce(RecordingSshTool.getExecCmds(), "myCheckRunning");
            }});
        
        assertFalse(task.isDone());
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, false);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);

        // Let startup complete
        RecordingSshTool.setCustomResponse(".*myCheckRunning.*", new CustomResponse(0, "", ""));
        waitForApplicationTasks(app);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);

        // Should never again do ssh-poll of checkRunning
        RecordingSshTool.clear();
        Asserts.succeedsContinually(new Runnable() {
            public void run() {
                ExecCmdAsserts.assertExecHasNever(RecordingSshTool.getExecCmds(), "myCheckRunning");
            }});
        
        // Should not have transitioned through wrong states (e.g. never "on-fire"!)
        assertEventsEqualEventually(serviceUpListener, ImmutableList.of(false, true), true);
        assertEventsEqualEventually(serviceStateListener, ImmutableList.of(Lifecycle.CREATED, Lifecycle.STARTING, Lifecycle.RUNNING), true);
    }
   
    @Test
    public void testAlternativeServiceUpPolling() throws Exception {
        AttributeSensor<Boolean> alternativeUpIndicator = Sensors.newBooleanSensor("myAlternativeUpIndicator");
        MyCallable.latch.set(new CountDownLatch(1));
        
        Entity app = createApplicationUnstarted(
            "location:",
            "  localhost:",
            "    sshToolClass: "+RecordingSshTool.class.getName(),
            "services:",
            "- type: "+VanillaSoftwareProcess.class.getName(),
            "  brooklyn.config:",
            "    softwareProcess.serviceProcessIsRunningPollPeriod: 10ms",
            "    sshMonitoring.enabled: false",
            "    checkRunning.command: myCheckRunning",
            "    launch.command: myLaunch",
            "  brooklyn.initializers:",
            "  - type: "+FunctionSensor.class.getName(),
            "    brooklyn.config:",
            "      "+FunctionSensor.SENSOR_PERIOD.getName()+": 10ms",
            "      "+FunctionSensor.SENSOR_NAME.getName()+": " + alternativeUpIndicator.getName(),
            "      "+FunctionSensor.SENSOR_TYPE.getName()+": boolean",
            "      "+FunctionSensor.FUNCTION.getName()+":",
            "        $brooklyn:object:",
            "          type: "+MyCallable.class.getName(),
            "  brooklyn.enrichers:",
            "  - type: " + UpdatingMap.class.getName(),
            "    brooklyn.config:",
            "      enricher.sourceSensor: $brooklyn:sensor(\"" + alternativeUpIndicator.getName() + "\")",
            "      enricher.targetSensor: $brooklyn:sensor(\"service.notUp.indicators\")",
            "      enricher.updatingMap.computing:",
            "        $brooklyn:object:",
            "          type: \"" + Functions.class.getName() + "\"",
            "          factoryMethod.name: \"forMap\"",
            "          factoryMethod.args:",
            "          - false: \"false\"",
            "            true: null",
            "          - \"no value\"");
        
        VanillaSoftwareProcess entity = (VanillaSoftwareProcess) Iterables.getOnlyElement(app.getChildren());
        
        RecordingSensorEventListener<Object> serviceUpListener = subscribe(entity, Attributes.SERVICE_UP);
        RecordingSensorEventListener<Object> serviceStateListener = subscribe(entity, Attributes.SERVICE_STATE_ACTUAL);

        // ensure these get the initial values because test relies on that later
        assertEventsEqualEventually(serviceStateListener, ImmutableList.of(Lifecycle.CREATED), true);
        assertEventsEqualEventually(serviceUpListener, ImmutableList.of(false), true);

        Task<Void> task = app.invoke(Startable.START, ImmutableMap.of());
        
        // Should eventually poll for 'checkRunning', but just once immediately after doing launch etc
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                ExecCmdAsserts.assertExecHasOnlyOnce(RecordingSshTool.getExecCmds(), "myCheckRunning");
            }});
        RecordingSshTool.clear();

        assertFalse(task.isDone());
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_NOT_UP_INDICATORS, ImmutableMap.of(alternativeUpIndicator.getName(), "no value"));
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, false);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STARTING);

        // Let the function return 'false'
        MyCallable.val.set(false);
        MyCallable.latch.get().countDown();
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_NOT_UP_INDICATORS, ImmutableMap.of(alternativeUpIndicator.getName(), "false"));
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, false);
        assertFalse(task.isDone());

        // Let startup complete, by the function returning 'true'
        MyCallable.val.set(true);
        waitForApplicationTasks(app, Asserts.DEFAULT_LONG_TIMEOUT);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);

        // Should not have transitioned through wrong states (e.g. never "on-fire"!)
        assertEventsEqualEventually(serviceUpListener, ImmutableList.of(false, true), true);
        assertEventsEqualEventually(serviceStateListener, ImmutableList.of(Lifecycle.CREATED, Lifecycle.STARTING, Lifecycle.RUNNING), true);
        
        ExecCmdAsserts.assertExecHasNever(RecordingSshTool.getExecCmds(), "myCheckRunning");
    }
   
    private RecordingSensorEventListener<Object> subscribe(Entity entity, Sensor<?> sensor) {
        RecordingSensorEventListener<Object> listener = new RecordingSensorEventListener<>();
        mgmt().getSubscriptionManager().subscribe(MutableMap.of("notifyOfInitialValue", true), entity, sensor, listener);
        return listener;
    }
    
    private void assertEventsEqualEventually(RecordingSensorEventListener<?> listener, Iterable<?> expected, boolean stripLeadingNulls) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertIterablesEqual(listener.getEventValues(), (stripLeadingNulls ? leadingNullsStripper() : Functions.identity()), expected);
            }});
    }

    private void assertIterablesEqualEventually(Supplier<? extends Iterable<?>> actual, Function<? super List<?>, List<?>> transformer, Iterable<?> expected) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertIterablesEqual(actual.get(), transformer, expected);
            }});
    }
    
    private void assertIterablesEqual(Iterable<?> actual, Function<? super List<?>, List<?>> transformer, Iterable<?> expected) {
        List<?> actualList = (actual instanceof List) ? (List<?>) actual : MutableList.copyOf(actual);
        List<?> expectedList = (expected instanceof List) ? (List<?>) expected : MutableList.copyOf(expected);
        String errMsg = "actual="+actualList+"; expected="+expectedList;
        assertEquals(transformer.apply(actualList), expectedList, errMsg);
    }
    
    private Function<List<?>, List<?>> leadingNullsStripper() {
        return new Function<List<?>, List<?>>() {
            @Override public List<?> apply(List<?> input) {
                if (input == null || input.isEmpty() || input.get(0) != null) {
                    return input;
                }
                List<Object> result = new ArrayList<>();
                boolean foundNonNull = false;
                for (Object element : input) {
                    if (foundNonNull || input != null) {
                        result.add(element);
                        foundNonNull = true;
                    }
                }
                return result;
            }
        };
    }
}
