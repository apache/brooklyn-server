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
package org.apache.brooklyn.core.workflow;

import com.google.common.base.Stopwatch;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.steps.RetryWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class WorkflowRetryTest extends RebindTestFixture<BasicApplication> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRetryTest.class);

    private BasicApplication app;

    @Override
    protected LocalManagementContext decorateOrigOrNewManagementContext(LocalManagementContext mgmt) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        return super.decorateOrigOrNewManagementContext(mgmt);
    }

    @Override
    protected BasicApplication createApp() {
        return null;
    }

    Task<?> runStep(Object step, Consumer<BasicApplication> appFunction) {
        return runSteps(MutableList.<Object>of(step), appFunction);
    }
    Task<?> runSteps(List<?> steps, Consumer<BasicApplication> appFunction) {
        return runSteps(steps, appFunction, null);
    }
    Task<?> runSteps(List<?> steps, Consumer<BasicApplication> appFunction, ConfigBag initialEffectorConfig) {
        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.app = app;
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, (List) steps)
                .copy(initialEffectorConfig)
        );
        if (appFunction!=null) appFunction.accept(app);
        eff.apply((EntityLocal)app);

        return app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
    }

    private List<Map<String,Object>> basicSteps() {
        return MutableList.of(
                MutableMap.of(
                        "s", "let integer x = ${x} + 1 ?? 0",
                        "id", "one",
                        "replayable", "yes"),

                MutableMap.of(
                        "s", "retry",
                        "limit", MutableList.of(5),
                        "condition",
                        MutableMap.of("target", "${x}",
                                "less-than", 3)
                ));
    }

    private List<Map<String,Object>> basicSteps(Consumer<List<Map<String,Object>>> tweaks) {
        List<Map<String, Object>> steps = basicSteps();
        tweaks.accept(steps);
        return steps;
    }

    @Test
    public void testRetryWithNext() {
        Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).put("next", "one")), null,
                ConfigBag.newInstance().configure(WorkflowEffector.OUTPUT, "${x}"));

        Asserts.assertEquals(lastInvocation.getUnchecked(), 3);
    }

    @Test
    public void testRetryWithExplicitReplayReachesMax() {
        // replay resets the workflow vars so it keeps setting x = 0
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).putAll(MutableMap.of("replay", "true"))), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "limit 5");
        }
    }

    @Test
    public void testRetryReplayByDefaultReachesMax() {
        try {
            Task<?> lastInvocation = runSteps(basicSteps(), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "limit 5");
        }
    }

    @Test
    public void testRetryWithReplayExplicitNextReachesMax() {
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).putAll(MutableMap.of("replay", "true", "next", "one"))), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "limit 5");
        }
    }


    @Test
    public void testNonreplayableRetryFails() {
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(0).remove("replayable")), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "not replayable");
        }
    }

    @Test
    public void testRetryWithReplayExplicitNextForcedReachesMax() {
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> {
                        l.get(0).remove("replayable");
                        l.get(1).putAll(MutableMap.of("replay", "force", "next", "one"));
                    }), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "limit 5");
        }
    }

    @Test
    public void testRetryWithReplayModeInvalidNiceError() {
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).putAll(MutableMap.of("replay", "bogus", "next", "one"))), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "invalid", "bogus", "expected one of", "true", "false", "force" );
        }
    }

    @Test(groups="Integration")  // because slow
    public void testRetryWithExponentialBackoffPercentage() {
        doTestRetryWithExponentialBackoff("300%");
    }
    @Test(groups="Integration")  // because slow
    public void testRetryWithExponentialBackoffTimes() {
        doTestRetryWithExponentialBackoff("4x");
    }

    void doTestRetryWithExponentialBackoff(String value) {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).putAll(MutableMap.of("backoff", "0 0 100ms increasing "+value))), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "limit 5");
        }
        long EXPECTED_DELAY = 0 + 0 + 100 + 400 + 1600;
        Asserts.assertThat(Duration.of(sw), d -> d.isLongerThan(Duration.millis(EXPECTED_DELAY)));
        long GRACE = 1500;
        Asserts.assertThat(Duration.of(sw), d -> d.isShorterThan(Duration.millis(EXPECTED_DELAY + GRACE)));
    }

    @Test(groups="Integration")  // because slow
    public void testRetryWithLinearBackoff() {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).putAll(MutableMap.of("backoff", "0 0 100ms increasing 100ms"))), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "limit 5");
        }
        long EXPECTED_DELAY = 0 + 0 + 100 + 200 + 300;
        Asserts.assertThat(Duration.of(sw), d -> d.isLongerThan(Duration.millis(EXPECTED_DELAY)));
        long GRACE = 1500;
        Asserts.assertThat(Duration.of(sw), d -> d.isShorterThan(Duration.millis(EXPECTED_DELAY + GRACE)));
    }

    @Test(groups="Integration")  // because slow
    public void testRetryWithBackoffAndJitter() {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).putAll(MutableMap.of("backoff",
                            MutableMap.of("initial", "50ms", "jitter", 5)))), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "limit 5");
        }
        long EXPECTED_DELAY = 50 * 5;
        // jitter will be up to 5x the above, averaging 3x, unlikely to be less than 2x
        Asserts.assertThat(Duration.of(sw), d -> d.isLongerThan(Duration.millis(EXPECTED_DELAY*2)));
        long GRACE = 500;
        Asserts.assertThat(Duration.of(sw), d -> d.isShorterThan(Duration.millis(EXPECTED_DELAY*10 + GRACE)));
    }

    @Test(groups="Integration")  // because slow
    public void testRetryWithLimitsInTime() {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> {
                        l.get(1).putAll(MutableMap.of("limit", "4 in 300ms", "backoff", "100ms 100ms 100ms 100ms 100ms 0"));
                    }), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "retries total,", "limit 4 in 300ms");
        }
        long EXPECTED_DELAY = 100*5;
        Asserts.assertThat(Duration.of(sw), d -> d.isLongerThan(Duration.millis(EXPECTED_DELAY)));
        long GRACE = 1000;
        Asserts.assertThat(Duration.of(sw), d -> d.isShorterThan(Duration.millis(EXPECTED_DELAY + GRACE)));
    }

    @Test(groups="Integration")  // because slow
    public void testRetryWithTimeout() {
        Stopwatch sw = Stopwatch.createStarted();
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> {
                        l.get(1).putAll(MutableMap.of("timeout", "300ms", "backoff", "100ms"));
                    }), null);
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailure(e);
            Asserts.assertNotNull(Exceptions.getFirstThrowableOfType(e, TimeoutException.class), "Exception "+e);
        }
        long EXPECTED_DELAY = 300;
        Asserts.assertThat(Duration.of(sw), d -> d.isLongerThan(Duration.millis(EXPECTED_DELAY)));
        long GRACE = 1000;
        Asserts.assertThat(Duration.of(sw), d -> d.isShorterThan(Duration.millis(EXPECTED_DELAY + GRACE)));
    }

    Task<?> lastInvocation;

    @Test
    public void testRetryInWorkflowOnError() {
        doTestRetryOnError(true);
    }

    @Test
    public void testRetryInStepOnError() {
        doTestRetryOnError(false);
    }

    void doTestRetryOnError(boolean onWorkflow) {
        // replay resets the workflow vars so it keeps setting x = 0
        // will keep
        Thread t = new Thread(() -> {
            ConfigBag effectorConfig = ConfigBag.newInstance().configureStringKey("replayable", "yes");
            Object failingStep = "let no_count = ${entity.sensor.no_count} + 1";

            if (onWorkflow) effectorConfig.configureStringKey("on-error", MutableList.of("retry backoff 10ms"));
            else failingStep = MutableMap.of("s", failingStep, "on-error", MutableList.of("retry backoff 10ms"));

            lastInvocation = runSteps(MutableList.of(
                            "let count = ${entity.sensor.count} ?? 0",
                            "let count = ${count} + 1",
                            "set-sensor count = ${count}",
                            failingStep,  // this will throw until we set the sensor
                            "set-sensor no_count = ${no_count}"),
                    null,
                    effectorConfig);
            log.info("Invocation completed with: "+lastInvocation.getUnchecked());
        });

        lastInvocation = null;
        t.start();
        while (lastInvocation==null) Time.sleep(Duration.millis(10));
        EntityAsserts.assertAttributeEventually(app, Sensors.newIntegerSensor("count"), v -> v!=null && v > 1);
        Asserts.assertFalse(lastInvocation.isDone());
        app.sensors().set(Sensors.newIntegerSensor("no_count"), -1);
        lastInvocation.getUnchecked(Duration.ONE_SECOND);
        EntityAsserts.assertAttributeEquals(app, Sensors.newIntegerSensor("no_count"), 0);
        // shouldn't have taken more than 5s
        EntityAsserts.assertAttribute(app, Sensors.newIntegerSensor("count"), v -> v < 50);
    }

}
