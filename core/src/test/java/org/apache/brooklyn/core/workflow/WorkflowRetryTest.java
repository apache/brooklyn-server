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

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.workflow.steps.RetryWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
    public void testRetryWithNext() throws IOException {
        Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).put("next", "one")), null,
                ConfigBag.newInstance().configure(WorkflowEffector.OUTPUT, "${x}"));

        Asserts.assertEquals(lastInvocation.getUnchecked(), 3);
    }

    @Test
    public void testRetryWithReplayReachesMax() throws IOException {
        // replay resets the workflow vars so it keeps setting x = 0
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(1).putAll(MutableMap.of("replay", "true", "next", "one"))), null,
                    ConfigBag.newInstance().configure(WorkflowEffector.OUTPUT, "${x}"));
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, RetryWorkflowStep.RetriesExceeded.class);
            Asserts.expectedFailureContainsIgnoreCase(e, "more than 5");
        }
    }

    @Test
    public void testNonreplayableRetryFails() throws IOException {
        try {
            Task<?> lastInvocation = runSteps(basicSteps(l -> l.get(0).remove("replayable")), null,
                    ConfigBag.newInstance().configure(WorkflowEffector.OUTPUT, "${x}"));
            Asserts.shouldHaveFailedPreviously("Instead got "+lastInvocation.getUnchecked());
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "not replayable");
        }
    }

}
