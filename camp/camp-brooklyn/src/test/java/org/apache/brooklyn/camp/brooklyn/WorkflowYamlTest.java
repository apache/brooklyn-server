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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.core.workflow.steps.*;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WorkflowYamlTest extends AbstractYamlTest {

    static final String VERSION = "0.1.0-SNAPSHOT";

    @SuppressWarnings("deprecation")
    static RegisteredType addRegisteredTypeBean(ManagementContext mgmt, String symName, Class<?> clazz) {
        RegisteredType rt = RegisteredTypes.bean(symName, VERSION,
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, clazz.getName()));
        ((BasicBrooklynTypeRegistry)mgmt.getTypeRegistry()).addToLocalUnpersistedTypeRegistry(rt, false);
        return rt;
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        addRegisteredTypeBean(mgmt(), "log", LogWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "sleep", SleepWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "no-op", NoOpWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "set-sensor", SetSensorWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "set-config", SetConfigWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "workflow-effector", WorkflowEffector.class);
    }

    @Test
    public void testWorkflowEffector() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      steps:",
                "        step1:",
                "          type: no-op",
                "        step2:",
                "          type: set-sensor",
                "          sensor: foo",
                "          value: bar",
                "        step3: set-sensor integer bar = 1",
                "        step4: set-config integer foo = 2",
                "");
        waitForApplicationTasks(app);

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();

        Task<?> invocation = app.invoke(effector, null);
        Object result = invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "foo"), "bar");
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "bar"), 1);
        EntityAsserts.assertConfigEquals(app, ConfigKeys.newConfigKey(Object.class, "foo"), 2);
    }

    @Test
    public void testWorkflowEffectorLogStep() throws Exception {

        // Prepare log watcher.
        ListAppender<ILoggingEvent> logWatcher;
        logWatcher = new ListAppender<>();
        logWatcher.start();
        ((Logger) LoggerFactory.getLogger(LogWorkflowStep.class)).addAppender(logWatcher);

        // Declare workflow in a blueprint, add various log steps.
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      steps:",
                "        step1:", // this step runs 3rd... so confused :-(
                "          type: log",
                "          message: test message 1",
                "        step2: log test message 2",
                "        step3: no-op",
                "        6: log ??", // this step runs 2nd...
                "        step4: log test message 3",
                "        5: log test message N"); // this step runs 1st !...
        waitForApplicationTasks(app);

        // Deploy the blueprint.
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
        Task<?> invocation = app.invoke(effector, null);
        invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        // Verify expected log messages.
        Assert.assertEquals(5, logWatcher.list.size());
        Assert.assertEquals(logWatcher.list.get(0).getFormattedMessage(), "5: test message N");
        Assert.assertEquals(logWatcher.list.get(1).getFormattedMessage(), "6: ??");
        Assert.assertEquals(logWatcher.list.get(2).getFormattedMessage(), "step1: test message 1");
        Assert.assertEquals(logWatcher.list.get(3).getFormattedMessage(), "step2: test message 2");
        Assert.assertEquals(logWatcher.list.get(4).getFormattedMessage(), "step4: test message 3");
    }
}
