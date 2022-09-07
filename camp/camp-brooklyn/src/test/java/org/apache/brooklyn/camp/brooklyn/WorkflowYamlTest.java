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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowDefinition;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepResolution;
import org.apache.brooklyn.core.workflow.steps.NoOpWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.SleepWorkflowStep;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Map;

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
        addRegisteredTypeBean(mgmt(), "sleep", SleepWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "no-op", NoOpWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "workflow-effector", WorkflowEffector.class);
    }

    @Test(groups="Integration")  // because uses sleeps - TODO rewrite using set-sensor when available, with better assertion
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
                "        step1:",
                "          type: sleep",
                "          duration: 1s",
                "        step2: { sleep: 1s }");
        waitForApplicationTasks(app);

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();

        Stopwatch sw = Stopwatch.createStarted();
        Task<?> invocation = app.invoke(effector, null);
        Object result = invocation.getUnchecked();
        Asserts.assertThat(Duration.of(sw), d -> !d.isShorterThan(Duration.seconds(2)));
        Dumper.dumpInfo(invocation);
    }

}
