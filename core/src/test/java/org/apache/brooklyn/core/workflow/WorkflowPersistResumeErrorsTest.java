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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.*;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WorkflowPersistResumeErrorsTest extends BrooklynMgmtUnitTestSupport {

    protected void loadTypes() {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
    }

    BasicApplication lastApp;
    Task<?> runStep(Object step, Consumer<BasicApplication> appFunction) {
        return runSteps(MutableList.<Object>of(step), appFunction);
    }
    Task<?> runSteps(List<Object> steps, Consumer<BasicApplication> appFunction) {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.lastApp = app;
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, steps)
        );
        if (appFunction!=null) appFunction.accept(app);
        eff.apply((EntityLocal)app);

        return app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
    }

    @Test
    public void testSensorWhenPaused() throws IOException {
        Task<?> invocation1 = runSteps(MutableList.of(
                "let integer x = 1",
                "set-sensor x = ${x}",
                "wait ${entity.attributeWhenReady.gate}",
                "let x = ${x} + 1",
                "set-sensor x = ${x}",
                "return ${x}"), null);

        EntityAsserts.assertAttributeEqualsEventually(lastApp, Sensors.newSensor(Object.class, "x"), 1);

        Map<String, WorkflowExecutionContext> workflow = new WorkflowStatePersistenceViaSensors(mgmt).getWorkflows(lastApp);
        Asserts.assertSize(workflow, 1);
        WorkflowExecutionContext current = workflow.values().iterator().next();
        Integer index = current.getCurrentStepIndex();
        Asserts.assertTrue(index >= 1 && index <= 2, "Index is "+index);
        Asserts.assertEquals(current.status, WorkflowExecutionContext.WorkflowStatus.RUNNING);
        Asserts.assertFalse(invocation1.isDone());

        lastApp.sensors().set(Sensors.newBooleanSensor("gate"), true);

        Asserts.assertEquals(invocation1.getUnchecked(), 2);
    }
}
