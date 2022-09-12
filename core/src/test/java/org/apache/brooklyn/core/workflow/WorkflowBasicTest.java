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
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.NoOpWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.SetSensorWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.SleepWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import java.util.Map;

public class WorkflowBasicTest extends BrooklynMgmtUnitTestSupport {

    static final String VERSION = "0.1.0-SNAPSHOT";

    @SuppressWarnings("deprecation")
    static RegisteredType addRegisteredTypeBean(ManagementContext mgmt, String symName, Class<?> clazz) {
        RegisteredType rt = RegisteredTypes.bean(symName, VERSION,
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, clazz.getName()));
        ((BasicBrooklynTypeRegistry)mgmt.getTypeRegistry()).addToLocalUnpersistedTypeRegistry(rt, false);
        return rt;
    }

    protected void loadTypes() {
        addRegisteredTypeBean(mgmt, "log", LogWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "sleep", SleepWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "no-op", NoOpWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "set-sensor", SetSensorWorkflowStep.class);
    }

    <T> T convert(Object input, Class<T> type) {
        return convert(input, TypeToken.of(type));
    }

    <T> T convert(Object input, TypeToken<T> type) {
        BrooklynClassLoadingContext loader = RegisteredTypes.getCurrentClassLoadingContextOrManagement(mgmt);
        try {
            return BeanWithTypeUtils.convert(mgmt, input, type, true, loader, false);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Test
    public void testStepResolution() {
        loadTypes();
        Map<String,Object> input = MutableMap.of("type", "no-op");

        // jackson
        WorkflowStepDefinition s = convert(input, WorkflowStepDefinition.class);
        Asserts.assertInstanceOf(s, NoOpWorkflowStep.class);

        // util
        s = WorkflowStepResolution.resolveStep(mgmt, "s1", input);
        Asserts.assertInstanceOf(s, NoOpWorkflowStep.class);
    }

    @Test
    public void testShorthandStepResolution() {
        loadTypes();
        String input = "sleep 1s";

        // only util will work for shorthand
        WorkflowStepDefinition s = WorkflowStepResolution.resolveStep(mgmt, "s1", input);
        Asserts.assertInstanceOf(s, SleepWorkflowStep.class);
        Asserts.assertEquals( ((SleepWorkflowStep)s).getDuration(), Duration.ONE_SECOND);
    }


    @Test
    public void testWorkflowDefinitionResolution() {
        loadTypes();

        Map<String,Object> input = MutableMap.of(
                "steps", MutableMap.of(
                        "step1", MutableMap.of("type", "no-op"),
                        "step2", MutableMap.of("type", "sleep", "duration", "1s"),
                        "step3", "sleep 1s",
                        "step4", "log test message"
                ));

        WorkflowDefinition w = convert(input, WorkflowDefinition.class);
        Asserts.assertNotNull(w);
        w.validate(mgmt);
        Map<String, WorkflowStepDefinition> steps = w.getStepsResolved(mgmt);
        Asserts.assertSize(steps, 4);
    }

    @Test
    public void testWorkflowEffector() {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableMap.of(
                        "step1", MutableMap.of("type", "no-op"),
                        "step2", MutableMap.of("type", "set-sensor", "sensor", "foo", "value", "bar"),
                        "step3", "set-sensor integer bar = 1",
                        "step4", "log test message"
                ))
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();

        Dumper.dumpInfo(invocation);

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "foo"), "bar");
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "bar"), 1);
    }

}
