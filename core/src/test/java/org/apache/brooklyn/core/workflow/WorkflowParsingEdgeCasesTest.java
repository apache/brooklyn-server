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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.AddEntityWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.AddPolicyWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.ApplyInitializerWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.ClearConfigWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.ClearSensorWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.DeleteEntityWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.DeletePolicyWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.DeployApplicationWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.InvokeEffectorWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.ReparentEntityWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.SetConfigWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.SetEntityNameWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.SetSensorWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.UpdateChildrenWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.external.HttpWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.external.ShellWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.external.SshWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.FailWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.ForeachWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.GotoWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.LogWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.NoOpWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.RetryWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.ReturnWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.SleepWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.SwitchWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.ClearVariableWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.LoadWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.TransformVariableWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.WaitWorkflowStep;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.json.BrooklynObjectsJsonMapper;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;


public class WorkflowParsingEdgeCasesTest extends BrooklynMgmtUnitTestSupport {

    private BasicApplication app;

    Object runSteps(String ...steps) {
        Pair<BasicApplication, Object> result = WorkflowBasicTest.runStepsInNewApp(mgmt(), Arrays.asList(steps));
        this.app = result.getLeft();
        return result.getRight();
    }

    @Test
    public void testBackslashEscaping() {
        final String BS = "\\";

        String UNQUOTED_INPUT = "double unquoted " + BS + BS;
        String QUOTED_INPUT_UNQUOTED = "quoted " + BS + BS;
        String QUOTED_INPUT = "\"" + QUOTED_INPUT_UNQUOTED + "\"";
        String QUOTED_INPUT_UNQUOTED_UNESCAPED = "quoted " + BS;

        // entire phrase quoted will be unquoted and unescaped
        Asserts.assertEquals(runSteps("return " + QUOTED_INPUT), QUOTED_INPUT_UNQUOTED_UNESCAPED);
        // but unquoted won't be
        Asserts.assertEquals(runSteps("return " + UNQUOTED_INPUT), UNQUOTED_INPUT);
        // and partial words won't be even if quoted
        Asserts.assertEquals(runSteps("return " + UNQUOTED_INPUT + " and " + QUOTED_INPUT),
            UNQUOTED_INPUT + " and " + QUOTED_INPUT);

        // however 'let' does do unquoting on word level (according to EP words)
        Asserts.assertEquals(runSteps("let x = "+UNQUOTED_INPUT+" and "+QUOTED_INPUT, "return ${x}"),
                UNQUOTED_INPUT + " and " + QUOTED_INPUT_UNQUOTED_UNESCAPED);
    }
    
}
