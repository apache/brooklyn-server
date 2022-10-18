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
package org.apache.brooklyn.entity.software.base;

import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.function.FunctionSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.enricher.stock.UpdatingMap;
import org.apache.brooklyn.entity.software.base.SoftwareProcess.ChildStartableMode;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponseGenerator;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmdPredicates;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecParams;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.brooklyn.util.core.internal.ssh.ExecCmdAsserts.*;

public class WorkflowSoftwareProcessTest extends BrooklynAppUnitTestSupport {

    private Location loc;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        WorkflowBasicTest.addWorkflowStepTypes(app.getManagementContext());
        loc = app.getManagementContext().getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .configure(FixedListMachineProvisioningLocation.MACHINE_SPECS, ImmutableList.<LocationSpec<? extends MachineLocation>>of(
                        LocationSpec.create(SshMachineLocation.class)
                                .configure("address", "1.2.3.4")
                                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()))));
        
        RecordingSshTool.clear();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshTool.clear();
    }

    private static CustomWorkflowStep workflow(Object ...steps) {
        return TypeCoercions.coerce(MutableMap.of("steps", Arrays.asList(steps)), CustomWorkflowStep.class);
    }

    @Test
    public void testWorkflowWithSensors() throws Exception {
        WorkflowSoftwareProcess child = app.createAndManageChild(EntitySpec.create(WorkflowSoftwareProcess.class)
                .configure(WorkflowSoftwareProcess.PRE_INSTALL_COMMAND, "preInstallCommand")
                .configure(WorkflowSoftwareProcess.INSTALL_WORKFLOW, workflow(
                        "ssh installWorkflow",
                        "set-sensor boolean installed = true"))
                .configure(WorkflowSoftwareProcess.POST_INSTALL_COMMAND, "postInstallCommand")
                .configure(WorkflowSoftwareProcess.PRE_CUSTOMIZE_COMMAND, "preCustomizeCommand")
                .configure(WorkflowSoftwareProcess.CUSTOMIZE_WORKFLOW, workflow("ssh customizeWorkflow"))
                .configure(WorkflowSoftwareProcess.POST_CUSTOMIZE_COMMAND, "postCustomizeCommand")
                .configure(WorkflowSoftwareProcess.PRE_LAUNCH_COMMAND, "preLaunchCommand")
                .configure(WorkflowSoftwareProcess.LAUNCH_WORKFLOW, workflow("ssh launchWorkflow"))
                .configure(WorkflowSoftwareProcess.POST_LAUNCH_COMMAND, "postLaunchCommand")
                .configure(WorkflowSoftwareProcess.CHECK_RUNNING_WORKFLOW, workflow("ssh checkRunningWorkflow", "return true"))
                .configure(WorkflowSoftwareProcess.STOP_WORKFLOW, workflow("ssh stopWorkflow", "set-sensor boolean stopped = true"))
        );
        app.start(ImmutableList.of(loc));

        assertExecsContain(RecordingSshTool.getExecCmds(), ImmutableList.of(
                "preInstallCommand", "installWorkflow", "postInstallCommand",
                "preCustomizeCommand", "customizeWorkflow", "postCustomizeCommand",
                "preLaunchCommand", "launchWorkflow", "postLaunchCommand",
                "checkRunningWorkflow"));

        EntityAsserts.assertAttributeEquals(child, Sensors.newSensor(Boolean.class, "installed"), true);
        EntityAsserts.assertAttributeEquals(child, Sensors.newSensor(Boolean.class, "stopped"), null);

        EntityAsserts.assertAttributeEqualsEventually(child, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEqualsEventually(child, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);

        app.stop();

        EntityAsserts.assertAttributeEquals(child, Sensors.newSensor(Boolean.class, "stopped"), true);
        assertExecContains(RecordingSshTool.getLastExecCmd(), "stopWorkflow");

        EntityAsserts.assertAttributeEqualsEventually(child, Attributes.SERVICE_UP, false);
        EntityAsserts.assertAttributeEqualsEventually(child, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPED);
    }
}
