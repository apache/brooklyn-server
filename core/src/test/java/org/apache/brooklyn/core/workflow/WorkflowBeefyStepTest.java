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

import com.google.mockwebserver.MockResponse;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocationReuseIntegrationTest;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.http.BetterMockWebServer;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class WorkflowBeefyStepTest extends BrooklynMgmtUnitTestSupport {

    protected void loadTypes() {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
    }

    BasicApplication lastApp;
    Object runStep(Object step, Consumer<BasicApplication> appFunction) {
        return runSteps(MutableList.<Object>of(step), appFunction);
    }
    Object runSteps(List<Object> steps, Consumer<BasicApplication> appFunction) {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.lastApp = app;
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, steps)
        );
        if (appFunction!=null) appFunction.accept(app);
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        return invocation.getUnchecked();
    }

    @Test
    public void testEffector() throws IOException {
        Object result = runSteps(MutableList.of(
                "let x = ${entity.sensor.x} + 1 ?? 0",
                "set-sensor x = ${x}",
                "set-sensor last-param = ${p1}",
                MutableMap.of(
                        "s", "invoke-effector myWorkflow",
                        "args", MutableMap.of("p1", "from-invocation"),
                        "condition", MutableMap.of("target", "${x}", "less-than", 2),
                        "next", "end"),
                "return ${x}"  // if effector isn't invoked
        ), null);
        Asserts.assertEquals(result, 2);
        EntityAsserts.assertAttributeEquals(lastApp, Sensors.newSensor(Object.class, "x"), 2);
        EntityAsserts.assertAttributeEquals(lastApp, Sensors.newSensor(Object.class, "last-param"), "from-invocation");
    }

    @Test
    public void testSshLocalhost() throws NoMachinesAvailableException {
        LocalhostMachineProvisioningLocation loc = mgmt.getLocationManager().createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class)
                .configure("address", Networking.getReachableLocalHost())
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()));
        SshMachineLocation ll = loc.obtain();

        RecordingSshTool.setCustomResponse(".*", new RecordingSshTool.CustomResponse(0, "foo", "<testing stderr>"));
        Object result = runStep("ssh echo foo", app -> ((EntityInternal) app).addLocations(MutableList.of(ll)));

        Asserts.assertEquals(RecordingSshTool.getExecCmds().stream().map(ex -> ex.commands).collect(Collectors.toList()), MutableList.of(MutableList.of("echo foo")));
        Asserts.assertEquals(result, MutableMap.of("exit_code", 0, "stdout", "foo", "stderr", "<testing stderr>"));
    }

    @Test
    public void testHttp() throws IOException {
        BetterMockWebServer server = BetterMockWebServer.newInstanceLocalhost();

        server.enqueue(new MockResponse().setResponseCode(200).setBody("ack"));
        server.play();

        Map result = (Map) runStep("http "+server.getUrl("/"), null);
        Asserts.assertEquals(result.get("status_code"), 200);
        Asserts.assertEquals(result.get("content"), "ack");
        Asserts.assertEquals(new String((byte[])result.get("content_bytes")), "ack");
        Asserts.assertThat(result.get("duration"), x -> Duration.nanos(1).isShorterThan(Duration.of(x)));
    }

    // container, winrm defined in downstream projects and tested in those projects and/or workflow yaml

    /*
     * TODO - custom ssh endpoint
     * TODO - ? - custom cert logic for http
     *
     * TODO - copying scp, kubecp ?; http put from file?; and filesets?
     * ... or ... stream-from: xxx; but that is too fiddly. support writing to temp file for use with cli?
     * xcp [ [?${FROM} [?${FILESET} "fileset"] ${LOCAL}] ${REMOTE_FILE_OR_PATH}
     *
     * type: scp
     * from:
     * - bundle: xxxx
     *   glob: ** / *.tf
     * to: path/
     * mkdir: true
     * rmdir: true
     *
     * output:
     * contents - if one argument supplied, receive that data, allow copy `from: { data: ${value} }`
     * count - number of files copied
     */
}
