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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.workflow.steps.appmodel.InvokeEffectorWorkflowStep;
import org.apache.brooklyn.core.workflow.store.WorkflowRetentionAndExpiration;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class WorkflowConfigSensorEffectorTest extends RebindTestFixture<TestApplication> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowConfigSensorEffectorTest.class);

    @Override
    protected LocalManagementContext decorateOrigOrNewManagementContext(LocalManagementContext mgmt) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        app = null; // clear this
        mgmt.getBrooklynProperties().put(WorkflowRetentionAndExpiration.WORKFLOW_RETENTION_DEFAULT, "forever");
        return super.decorateOrigOrNewManagementContext(mgmt);
    }

    @Override
    protected TestApplication createApp() {
        return null;
    }

    @Override protected TestApplication rebind() throws Exception {
        return rebind(RebindOptions.create().terminateOrigManagementContext(true));
    }

    TestApplication app;
    Task<?> lastInvocation;

    Object runWorkflow(List<Object> steps) throws Exception {
        return runWorkflow(steps, null);
    }
    Object runWorkflow(List<Object> steps, ConfigBag extraEffectorConfig) throws Exception {
        if (app==null) app = createSampleApp();
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, steps)
                .putAll(extraEffectorConfig));
        eff.apply(app);

        lastInvocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        return lastInvocation.getUnchecked();
    }

    TestApplication createSampleApp() {
        return mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class).configure(BrooklynConfigKeys.PLAN_ID, "chilld")));
    }

    @Test
    public void testSetAndClearSensorOnEntity() throws Exception {
        runWorkflow(MutableList.of("set-sensor foo on chilld = bar"));
        Entity chilld = Iterables.getOnlyElement(app.getChildren());
        AttributeSensor<String> sensor = Sensors.newStringSensor("foo");
        Asserts.assertEquals(chilld.sensors().get(sensor), "bar");
        Asserts.assertNull(app.sensors().get(sensor));

        runWorkflow(MutableList.of("clear-sensor foo on chilld"));
        Asserts.assertNull(app.sensors().get(sensor));

        runWorkflow(MutableList.of(MutableMap.of(
                "s", "set-sensor",
                "sensor", "foo",
                "value", "bar2",
                "entity", "chilld")
        ));
        Asserts.assertEquals(chilld.sensors().get(sensor), "bar2");

        runWorkflow(MutableList.of(MutableMap.of(
                "s", "set-sensor",
                "sensor", "foo",
                "entity", chilld,
                "value", "bar3")
        ));
        Asserts.assertEquals(chilld.sensors().get(sensor), "bar3");

        runWorkflow(MutableList.of(MutableMap.of(
                "s", "set-sensor",
                "sensor", MutableMap.of("name", "foo", "entity", "chilld"),
                "value", "bar4")
        ));
        Asserts.assertEquals(chilld.sensors().get(sensor), "bar4");
    }

    @Test
    public void testSetAndClearConfigOnEntity() throws Exception {
        runWorkflow(MutableList.of("set-config foo on chilld = bar"));
        Entity chilld = Iterables.getOnlyElement(app.getChildren());
        ConfigKey<String> cfg = ConfigKeys.newStringConfigKey("foo");
        Asserts.assertEquals(chilld.config().get(cfg), "bar");
        Asserts.assertNull(app.config().get(cfg));

        runWorkflow(MutableList.of("clear-config foo on chilld"));
        Asserts.assertNull(app.config().get(cfg));
    }

    @Test
    public void testParseKeyEqualsValueExpressionStringList() {
        Asserts.assertEquals(InvokeEffectorWorkflowStep.parseKeyEqualsValueExpressionStringList("key = value"),
                MutableMap.of("key", "value"));
        Asserts.assertEquals(InvokeEffectorWorkflowStep.parseKeyEqualsValueExpressionStringList("key = value, k2 = v2"),
                MutableMap.of("key", "value", "k2", "v2"));
        Asserts.assertEquals(InvokeEffectorWorkflowStep.parseKeyEqualsValueExpressionStringList("key = { sk = sv }, k2 : v2 v2b  ,  k3 =  \"v3 v4\"  v5 "),
                MutableMap.of("key", "{ sk = sv }", "k2", "v2 v2b", "k3", "v3 v4  v5"));
    }

    @Test
    public void testInvokeEffectorOnEntity() throws Exception {
        runWorkflow(MutableList.of(MutableMap.of(
                "step", "invoke-effector identityEffector on chilld",
                "args", MutableMap.of("arg", "V"))));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "V");

        runWorkflow(MutableList.of(MutableMap.of("step", "invoke-effector identityEffector on chilld with arg = V")));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "V");

        runWorkflow(MutableList.of(
                "let map v = { key: value }",
                MutableMap.of("step", "invoke-effector identityEffector on chilld with arg = ${v}")));
        Asserts.assertEquals(lastInvocation.getUnchecked(), MutableMap.of("key", "value"));

        runWorkflow(MutableList.of(MutableMap.of("step", "invoke-effector identityEffector on chilld with arg = \"{ key: value }\"")));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "{ key: value }");

        // and check we can cast, with multiple arguments
        Entity chilld = Iterables.getOnlyElement(app.getChildren());
        ((EntityInternal)chilld).getMutableEntityType().addEffector(Effectors.effector(Object.class, "mapSwapped")
                        .parameter(Map.class, "skmap")
                        .parameter(Integer.class, "n")
                        .impl(new MapSwapped()).build());
        runWorkflow(MutableList.of(MutableMap.of("step", "invoke-effector mapSwapped on chilld with n= 2,skmap = { key: value }")));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "key=valuex2");
    }

    static class MapSwapped extends EffectorBody<Object> {
        @Override
        public Object call(ConfigBag p) {
            Map skmap = (Map) p.getStringKey("skmap");
            Integer n = (Integer) p.getStringKey("n");
            Entry<?, ?> entry = Iterables.getOnlyElement(((Map<?, ?>) skmap).entrySet());
            return ""+entry.getKey()+"="+entry.getValue()+"x"+n;
        }
    }

    @Test
    public void testLetEntityType() throws Exception {
        runWorkflow(MutableList.of("let entity x = chilld", "return ${x}"));
        Entity chilld = Iterables.getOnlyElement(app.getChildren());
        Asserts.assertEquals(lastInvocation.getUnchecked(), chilld);

        runWorkflow(MutableList.of("let entity x = "+chilld.getId(), "return ${x}"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), chilld);

        runWorkflow(MutableList.of("let entity x = chilldx",
                        "let y = ${x} ?? missing",
                        "return ${y}"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "missing");
        Asserts.assertFailsWith(() -> runWorkflow(MutableList.of("let entity x = chilldx",
                        "return ${x}")),
                // would be nice not to cast it to null if not found, and maybe for return null to be allowed (?);
                // might want a different way to look up an entity (functional?)
//                Asserts.expectedFailureContainsIgnoreCase("entity", "not known", "chilldx")
//                        .and(Asserts.expectedFailureDoesNotContainIgnoreCase("${x}", "null"))
                // but for now it is useful to be able to do lookups, and null lets us tell it wasn't found
                Asserts.expectedFailureContainsIgnoreCase("${x}", "null")
                    );
    }

}
