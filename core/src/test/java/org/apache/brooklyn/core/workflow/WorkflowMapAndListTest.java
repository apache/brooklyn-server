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

import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.workflow.steps.appmodel.SetSensorWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class WorkflowMapAndListTest extends BrooklynMgmtUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(WorkflowMapAndListTest.class);

    private BasicApplication app;

    Object runSteps(List<?> steps) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);

        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.app = app;
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, (List) steps)
        );
        eff.apply((EntityLocal)app);
        return app.invoke(app.getEntityType().getEffectorByName ("myWorkflow").get(), null).getUnchecked();
    }

    @Test
    public void testSetWorkflowInMapWithDot() {
        Object result = runSteps(MutableList.of(
                "let map myMap = {}",
                "let myMap.a = 1",
                "return ${myMap.a}"
        ));
        Asserts.assertEquals(result, "1");
    }
    @Test
    public void testSetWorkflowInMapWithBrackets() {
        Object result = runSteps(MutableList.of(
                "let map myMap = {}",
                "let myMap['a'] = 1",
                "return ${myMap.a}"
        ));
        Asserts.assertEquals(result, "1");
    }
    @Test
    public void testSetWorkflowInListWithType() {
        Object result = runSteps(MutableList.of(
                "let list myList = []",
                "let int myList[0] = 1",
                "return ${myList[0]}"
        ));
        Asserts.assertEquals(result, 1);
    }

    @Test
    public void testSetWorkflowCreateMapAndList() {
        Object result = runSteps(MutableList.of(
                "let myMap['a'] = 1",
                "return ${myMap.a}"
        ));
        Asserts.assertEquals(result, "1");

        result = runSteps(MutableList.of(
                "let int myList[0] = 1",
                "return ${myList}"
        ));
        Asserts.assertEquals(result, Arrays.asList(1));
    }

    @Test
    public void testBasicSensorAndConfig() {
        runSteps(MutableList.of(
                "set-config my_config['a'] = 1",
                "set-sensor int my_sensor['a'] = 1"
        ));
        EntityAsserts.assertConfigEquals(app, ConfigKeys.newConfigKey(Object.class, "my_config"), MutableMap.of("a", "1"));
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "my_sensor"), MutableMap.of("a", 1));
    }

    @Test
    public void testMapBracketsMore() {
        Object result = runSteps(MutableList.of(
                "set-sensor service.problems['latency'] = \"too slow\"",
                "let x = ${entity.sensor['service.problems']}",
                "return ${x}"
        ));
        Asserts.assertEquals(result, ImmutableMap.of("latency", "too slow"));

        result = runSteps(MutableList.of(
                "let map myMap = {}",
                "let myMap['a'] = 1",
                "return ${myMap.a}"
        ));
        Asserts.assertEquals(result, "1");
    }

    @Test
    public void testMapBracketsUnquotedSyntaxLegacy() {
        // this will warn but will work, for legacy compatibility reasons

        Object result = runSteps(MutableList.of(
                "set-sensor service.problems[latency] = \"too slow\"",
                "let x = ${entity.sensor['service.problems']}",
                "return ${x}"
        ));
        Asserts.assertEquals(result, ImmutableMap.of("latency", "too slow"));

        result = runSteps(MutableList.of(
                "let map myMap = {}",
                "let myMap[a] = 1",
                "return ${myMap.a}"
        ));
        Asserts.assertEquals(result, "1");
    }

    @Test
    public void testMoreListByIndexInsertionCreationAndErrors() {
        Object result = runSteps(MutableList.of(
                "let list mylist = [1, 2, 3]",
                "let mylist[1] = 4",
                "return ${mylist[1]}"
        ));
        Asserts.assertEquals(result, "4");

        // 0 allowed to create (as does -1, further below)
        result = runSteps(MutableList.of(
                "let mylist[0] = 4",
                "return ${mylist[0]}"
        ));
        Asserts.assertEquals(result, "4");

        // other numbers not allowed to create
        Asserts.assertFailsWith(() -> runSteps(MutableList.of(
                "let mylist[123] = 4"
        )), Asserts.expectedFailureContainsIgnoreCase("cannot set index", "123", "mylist", "undefined"));

        // -1 puts at end
        result = runSteps(MutableList.of(
                "let list mylist = [1, 2, 3]",
                "let mylist[-1] = 4",
                "return ${mylist[3]}"
        ));
        Asserts.assertEquals(result, "4");

        // also works if we insert
        result = runSteps(MutableList.of(
                "let list mylist = [1, 2, 3]",
                "let mylist[-1]['a'][-1] = 4",
                "return ${mylist[3]['a'][0]}"
        ));
        Asserts.assertEquals(result, "4");
        // as does empty string
        result = runSteps(MutableList.of(
                "let list mylist = [1, 2, 3]",
                "let mylist[]['a'][] = 4",
                "return ${mylist[3]['a'][0]}"
        ));
        Asserts.assertEquals(result, "4");

        // note: getting -1 is not supported
        Asserts.assertFailsWith(() -> runSteps(MutableList.of(
                "let list mylist = [1,2,3]",
                "return ${mylist[-1]}"
        )), Asserts.expectedFailureContainsIgnoreCase("invalid", "reference", "-1", "mylist"));
    }

    @Test
    public void testUndefinedList() {
        Asserts.assertFailsWith(() -> runSteps(MutableList.of(
                    "let anotherList[123] = 4"
            )),
            e -> Asserts.expectedFailureContainsIgnoreCase(e.getCause(),
                    "cannot", "index", "anotherList", "123", "undefined"));
    }

    @Test
    public void testInvalidListSpecifier() {
        Asserts.assertFailsWith(() -> runSteps(MutableList.of(
                    "let list mylizt = [1, 2, 3]",
                    "let mylizt['abc'] = 4",
                    "return ${mylizt[1]}"
            )),
            e -> Asserts.expectedFailureContainsIgnoreCase(e.getCause(), "cannot", "mylizt", "abc", "list"));
    }

    @Test(groups = "Integration", invocationCount = 20)
    public void testBeefySensorRequireForAtomicityAndConfigCountsMapManyTimes() {
        testBeefySensorRequireForAtomicityAndConfigCountsMap();
    }

    @Test
    public void testBeefySensorRequireForAtomicityAndConfigCountsMap() {
        runSteps(MutableList.of(
                "set-sensor sum['total'] = 0", //needed for the 'last' check
                "set-sensor map counts = {}", //needed for the 'last' check
                MutableMap.of(
                        "step", "foreach n in 1..20",
                        "concurrency", 10,
                        "steps", MutableList.of(
                                "let last = ${entity.sensor['sum']['total']}",

                                // keep counts using sensors and config - observe config might get mismatches but no errors,
                                // and sensors update perfectly atomically

                                "let ck = count_${n}",
                                "let last_count = ${entity.sensor.counts[ck]} ?? 0",
                                "let count = ${last_count} + 1",
                                // races in setting config known; we might miss some here (see check)
                                "set-config counts['${ck}'] = ${count}",
                                // setting sensors however will be mutexed on the sensor (assuming it exists or is defined)
                                "set-sensor counts['${ck}'] = ${count}",
                                // append to a list
                                "set-config ${ck}[] = ${count}",

                                "let next = ${last} + ${n}",

                                "log trying to set ${n} to ${next}, attempt ${count}",
                                // also check require with retries
                                MutableMap.of(
                                        "step", "set-sensor sum['total']",
                                        // reference so we can easily find this test
                                        SetSensorWorkflowStep.REQUIRE.getName(), "${last}",
                                        "value", "${next}",
                                        "on-error", "retry from start"  // if condition not met, will replay
                                ),
                                "log succeeded setting ${n} to ${next}, attempt ${count}"
                        )
                )));

        Dumper.dumpInfo(app);
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "sum"), MutableMap.of("total", 210));

        Map counts;
        counts = app.sensors().get(Sensors.newSensor(Map.class, "counts"));
        Asserts.assertEquals(counts.size(), 20);
        Asserts.assertThat(counts.get("count_15"), x -> ((Integer) x) >= 1);

        counts = app.config().get(ConfigKeys.newConfigKey(Map.class, "counts"));
        Asserts.assertThat(counts.size(), x -> x >= 15);  // we don't guarantee concurrency on config map writes, so might lose a few
        if (counts.size()==20) {
            log.warn("ODD!!! config for counts updated perfectly"); // doesn't usually happen, unless slow machine
        }
        int nn = 0;
        for (int n=1; n<=20; n++) {
            List count_n = app.config().get(ConfigKeys.newConfigKey(List.class, "count_"+n));
            Asserts.assertThat(count_n.size(), x -> x >= 1);
            for (int i = 0; i < count_n.size(); i++)
                Asserts.assertEquals(count_n.get(i), i + 1);
            nn += count_n.size();
        }
        if (nn<=20) {
            log.warn("ODD!!! no retries");  // per odd comment above
        }
    }

}
