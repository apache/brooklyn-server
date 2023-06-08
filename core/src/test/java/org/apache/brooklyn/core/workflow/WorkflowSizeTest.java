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
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

public class WorkflowSizeTest extends BrooklynMgmtUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(WorkflowSizeTest.class);

    private BasicApplication app;

    protected void createAppWithEffector(List<?> steps) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);

        if (this.app!=null) throw new IllegalStateException("Already have an app");
        this.app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("param", null))
                .configure(WorkflowEffector.STEPS, (List) steps)
        );
        eff.apply((EntityLocal)app);
    }

    @Test
    public void testSizeOfAllSensors() throws JsonProcessingException {
        createAppWithEffector(MutableList.of(
                "let pc = ${param}",
                "let map myMap = {}",
                "transform param | prepend hello-",
                "let myMap.a = ${param}",
                "let myMap.b = ${output}",
                "return ${myMap}"
        ));

        String sampleData = "sample data for testing something big\n";

        app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), MutableMap.of("param", sampleData)).getUnchecked();

        Map<String, Integer> sizes = getSensorSizes();
        sizes.forEach((k,v) -> { log.info("Sensor "+k+": "+v); });

        Asserts.assertThat(sizes.values().stream().reduce(0, (v0,v1)->v0+v1), result -> result < 10*1000);

        // print out the above, search for "something big" to see where the size is used
        String out = BeanWithTypeUtils.newYamlMapper(mgmt, true, null, true).writeValueAsString(
                app.sensors().get(Sensors.newSensor(Object.class, "internals.brooklyn.workflow")));
        log.info("WORKFLOW IS:\n"+out);

        app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), MutableMap.of("param", sampleData)).getUnchecked();
        sizes = getSensorSizes();
        sizes.forEach((k,v) -> { log.info("Sensor "+k+": "+v); });
        Asserts.assertThat(sizes.values().stream().reduce(0, (v0,v1)->v0+v1), result -> result < 20*1000);

        // 100k payload now -> bumps sensor size from 5k to 3MB (before any optimization)
        // removing output which is identical to the previous gives minor savings (in this test): 3380416 -> 3176074
        for (int i=0; i<1000; i++) {
            for (int j=0; j<10; j++) sampleData += "0123456789";
            sampleData += "\n";
        }
        app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), MutableMap.of("param", sampleData)).getUnchecked();
        sizes = getSensorSizes();
        sizes.forEach((k,v) -> { log.info("Sensor "+k+": "+v); });
        Asserts.assertThat(sizes.values().stream().reduce(0, (v0,v1)->v0+v1), result -> result > 100*1000);
    }

    protected Map<String,Integer> getSensorSizes() {
        //Dumper.dumpInfo(app);
        Map<String,Integer> sizes = MutableMap.of();
        for (int retryWhileCME=0; ; retryWhileCME++) {
            try {
                sizes.clear();
                app.sensors().getAll().forEach((k, v) -> {
                    try {
                        sizes.put(k.getName(), BeanWithTypeUtils.newMapper(mgmt, false, null, false).writeValueAsString(v).length());
                    } catch (JsonProcessingException e) {
                        throw Exceptions.propagate(e);
                    }
                });
                break;
            } catch (Exception e) {
                boolean allowedToRetry = false;
                allowedToRetry |= Exceptions.getFirstThrowableOfType(e, ConcurrentModificationException.class)!=null;
                allowedToRetry |= Exceptions.getFirstThrowableOfType(e, NullPointerException.class)!=null;
                if (allowedToRetry && retryWhileCME<10) {
                    log.info("Serializing sensors failed; will retry: "+e);
                    Time.sleep(100);
                    continue;
                }
                throw Exceptions.propagate(e);
            }
        }
        return sizes;
    }
}
