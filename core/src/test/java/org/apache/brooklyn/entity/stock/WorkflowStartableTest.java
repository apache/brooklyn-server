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
package org.apache.brooklyn.entity.stock;

import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.*;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.location.Locations.LocationsFilter;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class WorkflowStartableTest extends BrooklynAppUnitTestSupport {

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
    }

    @Test
    public void testSettingSensors() throws Exception {
        WorkflowStartable entity = app.addChild(EntitySpec.create(WorkflowStartable.class)
                .configure(WorkflowStartable.START_WORKFLOW.getName(), MutableMap.of("steps", MutableList.of("set-sensor started = yes")))
                .configure(WorkflowStartable.STOP_WORKFLOW.getName(), MutableMap.of("steps", MutableList.of("set-sensor started = no", "set-sensor stopped = yes")))
        );
        app.start(null);
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("started"), "yes");
        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);

        entity.invoke(entity.getEntityType().getEffectorByName("stop").get(), null).getUnchecked();
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("started"), "no");
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("stopped"), "yes");
        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_UP, false);
        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.STOPPED);

        entity.sensors().set(Sensors.newStringSensor("stopped"), "manually no");  // make sure this is cleared
        entity.invoke(entity.getEntityType().getEffectorByName("restart").get(), null).getUnchecked();
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("started"), "yes");
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("stopped"), "yes");  // because stop ran before start
        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "make-problem")
                .configure(WorkflowEffector.STEPS, MutableList.of("set-sensor service.problems = { some_problem: Testing }")) );
//                .configure(WorkflowEffector.STEPS, MutableList.of("set-sensor service.problems['some_problem'] = Testing a problem")) );
        eff.apply((EntityLocal)entity);
        entity.invoke(entity.getEntityType().getEffectorByName("make-problem").get(), null).getUnchecked();

        Time.sleep(Duration.ONE_SECOND);
        Dumper.dumpInfo(entity);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);

        eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "fix-problem")
                .configure(WorkflowEffector.STEPS, MutableList.of("set-sensor service.problems = {}")) );
//                .configure(WorkflowEffector.STEPS, MutableList.of("clear-sensor service.problems['some_problem']")) );
        eff.apply((EntityLocal)entity);
        entity.invoke(entity.getEntityType().getEffectorByName("fix-problem").get(), null).getUnchecked();

        Time.sleep(Duration.ONE_SECOND);
        Dumper.dumpInfo(entity);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
    }

}
