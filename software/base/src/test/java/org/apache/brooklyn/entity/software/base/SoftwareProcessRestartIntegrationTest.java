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

import java.util.List;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class SoftwareProcessRestartIntegrationTest extends BrooklynAppUnitTestSupport {

    @DataProvider(name = "errorPhase")
    public Object[][] errorPhases() {
        return new Object[][]{
                {SoftwareProcess.PRE_LAUNCH_COMMAND},
                {VanillaSoftwareProcess.LAUNCH_COMMAND},
                {SoftwareProcess.POST_LAUNCH_COMMAND},
        };
    }

    @Test(dataProvider = "errorPhase", groups = "Integration")
    public void testEntityOnFireAfterRestartingWhenLaunchCommandFails(ConfigKey<String> key) {
        VanillaSoftwareProcess entity = app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "true")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "true")
                .configure(key, "exit 1"));
        try {
            app.start(ImmutableList.of(app.newLocalhostProvisioningLocation()));
            Asserts.shouldHaveFailedPreviously("entity has launch command that does not complete successfully");
        } catch (Exception e) {
            // expected
        }

        LastTwoServiceStatesListener listener = new LastTwoServiceStatesListener();
        entity.subscriptions().subscribe(entity, Attributes.SERVICE_STATE_ACTUAL, listener);

        EntityAsserts.assertAttributeEquals(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        entity.invoke(Startable.RESTART, ImmutableMap.<String, Object>of(
                SoftwareProcess.RestartSoftwareParameters.RESTART_CHILDREN.getName(), false,
                SoftwareProcess.RestartSoftwareParameters.RESTART_MACHINE.getName(), false));

        List<Lifecycle> expected = ImmutableList.of(Lifecycle.STARTING, Lifecycle.ON_FIRE);
        Asserts.eventually(listener, Predicates.equalTo(expected));
    }

    public static class LastTwoServiceStatesListener implements SensorEventListener<Lifecycle>, Supplier<List<Lifecycle>> {
        final Lifecycle[] events = new Lifecycle[2];

        @Override
        public void onEvent(SensorEvent<Lifecycle> event) {
            synchronized (events) {
                events[0] = events[1];
                events[1] = event.getValue();
            }
        }

        @Override
        public List<Lifecycle> get() {
            synchronized (events) {
                return Lists.newArrayList(events);
            }
        }
    }


}
