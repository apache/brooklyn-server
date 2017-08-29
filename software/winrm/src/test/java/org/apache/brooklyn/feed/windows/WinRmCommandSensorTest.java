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
package org.apache.brooklyn.feed.windows;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.http.HttpRequestSensor;
import org.apache.brooklyn.core.sensor.windows.WinRmCommandSensor;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class WinRmCommandSensorTest extends RebindTestFixtureWithApp {

    private Location loc;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = mgmt().getLocationManager().createLocation(LocationSpec.create(WinRmMachineLocation.class)
                .configure("address", "1.2.3.4")
                .configure(WinRmMachineLocation.WINRM_TOOL_CLASS, RecordingWinRmTool.class.getName()));
        
        RecordingWinRmTool.clear();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingWinRmTool.clear();
    }

    @Test
    public void testRebind() throws Exception {
        RecordingWinRmTool.setCustomResponse(".*mycommand.*", new RecordingWinRmTool.CustomResponse(0, "myval", ""));
        
        Entity entity = app().createAndManageChild(EntitySpec.create(TestEntity.class)
                .addInitializer(new WinRmCommandSensor<String>(ConfigBag.newInstance(ImmutableMap.of(
                        WinRmCommandSensor.SENSOR_PERIOD, "1ms",
                        WinRmCommandSensor.SENSOR_COMMAND, "mycommand",
                        WinRmCommandSensor.SENSOR_NAME, "mysensor")))));
        
        app().start(ImmutableList.of(loc));

        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor("mysensor"), "myval");
        
        rebind();
        
        RecordingWinRmTool.setCustomResponse(".*mycommand.*", new RecordingWinRmTool.CustomResponse(0, "myval2", ""));
        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor("mysensor"), "myval2");
    }
    
    // "Integration" because takes a second 
    @Test(groups="Integration")
    public void testSupressingDuplicates() throws Exception {
        AttributeSensor<String> mySensor = Sensors.newStringSensor("mysensor");
        
        RecordingSensorEventListener<String> listener = new RecordingSensorEventListener<>();
        app().subscriptions().subscribe(null, mySensor, listener);
        
        RecordingWinRmTool.setCustomResponse(".*mycommand.*", new RecordingWinRmTool.CustomResponse(0, "myval", ""));
        
        Entity entity = app().createAndManageChild(EntitySpec.create(TestEntity.class)
                .addInitializer(new WinRmCommandSensor<String>(ConfigBag.newInstance(ImmutableMap.of(
                        HttpRequestSensor.SUPPRESS_DUPLICATES, true,
                        WinRmCommandSensor.SENSOR_PERIOD, "1ms",
                        WinRmCommandSensor.SENSOR_COMMAND, "mycommand",
                        WinRmCommandSensor.SENSOR_NAME, mySensor.getName())))));
        
        app().start(ImmutableList.of(loc));

        EntityAsserts.assertAttributeEqualsEventually(entity, mySensor, "myval");
        listener.assertHasEventEventually(Predicates.alwaysTrue());
        
        Asserts.succeedsContinually(new Runnable() {
            @Override public void run() {
                listener.assertEventCount(1);
            }});
    }
}
