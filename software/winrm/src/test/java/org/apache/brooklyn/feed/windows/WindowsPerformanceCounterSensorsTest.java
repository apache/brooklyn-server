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
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.windows.WindowsPerformanceCounterSensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class WindowsPerformanceCounterSensorsTest extends RebindTestFixtureWithApp {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(WindowsPerformanceCounterFeedTest.class);

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
        String response = generateCounterReponse("mycounter", "myval");
        RecordingWinRmTool.setCustomResponse(".*mycounter.*", new RecordingWinRmTool.CustomResponse(0, response, ""));
                
        Entity entity = app().createAndManageChild(EntitySpec.create(TestEntity.class)
                .addInitializer(new WindowsPerformanceCounterSensors(ConfigBag.newInstance(ImmutableMap.of(
                        WindowsPerformanceCounterSensors.PERIOD, "1ms",
                        WindowsPerformanceCounterSensors.PERFORMANCE_COUNTERS, ImmutableSet.of(
                                ImmutableMap.of(
                                        "name", "mysensor",
                                        "sensorType", java.lang.String.class.getName(), //FIXME
                                        "counter", "\\mycounter")))))));
        
        app().start(ImmutableList.of(loc));
        
        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor("mysensor"), "myval");
        
        rebind();
        
        String response2 = generateCounterReponse("mycounter", "myval2");
        RecordingWinRmTool.setCustomResponse(".*mycounter.*", new RecordingWinRmTool.CustomResponse(0, response2, ""));
        EntityAsserts.assertAttributeEqualsEventually(entity, Sensors.newStringSensor("mysensor"), "myval2");
    }
    
    private String generateCounterReponse(String counterName, String val) {
        String firstPart = "\\\\machine.name\\" + counterName;
        return new StringBuilder()
                .append(firstPart)
                .append(Strings.repeat(" ", 200 - (firstPart.length() + val.length())))
                .append(val)
                .toString();
    }
}
