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
package org.apache.brooklyn.camp.yoml.demos;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.yoml.types.YomlInitializers;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class YomlSensorEffectorTest extends AbstractYamlTest {

    private static final Logger log = LoggerFactory.getLogger(YomlSensorEffectorTest.class);

    protected EmptySoftwareProcess startApp() throws Exception {
        YomlInitializers.install(mgmt());
        
        Entity app = createAndStartApplication(loadYaml("ssh-sensor-effector-yoml-demo.yaml"));
        waitForApplicationTasks(app);
        
        log.info("App started:");
        Entities.dumpInfo(app);
        
        EmptySoftwareProcess entity = (EmptySoftwareProcess) app.getChildren().iterator().next();
        return entity;
    }
    
    @Test(groups="Integration")
    public void testBasicEffector() throws Exception {
        EmptySoftwareProcess entity = startApp();
        
        String name = entity.getConfig(ConfigKeys.newStringConfigKey("name"));
        Assert.assertEquals(name, "bob");
        
        Task<String> hi = entity.invoke(Effectors.effector(String.class, "echo-hi").buildAbstract(), MutableMap.<String,Object>of());
        Assert.assertEquals(hi.get().trim(), "hi");
    }

    @Test(groups="Integration")
    public void testConfigEffector() throws Exception {
        EmptySoftwareProcess entity = startApp();
        
        String name = entity.getConfig(ConfigKeys.newStringConfigKey("name"));
        Assert.assertEquals(name, "bob");
        
        Task<String> hi = entity.invoke(Effectors.effector(String.class, "echo-hi-name-from-config").buildAbstract(), MutableMap.<String,Object>of());
        Assert.assertEquals(hi.get().trim(), "hi bob");
    }

    @Test(groups="Integration")
    public void testSensorAndEffector() throws Exception {
        EmptySoftwareProcess entity = startApp();
        
        EntityAsserts.assertAttributeChangesEventually(entity, Sensors.newStringSensor("date"));
    }
    
}