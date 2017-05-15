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
package org.apache.brooklyn.core.entity.drivers;

import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.drivers.EntityDriver;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.drivers.ReflectiveEntityDriverFactoryTest.MyDriver;
import org.apache.brooklyn.core.entity.drivers.ReflectiveEntityDriverFactoryTest.MyDriverDependentEntity;
import org.apache.brooklyn.core.entity.drivers.RegistryEntityDriverFactoryTest.MyOtherSshDriver;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityDriverRegistryTest extends BrooklynAppUnitTestSupport {

    private SshMachineLocation sshLocation;
    private MyDriverDependentEntity entity;
    
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
        sshLocation = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", "localhost"));
        entity = app.addChild(EntitySpec.create(MyDriverDependentEntity.class)
                .configure(MyDriverDependentEntity.DRIVER_CLASS, MyDriver.class));
    }

    @Test
    public void testInstantiatesRegisteredDriver() throws Exception {
        mgmt.getEntityDriverManager().registerDriver(MyDriver.class, SshMachineLocation.class, MyOtherSshDriver.class);
        EntityDriver driver = mgmt.getEntityDriverManager().build(entity, sshLocation);
        assertTrue(driver instanceof MyOtherSshDriver);
    }
}
