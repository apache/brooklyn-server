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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.entity.drivers.DriverDependentEntity;
import org.apache.brooklyn.api.entity.drivers.EntityDriver;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.paas.PaasLocation;
import org.apache.brooklyn.location.paas.TestPaasLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.reflect.TypeToken;

public class ReflectiveEntityDriverFactoryTest extends BrooklynAppUnitTestSupport {

    private ReflectiveEntityDriverFactory factory;
    private SshMachineLocation sshLocation;
    private PaasLocation paasLocation;
    private MyDriverDependentEntity entity;
    
    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
        sshLocation = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", "localhost"));
        paasLocation = mgmt.getLocationManager().createLocation(LocationSpec.create(TestPaasLocation.class));
        factory = new ReflectiveEntityDriverFactory();
        entity = app.addChild(EntitySpec.create(MyDriverDependentEntity.class)
                .configure(MyDriverDependentEntity.DRIVER_CLASS, MyDriver.class));
    }

    protected void assertDriverIs(Class<?> clazz, Location location) {
        MyDriver driver = (MyDriver) factory.build(entity, location);
        assertTrue(driver.getClass().equals(clazz), "driver="+driver+"; should be "+clazz);
    }
    
    @Test
    public void testInstantiatesSshDriver() throws Exception {
        assertDriverIs(MySshDriver.class, sshLocation);
    }

    @Test
    public void testInstantiatesPaasDriver() throws Exception {
        assertDriverIs(MyTestPaasDriver.class, paasLocation);
    }
    
    @Test
    public void testFullNameMapping() throws Exception {
        factory.addClassFullNameMapping(MyDriver.class.getName(), MyCustomDriver.class.getName());
        assertDriverIs(MyCustomDriver.class, sshLocation);
    }

    @Test
    public void testFullNameMappingMulti() throws Exception {
        factory.addClassFullNameMapping(MyDriver.class.getName(), "X");
        factory.addClassFullNameMapping(MyDriver.class.getName(), MyCustomDriver.class.getName());
        assertDriverIs(MyCustomDriver.class, sshLocation);
    }


    @Test
    public void testFullNameMappingFailure1() throws Exception {
        factory.addClassFullNameMapping(MyDriver.class.getName()+"X", MyCustomDriver.class.getName());
        assertDriverIs(MySshDriver.class, sshLocation);
    }

    @Test
    public void testFullNameMappingFailure2() throws Exception {
        factory.addClassFullNameMapping(MyDriver.class.getName(), MyCustomDriver.class.getName());
        factory.addClassFullNameMapping(MyDriver.class.getName(), "X");
        assertDriverIs(MySshDriver.class, sshLocation);
    }

    @Test
    public void testSimpleNameMapping() throws Exception {
        factory.addClassSimpleNameMapping(MyDriver.class.getSimpleName(), MyCustomDriver.class.getSimpleName());
        assertDriverIs(MyCustomDriver.class, sshLocation);
    }

    @Test
    public void testSimpleNameMappingFailure() throws Exception {
        factory.addClassSimpleNameMapping(MyDriver.class.getSimpleName()+"X", MyCustomDriver.class.getSimpleName());
        assertDriverIs(MySshDriver.class, sshLocation);
    }

    @ImplementedBy(MyDriverDependentEntityImpl.class)
    public static interface MyDriverDependentEntity extends Entity, DriverDependentEntity<EntityDriver> {
        @SuppressWarnings("serial")
        ConfigKey<Class<? extends EntityDriver>> DRIVER_CLASS = ConfigKeys.newConfigKey(
                new TypeToken<Class<? extends EntityDriver>>() {}, 
                "driverClass");
    }
    
    public static class MyDriverDependentEntityImpl extends AbstractEntity implements MyDriverDependentEntity {
        @Override
        @SuppressWarnings("unchecked")
        public Class<EntityDriver> getDriverInterface() {
            return (Class<EntityDriver>) config().get(DRIVER_CLASS);
        }
        
        @Override
        public EntityDriver getDriver() {
            throw new UnsupportedOperationException();
        }
    }
    
    public static interface MyDriver extends EntityDriver {
    }
    
    public static class MySshDriver implements MyDriver {
        public MySshDriver(Entity entity, SshMachineLocation machine) {
        }

        @Override
        public Location getLocation() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EntityLocal getEntity() {
            throw new UnsupportedOperationException();
        }
    }
    
    public static class MyCustomDriver extends MySshDriver {
        public MyCustomDriver(Entity entity, SshMachineLocation machine) {
            super(entity, machine);
        }
    }
    
    public static class MyTestPaasDriver implements MyDriver {
        public MyTestPaasDriver(Entity entity, PaasLocation location) {
        }

        @Override
        public Location getLocation() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EntityLocal getEntity() {
            throw new UnsupportedOperationException();
        }
    }
}
