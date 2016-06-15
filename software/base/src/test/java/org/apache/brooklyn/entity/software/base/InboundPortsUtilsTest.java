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

import java.util.Collection;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.sensor.PortAttributeSensorAndConfigKey;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

public class InboundPortsUtilsTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testGetRequiredOpenPortsGetsDynamicallyAddedKeys() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        Collection<Integer> defaultRequiredOpenPorts = InboundPortsUtils.getRequiredOpenPorts(entity, ImmutableSet.<ConfigKey<?>>of(), true, null);
        Assert.assertEquals(defaultRequiredOpenPorts, ImmutableSet.of(), "Expected no ports");
        ConfigKey<Integer> newTestConfigKeyPort = ConfigKeys.newIntegerConfigKey("new.test.config.key.port");
        ConfigKey<String> newTestConfigKeyString = ConfigKeys.newStringConfigKey("new.test.config.key.string");
        entity.config().set(newTestConfigKeyPort, 9999);
        entity.config().set(newTestConfigKeyString, "foo.bar");
        Collection<Integer> dynamicRequiredOpenPorts = InboundPortsUtils.getRequiredOpenPorts(entity, ImmutableSet.<ConfigKey<?>>of(), true, null);
        Assert.assertEquals(dynamicRequiredOpenPorts, ImmutableSet.of(9999), "Expected new port to be added");
    }

    @Test
    public void testGetRequiredOpenPortsGetsDynamicallyAddedPortBasedKeys() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        PortAttributeSensorAndConfigKey newTestConfigKeyPort = ConfigKeys.newPortSensorAndConfigKey("new.test.config.port.string.first", "port", "7777+");
        PortAttributeSensorAndConfigKey newTestConfigKeyPort2 = ConfigKeys.newPortSensorAndConfigKey("new.test.config.port.string.second", "port");

        ConfigKey<Object> newTestConfigKeyObject = ConfigKeys.newConfigKey(Object.class, "new.test.config.object");
        ConfigKey<String> newTestConfigKeyString = ConfigKeys.newStringConfigKey("new.test.config.key.string");
        entity.config().set(newTestConfigKeyPort, PortRanges.fromString("8888+"));
        entity.config().set(newTestConfigKeyPort2, PortRanges.fromInteger(9999));
        entity.config().set(newTestConfigKeyObject, PortRanges.fromInteger(2222));
        entity.config().set(newTestConfigKeyString, "foo.bar");

        Collection<Integer> dynamicRequiredOpenPorts = InboundPortsUtils.getRequiredOpenPorts(entity, ImmutableSet.<ConfigKey<?>>of(), true, null);
        Assert.assertTrue(dynamicRequiredOpenPorts.contains(8888));
        Assert.assertTrue(dynamicRequiredOpenPorts.contains(9999));
        Assert.assertTrue(dynamicRequiredOpenPorts.contains(2222));
    }


}
