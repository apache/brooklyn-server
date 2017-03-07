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

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityInternal.FeedSupport;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.feed.CommandPollConfig;
import org.apache.brooklyn.feed.ssh.SshPollValue;
import org.apache.brooklyn.feed.ssh.SshValueFunctions;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringFunctions;
import org.apache.brooklyn.util.text.StringPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test is almost identical to {@link org.apache.brooklyn.feed.ssh.SshFeedIntegrationTest}.
 * To launch the test I put in ~/.brooklyn/brooklyn.properties
 *   brooklyn.location.named.WindowsLiveTest=byon:(hosts=192.168.1.2,osFamily=windows,user=winUser,password=p0ssw0rd)
 */
public class WinRmFeedIntegrationTest extends BrooklynAppLiveTestSupport {

    private static final Logger log = LoggerFactory.getLogger(WinRmFeedIntegrationTest.class);

    private static final String LOCATION_SPEC = "named:WindowsLiveTest";
    
    final static AttributeSensor<String> SENSOR_STRING = Sensors.newStringSensor("aString", "");
    final static AttributeSensor<Integer> SENSOR_INT = Sensors.newIntegerSensor("aLong", "");

    private WinRmMachineLocation machine;
    private TestEntity entity;
    private CmdFeed feed;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        MachineProvisioningLocation<?> provisioningLocation = (MachineProvisioningLocation<?>)
                mgmt.getLocationRegistry().getLocationManaged(LOCATION_SPEC);
        machine = (WinRmMachineLocation)provisioningLocation.obtain(ImmutableMap.of());
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        app.start(ImmutableList.of(machine));
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        if (feed != null) feed.stop();
        super.tearDown();
    }
    
    /** this is one of the most common pattern */
    @Test(groups="Integration")
    public void testReturnsStdoutAndInfersMachine() throws Exception {
        final TestEntity entity2 = app.createAndManageChild(EntitySpec.create(TestEntity.class)
            // inject the machine location, because the app was started with a provisioning location
            // and TestEntity doesn't provision
            .location(machine));
        
        feed = CmdFeed.builder()
                .entity(entity2)
                .poll(new CommandPollConfig<String>(SENSOR_STRING)
                        .command("echo hello")
                        .onSuccess(SshValueFunctions.stdout()))
                .build();
        
        EntityAsserts.assertAttributeEventuallyNonNull(entity2, SENSOR_STRING);
        String val = entity2.getAttribute(SENSOR_STRING);
        Assert.assertTrue(val.contains("hello"), "val="+val);
        Assert.assertEquals(val.trim(), "hello");
    }

    @Test(groups="Integration")
    public void testFeedDeDupe() throws Exception {
        testReturnsStdoutAndInfersMachine();
        entity.addFeed(feed);
        log.info("Feed 0 is: "+feed);
        
        testReturnsStdoutAndInfersMachine();
        log.info("Feed 1 is: "+feed);
        entity.addFeed(feed);
                
        FeedSupport feeds = ((EntityInternal)entity).feeds();
        Assert.assertEquals(feeds.getFeeds().size(), 1, "Wrong feed count: "+feeds.getFeeds());
    }
    
    @Test(groups="Integration")
    public void testReturnsSshExitStatus() throws Exception {
        feed = CmdFeed.builder()
                .entity(entity)
                .machine(machine)
                .poll(new CommandPollConfig<Integer>(SENSOR_INT)
                        .command("exit 123")
                        .checkSuccess(Predicates.alwaysTrue())
                        .onSuccess(SshValueFunctions.exitStatus()))
                .build();

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_INT, 123);
    }
    
    @Test(groups="Integration")
    public void testReturnsStdout() throws Exception {
        feed = CmdFeed.builder()
                .entity(entity)
                .machine(machine)
                .poll(new CommandPollConfig<String>(SENSOR_STRING)
                        .command("echo hello")
                        .onSuccess(SshValueFunctions.stdout()))
                .build();
        
        EntityAsserts.assertAttributeEventually(entity, SENSOR_STRING, 
            Predicates.compose(Predicates.equalTo("hello"), StringFunctions.trim()));
    }

    @Test(groups="Integration")
    public void testReturnsStderr() throws Exception {
        final String cmd = "thiscommanddoesnotexist";
        
        feed = CmdFeed.builder()
                .entity(entity)
                .machine(machine)
                .poll(new CommandPollConfig<String>(SENSOR_STRING)
                        .command(cmd)
                        .onFailure(SshValueFunctions.stderr()))
                .build();
        
        EntityAsserts.assertAttributeEventually(entity, SENSOR_STRING, StringPredicates.containsLiteral(cmd));
    }
    
    @Test(groups="Integration")
    public void testFailsOnNonZero() throws Exception {
        feed = CmdFeed.builder()
                .entity(entity)
                .machine(machine)
                .poll(new CommandPollConfig<String>(SENSOR_STRING)
                        .command("exit 123")
                        .onFailure(new Function<SshPollValue, String>() {
                            @Override
                            public String apply(SshPollValue input) {
                                return "Exit status " + input.getExitStatus();
                            }}))
                .build();
        
        EntityAsserts.assertAttributeEventually(entity, SENSOR_STRING, StringPredicates.containsLiteral("Exit status 123"));
    }
    
    @Test(groups="Integration")
    public void testAddedEarly() throws Exception {
        final TestEntity entity2 = app.addChild(EntitySpec.create(TestEntity.class)
            .location(machine)
            .addInitializer(new EntityInitializer() {
                @Override
                public void apply(EntityLocal entity) {
                    CmdFeed.builder()
                        .entity(entity)
                        .onlyIfServiceUp()
                        .poll(new CommandPollConfig<String>(SENSOR_STRING)
                            .command("echo hello")
                            .onSuccess(SshValueFunctions.stdout()))
                        .build();
                }
            }));

        // TODO would be nice to hook in and assert no errors
        EntityAsserts.assertAttributeEqualsContinually(entity2, SENSOR_STRING, null);

        entity2.sensors().set(Attributes.SERVICE_UP, true);
        EntityAsserts.assertAttributeEventually(entity2, SENSOR_STRING, StringPredicates.containsLiteral("hello"));
    }

    
    @Test(groups="Integration")
    public void testDynamicEnvAndCommandSupplier() throws Exception {
        final TestEntity entity2 = app.createAndManageChild(EntitySpec.create(TestEntity.class).location(machine));
        
        final AtomicInteger count = new AtomicInteger();
        Supplier<Map<String, String>> envSupplier = new Supplier<Map<String,String>>() {
            @Override
            public Map<String, String> get() {
                return MutableMap.of("COUNT", ""+count.incrementAndGet());
            }
        };
        Supplier<String> cmdSupplier = new Supplier<String>() {
            @Override
            public String get() {
                return "echo count-"+count.incrementAndGet()+"-%COUNT%";
            }
        };
        
        feed = CmdFeed.builder()
                .entity(entity2)
                .poll(new CommandPollConfig<String>(SENSOR_STRING)
                        .env(envSupplier)
                        .command(cmdSupplier)
                        .onSuccess(SshValueFunctions.stdout()))
                .build();
        
        EntityAsserts.assertAttributeEventuallyNonNull(entity2, SENSOR_STRING);
        final String val1 = assertDifferentOneInOutput(entity2);
        
        EntityAsserts.assertAttributeEventually(entity2, SENSOR_STRING, Predicates.not(Predicates.equalTo(val1)));        
        final String val2 = assertDifferentOneInOutput(entity2);
        log.info("vals from dynamic sensors are: "+val1.trim()+" and "+val2.trim());
    }

    private String assertDifferentOneInOutput(final TestEntity entity2) {
        String val = entity2.getAttribute(SENSOR_STRING);
        Assert.assertTrue(val.startsWith("count"), "val="+val);
        try {
            String[] fields = val.trim().split("-");
            int field1 = Integer.parseInt(fields[1]); 
            int field2 = Integer.parseInt(fields[2]);
            Assert.assertEquals(Math.abs(field2-field1), 1, "expected difference of 1");
        } catch (Throwable t) {
            Exceptions.propagateIfFatal(t);
            Assert.fail("Wrong output from sensor, got '"+val.trim()+"', giving error: "+t);
        }
        return val;
    }

}
