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
package org.apache.brooklyn.entity.machine;

import static org.testng.Assert.fail;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsRebindStubUnitTest;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecParams;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class MachineEntityJcloudsRebindTest extends JcloudsRebindStubUnitTest {

    //TODO To decrease noisy erroneous warnings, could also stub the response for
    // "free | grep Mem" etc in initSshCustomResponses()

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        initSshCustomResponses();
    }

    private void initSshCustomResponses() {
        RecordingSshTool.setCustomResponse("cat /proc/uptime", new RecordingSshTool.CustomResponseGenerator() {
            private final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public CustomResponse generate(ExecParams execParams) throws Exception {
                return new CustomResponse(0, Integer.toString(counter.getAndIncrement()), "");
            }});
        RecordingSshTool.setCustomResponse(".*/etc/os-release.*", new RecordingSshTool.CustomResponseGenerator() {
            @Override
            public CustomResponse generate(ExecParams execParams) throws Exception {
                String stdout = Joiner.on("\n").join(
                        "name:centos",
                        "version:7.0",
                        "architecture:myarch",
                        "ram:1024",
                        "cpus:1");
                return new CustomResponse(0, stdout, "");
            }});
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-425
    @Test
    @Override
    public void testRebind() throws Exception {
        this.nodeCreator = newNodeCreator();
        this.computeServiceRegistry = new StubbedComputeServiceRegistry(nodeCreator, false);
        JcloudsLocation origJcloudsLoc = newJcloudsLocation(computeServiceRegistry);
    
        MachineEntity machine = origApp.createAndManageChild(EntitySpec.create(MachineEntity.class)
                .configure(MachineEntity.MAXIMUM_REBIND_SENSOR_CONNECT_DELAY, Duration.millis(100)));
        origApp.start(ImmutableList.of(origJcloudsLoc));
        EntityAsserts.assertAttributeEqualsEventually(machine, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        
        RecordingSshTool.clear();
        initSshCustomResponses();
        rebind();
        
        Entity newMachine = mgmt().getEntityManager().getEntity(machine.getId());
        EntityAsserts.assertAttributeEqualsEventually(newMachine, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        
        // Expect SshMachineLocation.inferMachineDetails to have successfully retrieved os details,
        // which we've stubbed to return centos (in ssh call).
        assertRecordedSshCmdContainsEventually("/etc/os-release");
        
        // TODO Would like to assert that we have the feed, but it's not actually added to the entity!
        // See AddMachineMetrics.createMachineMetricsFeed, which doesn't call `feeds().addFeed()` so
        // it's not persisted and is not accessible from entity.feeds().getFeeds(). Instead, it just
        // adds the entity to the feed (which is the old way, for if your feed is not persistable).
        //     assertHasFeedEventually(newMachine, "machineMetricsFeed");

        // TODO AddMachineMetrics.createMachineMetricsFeed poll period is not configurable; 
        // we'd have to wait 30 seconds for a change.
        //     EntityAsserts.assertAttributeChangesEventually(newMachine, MachineAttributes.UPTIME);
    }
    
    private void assertRecordedSshCmdContainsEventually(final String expected) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                List<ExecCmd> cmds = RecordingSshTool.getExecCmds();
                for (ExecCmd cmd : cmds) {
                    if (cmd.commands.toString().contains(expected)) {
                        return;
                    }
                }
                fail("Commands (" + expected + ") not contain in " + cmds);
            }});
    }
    
    @SuppressWarnings("unused")
    private void assertHasFeedEventually(final Entity entity, final String uniqueTag) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                Collection<Feed> feeds = ((EntityInternal)entity).feeds().getFeeds();
                for (Feed feed : feeds) {
                    if (uniqueTag.equals(feed.getUniqueTag())) {
                        return;
                    }
                }
                fail("No feed found with uniqueTag "+uniqueTag+" in entity "+entity+"; feeds="+feeds);
            }});
    }
}
