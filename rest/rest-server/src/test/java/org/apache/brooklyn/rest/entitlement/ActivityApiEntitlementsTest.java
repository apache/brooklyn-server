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
package org.apache.brooklyn.rest.entitlement;

import static org.testng.Assert.assertEquals;

import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementClass;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementManager;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.entity.machine.MachineEntity;
import org.apache.brooklyn.entity.software.base.AbstractSoftwareProcessStreamsTest;
import org.apache.brooklyn.util.core.task.TaskPredicates;
import org.apache.brooklyn.util.text.StringPredicates;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

@Test(singleThreaded = true)
public class ActivityApiEntitlementsTest extends AbstractRestApiEntitlementsTest {

    protected MachineEntity machineEntity;
    protected Task<?> subTask;
    protected Map<String, String> streams;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        machineEntity = app.addChild(EntitySpec.create(MachineEntity.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
                .location(TestApplication.LOCALHOST_PROVISIONER_SPEC));
        machineEntity.start(ImmutableList.<Location>of());
        
        machineEntity.execCommand("echo myval");
        
        Set<Task<?>> tasks = BrooklynTaskTags.getTasksInEntityContext(mgmt.getExecutionManager(), machineEntity);
        String taskNameRegex = "ssh: echo myval";
        
        streams = Maps.newLinkedHashMap();
        subTask = AbstractSoftwareProcessStreamsTest.findTaskOrSubTask(tasks, TaskPredicates.displayNameSatisfies(StringPredicates.matchesRegex(taskNameRegex))).get();
        for (String streamId : ImmutableList.of("stdin", "stdout", "stderr")) {
            streams.put(streamId, AbstractSoftwareProcessStreamsTest.getStreamOrFail(subTask, streamId));
        }
    }
    
    @Test(groups = "Integration")
    public void testGetTask() throws Exception {
        String path = "/v1/activities/"+subTask.getId();
        assert401(path);
        assertPermitted("myRoot", path);
        assertPermitted("myUser", path);
        assertPermitted("myReadonly", path);
        assertForbidden("myMinimal", path);
        assertForbidden("unrecognisedUser", path);
    }
    
    @Test(groups = "Integration")
    public void testGetStream() throws Exception {
        String pathPrefix = "/v1/activities/"+subTask.getId()+"/stream/";
        for (Map.Entry<String, String> entry : streams.entrySet()) {
            String streamId = entry.getKey();
            String expectedStream = entry.getValue();
            String path = pathPrefix+streamId;

            assert401(path);
            assertEquals(httpGet("myRoot", path), expectedStream);
            assertEquals(httpGet("myUser", path), expectedStream);
            assertEquals(httpGet("myReadonly", path), expectedStream);
            assertForbidden("myMinimal", path);
            assertForbidden("unrecognisedUser", path);
            
            StaticDelegatingEntitlementManager.setDelegate(new SeeSelectiveStreams(streamId));
            assertEquals(httpGet("myCustom", path), expectedStream);
            
            StaticDelegatingEntitlementManager.setDelegate(new SeeSelectiveStreams("differentStreamId"));
            assertForbidden("myCustom", path);
        }
    }
    
    public static class SeeSelectiveStreams implements EntitlementManager {
        private final String regex;
        
        public SeeSelectiveStreams(String regex) {
            this.regex = regex;
        }
        @Override 
        @SuppressWarnings("unchecked")
        public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> entitlementClass, T entitlementClassArgument) {
            String type = entitlementClass.entitlementClassIdentifier();
            if (Entitlements.SEE_ACTIVITY_STREAMS.entitlementClassIdentifier().equals(type)) {
                Entitlements.TaskAndItem<String> pair = (Entitlements.TaskAndItem<String>) entitlementClassArgument;
                return pair.getItem().matches(regex);
            } else {
                return true;
            }
        }
    }
}