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
package org.apache.brooklyn.util.core.task;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Callables;

public class TaskPredicatesTest extends BrooklynAppUnitTestSupport {

    private ExecutionManager execManager;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        execManager = mgmt.getExecutionManager();
    }

    @Test
    public void testDisplayNameEqualTo() throws Exception {
        Task<Object> task = execManager.submit(TaskBuilder.builder()
                .body(Callables.<Object>returning("val"))
                .displayName("myname")
                .build());
        assertTrue(TaskPredicates.displayNameEqualTo("myname").apply(task));
        assertFalse(TaskPredicates.displayNameEqualTo("wrong").apply(task));
    }
    
    @Test
    public void testDisplayNameMatches() throws Exception {
        Task<Object> task = execManager.submit(TaskBuilder.builder()
                .body(Callables.<Object>returning("val"))
                .displayName("myname")
                .build());
        assertTrue(TaskPredicates.displayNameSatisfies(Predicates.equalTo("myname")).apply(task));
        assertFalse(TaskPredicates.displayNameSatisfies(Predicates.equalTo("wrong")).apply(task));
    }
    
    @Test
    public void testDisplayNameSatisfies() throws Exception {
        Task<Object> task = execManager.submit(TaskBuilder.builder()
                .body(Callables.<Object>returning("val"))
                .displayName("myname")
                .build());
        assertTrue(TaskPredicates.displayNameSatisfies(Predicates.equalTo("myname")).apply(task));
        assertFalse(TaskPredicates.displayNameSatisfies(Predicates.equalTo("wrong")).apply(task));
    }
    
    @Test
    public void testIsDone() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Task<?> task = app.getExecutionContext().submit("await latch", () -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            });
        
        assertFalse(TaskPredicates.isDone().apply(task));
        
        latch.countDown();
        task.get();
        assertTrue(TaskPredicates.isDone().apply(task));
    }
    
    @Test
    public void testHasTag() throws Exception {
        Task<?> task = execManager.submit(TaskBuilder.<Object>builder()
                .body(Callables.<Object>returning("val"))
                .tag("mytag")
                .build());
        assertTrue(TaskPredicates.hasTag("mytag").apply(task));
        assertFalse(TaskPredicates.hasTag("wrongtag").apply(task));
    }
    
    @Test
    public void testIsEffector() throws Exception {
        Task<?> task = app.invoke(TestApplication.START, ImmutableMap.of("locations", ImmutableList.<Location>of()));
        Task<?> otherTask = execManager.submit(TaskBuilder.<Object>builder()
                .body(Callables.<Object>returning("val"))
                .build());
        assertTrue(TaskPredicates.isEffector().apply(task));
        assertFalse(TaskPredicates.isEffector().apply(otherTask));
        
    }
    
    @Test
    public void testIsTransient() throws Exception {
        Task<?> task = execManager.submit(TaskBuilder.<Object>builder()
                .body(Callables.<Object>returning("val"))
                .build());
        assertFalse(TaskPredicates.isTransient().apply(task));
        
        BrooklynTaskTags.setTransient(task);
        assertTrue(TaskPredicates.isTransient().apply(task));
    }
    
    @Test
    public void testIsInessential() throws Exception {
        Task<?> task = execManager.submit(TaskBuilder.<Object>builder()
                .body(Callables.<Object>returning("val"))
                .build());
        assertFalse(TaskPredicates.isInessential().apply(task));
        
        BrooklynTaskTags.setInessential(task);
        assertTrue(TaskPredicates.isInessential().apply(task));
    }
}
