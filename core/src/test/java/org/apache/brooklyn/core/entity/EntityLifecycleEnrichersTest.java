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
package org.apache.brooklyn.core.entity;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class EntityLifecycleEnrichersTest extends BrooklynAppUnitTestSupport {

    private ListeningExecutorService executor;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (executor != null) executor.shutdownNow();
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-291
    @Test
    public void testManuallySettingServiceStateIsNotOverwritten() throws Exception {
        List<ListenableFuture<?>> futures = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            ListenableFuture<Void> future = executor.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class));
                    entity.sensors().set(TestEntity.SERVICE_UP, true);
                    entity.sensors().set(TestEntity.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
                    Thread.sleep(10);
                    assertEquals(entity.sensors().get(TestEntity.SERVICE_UP), Boolean.TRUE);
                    assertEquals(entity.sensors().get(TestEntity.SERVICE_STATE_ACTUAL), Lifecycle.RUNNING);
                    Entities.unmanage(entity);
                    return null;
                }});
            futures.add(future);
        }
        Futures.allAsList(futures).get();
    }
}
