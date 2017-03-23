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
package org.apache.brooklyn.entity.software.base.test.core.mgmt.usage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

import java.util.List;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalUsageManagerTest.RecordingStaticUsageListener;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.mgmt.usage.UsageManager;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class ApplicationUsageTrackingRebindTest extends RebindTestFixture<TestApplication> {

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        RecordingStaticUsageListener.clearInstances();
        super.setUp();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            RecordingStaticUsageListener.clearInstances();
        }
    }

    @Override
    protected TestApplication createApp() {
        return null; // no-op
    }
    
    @Override
    protected BrooklynProperties createBrooklynProperties() {
        // TODO Need tests in brooklyn that can register multiple usage-listeners
        // (e.g. for metering and for amp-cluster).
        BrooklynProperties result = BrooklynProperties.Factory.newEmpty();
        result.put(UsageManager.USAGE_LISTENERS, RecordingStaticUsageListener.class.getName());
        return result;
    }
    
    @Test
    public void testUsageListenerReceivesEventsAfterRebind() throws Exception {
        final RecordingStaticUsageListener origListener = RecordingStaticUsageListener.getLastInstance();

        // Expect CREATED
        final TestApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = origListener.getApplicationEvents();
                assertEquals(events.size(), 1, "events="+events);
                origListener.assertAppEvent(0, app, Lifecycle.CREATED, "events="+events);
            }});
        
        // After rebind, expect it to have a new listener
        rebind();
        final TestApplication newApp = (TestApplication) mgmt().getEntityManager().getEntity(app.getId());
        final RecordingStaticUsageListener newListener = RecordingStaticUsageListener.getLastInstance();
        assertNotSame(origListener, newListener);
        
        // Expect STARTING and RUNNING on the new listener (but not CREATED again)
        newApp.start(ImmutableList.<Location>of());

        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = newListener.getApplicationEvents();
                assertEquals(events.size(), 2, "events="+events);
                newListener.assertAppEvent(0, newApp, Lifecycle.STARTING, "events="+events);
                newListener.assertAppEvent(1, newApp, Lifecycle.RUNNING, "events="+events);
            }});
    }
}
