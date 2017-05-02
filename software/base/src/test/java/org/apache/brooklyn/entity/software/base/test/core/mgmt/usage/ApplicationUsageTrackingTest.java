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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.mgmt.usage.ApplicationUsage;
import org.apache.brooklyn.core.mgmt.usage.ApplicationUsage.ApplicationEvent;
import org.apache.brooklyn.core.mgmt.usage.RecordingUsageListener;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class ApplicationUsageTrackingTest extends BrooklynMgmtUnitTestSupport {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationUsageTrackingTest.class);

    protected TestApplication app;

    @Test
    public void testUsageInitiallyEmpty() {
        Set<ApplicationUsage> usage = mgmt.getUsageManager().getApplicationUsage(Predicates.alwaysTrue());
        assertEquals(usage, ImmutableSet.of());
    }

    @Test
    public void testUsageListenerReceivesEvents() throws Exception {
        final RecordingUsageListener listener = new RecordingUsageListener();
        mgmt.getUsageManager().addUsageListener(listener);

        // Expect CREATED
        app = TestApplication.Factory.newManagedInstanceForTests(mgmt);
        
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = listener.getApplicationEvents();
                assertEquals(events.size(), 1, "events="+events);
                listener.assertAppEvent(0, app, Lifecycle.CREATED, "events="+events);
            }});

        // Expect STARTING and RUNNING
        app.setCatalogItemId("testCatalogItem");
        app.start(ImmutableList.<Location>of());

        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = listener.getApplicationEvents();
                assertEquals(events.size(), 3, "events="+events);
                listener.assertAppEvent(1, app, Lifecycle.STARTING, "events="+events);
                listener.assertAppEvent(2, app, Lifecycle.RUNNING, "events="+events);
            }});

        // Expect STOPPING, STOPPED and DESTROYED
        app.stop();

        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = listener.getApplicationEvents();
                assertEquals(events.size(), 6, "events="+events);
                listener.assertAppEvent(3, app, Lifecycle.STOPPING, "events="+events);
                listener.assertAppEvent(4, app, Lifecycle.STOPPED, "events="+events);
                listener.assertAppEvent(5, app, Lifecycle.DESTROYED, "events="+events);
            }});
    }
    

    @Test
    public void testUsageListenerCanRecogniseTopLevelApps() throws Exception {
        final LinkedHashSet<Application> topLevelApps = Sets.newLinkedHashSet();
        final RecordingUsageListener listener = new RecordingUsageListener() {
            public void onApplicationEvent(ApplicationMetadata app, ApplicationEvent event) {
                if (app.getApplication().getParent() == null) {
                    topLevelApps.add(app.getApplication());
                }
                super.onApplicationEvent(app, event);
            }
        };
        mgmt.getUsageManager().addUsageListener(listener);

        // Expect CREATED
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestApplication.class)));
        final TestApplication childApp = (TestApplication) Iterables.getOnlyElement(app.getChildren());
        
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = listener.getApplicationEvents();
                assertEquals(events.size(), 2, "events="+events);
                listener.assertHasAppEvent(app, Lifecycle.CREATED, "events="+events);
                listener.assertHasAppEvent(app, Lifecycle.CREATED, "events="+events);
            }});
        assertEquals(topLevelApps, ImmutableSet.of(app));
        
        listener.clearEvents();
        topLevelApps.clear();

        // Expect STOPPING, STOPPED and DESTROYED
        app.stop();

        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = listener.getApplicationEvents();
                listener.assertHasAppEvent(app, Lifecycle.DESTROYED, "events="+events);
                listener.assertHasAppEvent(childApp, Lifecycle.DESTROYED, "events="+events);
            }});
        
        // TODO By the point of being DESTROYED, we've cleared the app.parent(), so we can't tell
        // if it was a top-level app anymore. Therefore we are not asserting:
        //    assertEquals(topLevelApps, ImmutableSet.of(app));
    }
    
    @Test
    public void testAddAndRemoveUsageListener() throws Exception {
        final RecordingUsageListener listener = new RecordingUsageListener();
        mgmt.getUsageManager().addUsageListener(listener);

        // Expect CREATED
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = listener.getApplicationEvents();
                assertEquals(events.size(), 1, "events="+events);
                listener.assertAppEvent(0, app, Lifecycle.CREATED, "events="+events);
            }});

        // Remove the listener; will get no more notifications
        listener.clearEvents();
        mgmt.getUsageManager().removeUsageListener(listener);
        
        app.start(ImmutableList.<Location>of());
        Asserts.succeedsContinually(new Runnable() {
            @Override public void run() {
                List<List<?>> events = listener.getLocationEvents();
                assertEquals(events.size(), 0, "events="+events);
            }});
    }
    
    @Test
    public void testUsageIncludesStartAndStopEvents() {
        // Start event
        long preStart = System.currentTimeMillis();
        app = TestApplication.Factory.newManagedInstanceForTests(mgmt);
        app.start(ImmutableList.<Location>of());
        long postStart = System.currentTimeMillis();

        Set<ApplicationUsage> usages1 = mgmt.getUsageManager().getApplicationUsage(Predicates.alwaysTrue());
        ApplicationUsage usage1 = Iterables.getOnlyElement(usages1);
        assertApplicationUsage(usage1, app);
        assertApplicationEvent(usage1.getEvents().get(0), Lifecycle.CREATED, preStart, postStart);
        assertApplicationEvent(usage1.getEvents().get(1), Lifecycle.STARTING, preStart, postStart);
        assertApplicationEvent(usage1.getEvents().get(2), Lifecycle.RUNNING, preStart, postStart);

        // Stop events
        long preStop = System.currentTimeMillis();
        app.stop();
        long postStop = System.currentTimeMillis();

        Set<ApplicationUsage> usages2 = mgmt.getUsageManager().getApplicationUsage(Predicates.alwaysTrue());
        ApplicationUsage usage2 = Iterables.getOnlyElement(usages2);
        assertApplicationUsage(usage2, app);
        assertApplicationEvent(usage2.getEvents().get(3), Lifecycle.STOPPING, preStop, postStop);
        assertApplicationEvent(usage2.getEvents().get(4), Lifecycle.STOPPED, preStop, postStop);
        //Apps unmanage themselves on stop
        assertApplicationEvent(usage2.getEvents().get(5), Lifecycle.DESTROYED, preStop, postStop);
        
        assertFalse(mgmt.getEntityManager().isManaged(app), "App should already be unmanaged");
        
        Set<ApplicationUsage> usages3 = mgmt.getUsageManager().getApplicationUsage(Predicates.alwaysTrue());
        ApplicationUsage usage3 = Iterables.getOnlyElement(usages3);
        assertApplicationUsage(usage3, app);
        
        assertEquals(usage3.getEvents().size(), 6, "usage="+usage3);
    }
    
    private void assertApplicationUsage(ApplicationUsage usage, Application expectedApp) {
        assertEquals(usage.getApplicationId(), expectedApp.getId());
        assertEquals(usage.getApplicationName(), expectedApp.getDisplayName());
        assertEquals(usage.getEntityType(), expectedApp.getEntityType().getName());
    }
    
    private void assertApplicationEvent(ApplicationEvent event, Lifecycle expectedState, long preEvent, long postEvent) {
        // Saw times differ by 1ms - perhaps different threads calling currentTimeMillis() can get out-of-order times?!
        final int TIMING_GRACE = 5;
        
        assertEquals(event.getState(), expectedState);
        long eventTime = event.getDate().getTime();
        if (eventTime < (preEvent - TIMING_GRACE) || eventTime > (postEvent + TIMING_GRACE)) {
            fail("for "+expectedState+": event=" + Time.makeDateString(eventTime) + "("+eventTime + "); "
                    + "pre=" + Time.makeDateString(preEvent) + " ("+preEvent+ "); "
                    + "post=" + Time.makeDateString(postEvent) + " ("+postEvent + ")");
        }
    }
}
