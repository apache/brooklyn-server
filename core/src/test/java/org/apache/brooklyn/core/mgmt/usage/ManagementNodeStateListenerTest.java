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

package org.apache.brooklyn.core.mgmt.usage;

import static org.apache.brooklyn.api.mgmt.ha.ManagementNodeState.INITIALIZING;
import static org.apache.brooklyn.api.mgmt.ha.ManagementNodeState.MASTER;
import static org.apache.brooklyn.api.mgmt.ha.ManagementNodeState.TERMINATED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ManagementNodeStateListenerTest extends BrooklynMgmtUnitTestSupport {

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        RecordingStaticManagementNodeStateListener.clearInstances();
        super.setUp();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingStaticManagementNodeStateListener.clearInstances();
    }

    private LocalManagementContext newManagementContext(BrooklynProperties brooklynProperties) {
        // Need to call HighAvailabilityManager explicitly; otherwise it will never publish
        // the ManagementNodeState.
        LocalManagementContext result = LocalManagementContextForTests.newInstance(brooklynProperties);
        result.getHighAvailabilityManager().disabled();
        result.noteStartupComplete();
        return result;
    }
    
    @Test
    public void testAddUsageListenerInstance() throws Exception {
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put(BrooklynServerConfig.MANAGEMENT_NODE_STATE_LISTENERS, ImmutableList.of(new RecordingStaticManagementNodeStateListener()));
        replaceManagementContext(newManagementContext(brooklynProperties));

        assertEventsEventually(RecordingStaticManagementNodeStateListener.getInstance(), ImmutableList.of(INITIALIZING, MASTER));
        
        mgmt.terminate();
        assertEventsEventually(RecordingStaticManagementNodeStateListener.getInstance(), ImmutableList.of(INITIALIZING, MASTER, TERMINATED));
    }

    @Test
    public void testAddUsageListenerViaProperties() throws Exception {
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put(BrooklynServerConfig.MANAGEMENT_NODE_STATE_LISTENERS, RecordingStaticManagementNodeStateListener.class.getName());
        replaceManagementContext(newManagementContext(brooklynProperties));
        
        assertEventsEventually(RecordingStaticManagementNodeStateListener.getInstance(), ImmutableList.of(INITIALIZING, MASTER));
    }

    @Test
    public void testAddMultipleUsageListenersViaProperties() throws Exception {
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put(BrooklynServerConfig.MANAGEMENT_NODE_STATE_LISTENERS, RecordingStaticManagementNodeStateListener.class.getName() + "," + RecordingStaticManagementNodeStateListener.class.getName());
        replaceManagementContext(newManagementContext(brooklynProperties));
        
        final List<RecordingStaticManagementNodeStateListener> listeners = RecordingStaticManagementNodeStateListener.getInstances();
        assertEquals(listeners.size(), 2);
        assertTrue(listeners.get(0) instanceof RecordingStaticManagementNodeStateListener, "listeners="+listeners);
        assertTrue(listeners.get(1) instanceof RecordingStaticManagementNodeStateListener, "listeners="+listeners);
        
        assertEventsEventually(listeners.get(0), ImmutableList.of(INITIALIZING, MASTER));
        assertEventsEventually(listeners.get(1), ImmutableList.of(INITIALIZING, MASTER));
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testErrorWhenConfiguredClassIsNotAListener() {
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put(BrooklynServerConfig.MANAGEMENT_NODE_STATE_LISTENERS, Integer.class.getName());
        replaceManagementContext(LocalManagementContextForTests.newInstance(brooklynProperties));
    }

    private void assertEventsEventually(RecordingManagementNodeStateListener listener, List<ManagementNodeState> expected) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                List<ManagementNodeState> actual = listener.getEvents();
                String errMsg = "actual="+actual+"; expected="+expected;
                assertEquals(actual, expected, errMsg);
            }});
    }
    
    public static class RecordingStaticManagementNodeStateListener extends RecordingManagementNodeStateListener implements ManagementNodeStateListener {
        private static final List<RecordingStaticManagementNodeStateListener> STATIC_INSTANCES = Lists.newCopyOnWriteArrayList();

        public static RecordingStaticManagementNodeStateListener getInstance() {
            return Iterables.getOnlyElement(STATIC_INSTANCES);
        }

        public static RecordingStaticManagementNodeStateListener getLastInstance() {
            return Iterables.getLast(STATIC_INSTANCES);
        }
        
        public static List<RecordingStaticManagementNodeStateListener> getInstances() {
            return ImmutableList.copyOf(STATIC_INSTANCES);
        }

        public static void clearInstances() {
            STATIC_INSTANCES.clear();
        }

        public RecordingStaticManagementNodeStateListener() {
            // Bad to leak a ref to this before constructor finished, but we'll live with it because
            // it's just test code!
            STATIC_INSTANCES.add(this);
        }
    }
    
    public static class RecordingManagementNodeStateListener implements ManagementNodeStateListener {
        private final List<ManagementNodeState> events = Lists.newCopyOnWriteArrayList();

        @Override
        public void onStateChange(ManagementNodeState state) {
            events.add(state);
        }
        
        public List<ManagementNodeState> getEvents() {
            return ImmutableList.copyOf(events);
        }
    }
}
