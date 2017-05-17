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
package org.apache.brooklyn.core.mgmt.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ObservableSetTest {

    private ObservableSet<Object> set;
    private RecordingListener listener;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        set = new ObservableSet<Object>();
        listener = new RecordingListener();
        set.addListener(listener);
    }
    
    @Test
    public void testAdd() {
        boolean result1 = set.add("val1");
        assertTrue(result1);
        assertEquals(listener.getCalls(), ImmutableList.of(new Call(NotificationType.ADDED, "val1")));
        listener.clear();
        
        boolean result2 = set.add("val1");
        assertFalse(result2);
        assertEquals(listener.getCalls(), ImmutableList.of());
    }
    
    @Test
    public void testRemove() {
        set.add("val1");
        listener.clear();
        
        boolean result1 = set.remove("val1");
        assertTrue(result1);
        assertEquals(listener.getCalls(), ImmutableList.of(new Call(NotificationType.REMOVED, "val1")));
        listener.clear();
        
        boolean result2 = set.remove("val1");
        assertFalse(result2);
        assertEquals(listener.getCalls(), ImmutableList.of());
        
        boolean result3 = set.remove("different");
        assertFalse(result3);
        assertEquals(listener.getCalls(), ImmutableList.of());
    }
    
    @Test
    public void testContains() {
        set.add("val1");
        assertTrue(set.contains("val1"));
        
        set.remove("val1");
        assertFalse(set.contains("val1"));
        
        assertFalse(set.contains("different"));
    }
    
    @Test
    public void testMultipleListeners() {
        List<RecordingListener> listeners = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            RecordingListener l = new RecordingListener();
            listeners.add(l);
            set.addListener(l);
        }
        
        set.add("val1");
        for (RecordingListener listener: listeners) {
            assertEquals(listener.getCalls(), ImmutableList.of(new Call(NotificationType.ADDED, "val1")));
            listener.clear();
        }

        set.remove("val1");
        for (RecordingListener listener: listeners) {
            assertEquals(listener.getCalls(), ImmutableList.of(new Call(NotificationType.REMOVED, "val1")));
            listener.clear();
        }
    }
    
    @Test
    public void testRemoveListenerNotSubsequentlyNotified() {
        set.removeListener(listener);
        
        set.add("val1");
        assertEquals(listener.getCalls(), ImmutableList.of());

        set.remove("val1");
        assertEquals(listener.getCalls(), ImmutableList.of());
    }
    
    @Test
    public void testAddAndRemoveListenerIdempotent() {
        RecordingListener listener2 = new RecordingListener();

        set.addListener(listener2);
        set.addListener(listener2);
        
        set.add("val1");
        assertEquals(listener2.getCalls(), ImmutableList.of(new Call(NotificationType.ADDED, "val1")));
        listener2.clear();

        set.removeListener(listener2);
        
        set.add("val2");
        assertEquals(listener2.getCalls(), ImmutableList.of());
        
        set.removeListener(listener2);
    }
    
    static enum NotificationType {
        ADDED,
        REMOVED;
    }

    static class Call {
        final NotificationType type;
        final Object val;
        
        Call(NotificationType type, Object val) {
            this.type = type;
            this.val = val;
        }
        
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof Call) && Objects.equal(((Call)obj).type, type)
                    && Objects.equal(((Call)obj).val, val);
        }
    }
    
    static class RecordingListener implements CollectionChangeListener<Object> {
        final List<Call> calls = Lists.newCopyOnWriteArrayList();
        
        @Override
        public void onItemAdded(Object item) {
            calls.add(new Call(NotificationType.ADDED, item));
        }

        @Override
        public void onItemRemoved(Object item) {
            calls.add(new Call(NotificationType.REMOVED, item));
        }
        
        public List<Call> getCalls() {
            return ImmutableList.copyOf(calls);
        }
        
        public void clear() {
            calls.clear();
        }
    }
}
