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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.mgmt.usage.ApplicationUsage.ApplicationEvent;
import org.apache.brooklyn.core.mgmt.usage.LocationUsage.LocationEvent;
import org.apache.brooklyn.core.objs.proxy.EntityProxy;
import org.apache.brooklyn.util.collections.MutableList;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class RecordingUsageListener implements UsageListener {

    private final List<List<?>> events = Lists.newCopyOnWriteArrayList();
    
    @Override
    public void onApplicationEvent(ApplicationMetadata app, ApplicationEvent event) {
        events.add(MutableList.of("application", app, event));
    }

    @Override
    public void onLocationEvent(LocationMetadata loc, LocationEvent event) {
        events.add(MutableList.of("location", loc, event));
    }
    
    public void clearEvents() {
        events.clear();
    }
    
    public List<List<?>> getEvents() {
        return ImmutableList.copyOf(events);
    }
    
    public List<List<?>> getLocationEvents() {
        List<List<?>> result = Lists.newArrayList();
        for (List<?> event : events) {
            if (event.get(0).equals("location")) result.add(event);
        }
        return ImmutableList.copyOf(result);
    }
    
    public List<List<?>> getApplicationEvents() {
        List<List<?>> result = Lists.newArrayList();
        for (List<?> event : events) {
            if (event.get(0).equals("application")) result.add(event);
        }
        return ImmutableList.copyOf(result);
    }
    
    public void assertAppEvent(int index, Application expectedApp, Lifecycle expectedState, String errMsg) {
        List<?> actual = getApplicationEvents().get(index);
        ApplicationMetadata appMetadata = (ApplicationMetadata) actual.get(1);
        ApplicationEvent appEvent = (ApplicationEvent) actual.get(2);
        
        assertEquals(appMetadata.getApplication(), expectedApp, errMsg);
        assertTrue(appMetadata.getApplication() instanceof EntityProxy, errMsg);
        assertEquals(appMetadata.getApplicationId(), expectedApp.getId(), errMsg);
        assertNotNull(appMetadata.getApplicationName(), errMsg);
        assertEquals(appMetadata.getCatalogItemId(), expectedApp.getCatalogItemId(), errMsg);
        assertNotNull(appMetadata.getEntityType(), errMsg);
        assertNotNull(appMetadata.getMetadata(), errMsg);
        assertEquals(appEvent.getState(), expectedState, errMsg);
    }
    
    public void assertLocEvent(int index, Location expectedLoc, Application expectedApp, Lifecycle expectedState, String errMsg) {
        List<?> actual = getLocationEvents().get(index);
        LocationMetadata locMetadata = (LocationMetadata) actual.get(1);
        LocationEvent locEvent = (LocationEvent) actual.get(2);
        
        assertEquals(locMetadata.getLocation(), expectedLoc, errMsg);
        assertEquals(locMetadata.getLocationId(), expectedLoc.getId(), errMsg);
        assertNotNull(locMetadata.getMetadata(), errMsg);
        assertEquals(locEvent.getApplicationId(), expectedApp.getId(), errMsg);
        assertEquals(locEvent.getState(), Lifecycle.CREATED, errMsg);
    }

    public void assertHasAppEvent(Application expectedApp, Lifecycle expectedState, String errMsg) {
        for (List<?> contender : getApplicationEvents()) {
            ApplicationMetadata appMetadata = (ApplicationMetadata) contender.get(1);
            ApplicationEvent appEvent = (ApplicationEvent) contender.get(2);
            if (Objects.equal(appMetadata.getApplication(), expectedApp) && appEvent.getState() == expectedState) {
                return;
            }
        }
        
        fail(errMsg);
    }
}
