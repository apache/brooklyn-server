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
package org.apache.brooklyn.rest;

import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ResourcesInstalledEverywhereTest {

    public void testResourcesInBlueprintServiceXml() {
        // from brooklyn-rest-resources
        assertAllClassesListedAtUrl(ResourceUtils.create(this), "classpath://OSGI-INF/blueprint/service.xml", BrooklynRestApi.getBrooklynRestResources() );
    }

    public void testResourcesInWebXml() {
        // in this project
        // TODO not sure this is used? possibly we should delete as part of karaf bias.
        // (we already have the java and the karaf blueprint ... maybe we could even get rid of the java
        // but i think it's used a lot in dev)
        assertAllClassesListedAtUrl(ResourceUtils.create(this), "classpath://WEB-INF/web.xml", BrooklynRestApi.getBrooklynRestResources() );
    }
    
    private static void assertAllClassesListedAtUrl(ResourceUtils loader, String url, Iterable<?> resourceClasses) {
        String text = loader.getResourceAsString(url);
        List<String> missingClasses = MutableList.of();
        for (Object c: resourceClasses) {
            if (c instanceof Class) c = ((Class<?>)c).getName();
            else c = c.getClass().getName();
            
            if (text.indexOf(c.toString())==-1) missingClasses.add(c.toString());
        }
        
        if (!missingClasses.isEmpty())
            Assert.fail("Missing entries at "+url+": "+missingClasses);
    }
}
