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
package org.apache.brooklyn.location.jclouds;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.util.executor.HttpExecutorFactory;
import org.apache.brooklyn.util.executor.HttpExecutorFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class JcloudsLocationExtensionStubbedTest extends AbstractJcloudsStubbedUnitTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JcloudsLocationExtensionStubbedTest.class);
    
    @Override
    protected LocalManagementContext newManagementContext() {
        LocalManagementContext result = super.newManagementContext();
        BrooklynProperties brooklynProperties = result.getBrooklynProperties();
        brooklynProperties.put("brooklyn.location.named.jclouds-with-extension", super.getLocationSpec());
        brooklynProperties.put("brooklyn.location.named.jclouds-with-extension.extensions", 
                "{ "+HttpExecutorFactory.class.getName()+": "+MyHttpExecutorFactory.class.getName()+" }");
       return result;
    }

    @Override
    protected String getLocationSpec() {
        return "jclouds-with-extension";
    }

    @Test
    public void testHasExtension() throws Exception {
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of());
        MachineLocation machine = jcloudsLocation.obtain();
        
        HttpExecutorFactory extension = machine.getExtension(HttpExecutorFactory.class);
        assertNotNull(extension);
        assertTrue(extension instanceof MyHttpExecutorFactory, "extension="+extension);
    }
    
    public static class MyHttpExecutorFactory extends HttpExecutorFactoryImpl {
    }
}
