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
package org.apache.brooklyn.core.config.external;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class InPlaceExternalConfigSupplierTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(InPlaceExternalConfigSupplierTest.class);

    protected ManagementContextInternal mgmt;

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        try {
            if (mgmt != null) Entities.destroyAll(mgmt);
        } finally {
            mgmt = null;
        }
    }

    @Test
    public void testInPlace() throws Exception {
        BrooklynProperties props = BrooklynProperties.Factory.newEmpty();
        props.put("brooklyn.external.foo", InPlaceExternalConfigSupplier.class.getName());
        props.put("brooklyn.external.foo.mykey", "myval");
        
        mgmt = LocalManagementContextForTests.newInstance(props);
        
        assertEquals(mgmt.getExternalConfigProviderRegistry().getConfig("foo", "mykey"), "myval");
        assertNull(mgmt.getExternalConfigProviderRegistry().getConfig("foo", "wrongkey"));
    }
}
