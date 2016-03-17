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

import java.io.File;

import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class PropertiesFileExternalConfigSupplierTest {

    // TODO I (Aled) thought that the ExternalConfigSuppliers were re-initialised when
    // mgmt.reloadBrooklynProperties was called, but that doesn't seem to be the case.
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesFileExternalConfigSupplierTest.class);

    protected File propsFile;
    protected ManagementContextInternal mgmt;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        propsFile = File.createTempFile("PropertiesFileExternalConfigSupplierTest", ".properties");
    }
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        try {
            if (mgmt != null) Entities.destroyAll(mgmt);
        } finally {
            mgmt = null;
            if (propsFile != null) propsFile.delete();
        }
    }

    @Test
    public void testFromProperties() throws Exception {
        String contents =
                "mykey=myval"+"\n"+
                "mykey2=myval2";
        Files.write(contents, propsFile, Charsets.UTF_8);
        BrooklynProperties props = BrooklynProperties.Factory.newEmpty();
        props.put("brooklyn.external.foo", PropertiesFileExternalConfigSupplier.class.getName());
        props.put("brooklyn.external.foo.propertiesUrl", propsFile.toURI().toString());
        
        mgmt = LocalManagementContextForTests.newInstance(props);
        
        assertEquals(mgmt.getExternalConfigProviderRegistry().getConfig("foo", "mykey"), "myval");
        assertEquals(mgmt.getExternalConfigProviderRegistry().getConfig("foo", "mykey2"), "myval2");
        assertNull(mgmt.getExternalConfigProviderRegistry().getConfig("foo", "wrongkey"));
    }
}
