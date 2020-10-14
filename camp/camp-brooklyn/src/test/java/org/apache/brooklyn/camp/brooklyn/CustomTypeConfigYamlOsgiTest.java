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
package org.apache.brooklyn.camp.brooklyn;

import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

@Test
public class CustomTypeConfigYamlOsgiTest extends CustomTypeConfigYamlTest {
    private static final Logger log = LoggerFactory.getLogger(CustomTypeConfigYamlOsgiTest.class);

    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Test
    public void testLoadBundleBom() throws NoSuchFieldException, IllegalAccessException {
        ReferenceWithError<OsgiBundleInstallationResult> result = ((ManagementContextInternal) mgmt()).getOsgiManager().get().install(
                () -> new ResourceUtils(getClass()).getResourceFromUrl(OsgiTestResources.BROOKLYN_TEST_OSGI_BEANS_URL));

        OsgiBundleInstallationResult r = result.getWithError();
        RegisteredType rt = r.getTypesInstalled().stream().filter(rti -> "sampleBean:0.1.0".equals(rti.getId())).findAny()
                .orElseThrow(() -> Asserts.fail("Bean not found; RTs were: " + r.getTypesInstalled()));
        Asserts.assertEquals(rt.getKind(), RegisteredTypeKind.BEAN);

        Object b1 = mgmt().getTypeRegistry().create(rt, null, null);
        Object b1n = Reflections.getFieldValueMaybe(b1, "number").get();
        Asserts.assertEquals(b1n, 1);
    }
}
