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

import java.util.Map;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
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
                new ResourceUtils(getClass()).getResourceFromUrl(OsgiTestResources.BROOKLYN_TEST_OSGI_BEANS_URL));

        OsgiBundleInstallationResult r = result.getWithError();
        RegisteredType rt = r.getTypesInstalled().stream().filter(rti -> "sampleBean".equals(rti.getId())).findAny()
                .orElseThrow(() -> {
                    throw Asserts.fail("Bean not found; RTs were: " + r.getTypesInstalled());
                });
        Asserts.assertEquals(rt.getKind(), RegisteredTypeKind.BEAN);

        Object b1 = mgmt().getTypeRegistry().create(rt, null, null);
        Object b1n = b1.getClass().getField("number").get(b1);
        Asserts.assertEquals(b1n, 1);
    }
}
