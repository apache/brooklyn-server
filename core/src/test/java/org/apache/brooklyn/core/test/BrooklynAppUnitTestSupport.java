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
package org.apache.brooklyn.core.test;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerializationTest;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.BeforeMethod;

import java.util.Collection;

/**
 * To be extended by unit/integration tests.
 * <p>
 * Uses a light-weight management context that will not read {@code ~/.brooklyn/brooklyn.properties}.
 */
public class BrooklynAppUnitTestSupport extends BrooklynMgmtUnitTestSupport {

    protected TestApplication app;

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        setUpApp();
    }

    /** set null not to set, or a boolean to set explicitly; default true */
    protected Boolean shouldSkipOnBoxBaseDirResolution() {
        return true;
    }

    protected void setUpApp() {
        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class);
        if (shouldSkipOnBoxBaseDirResolution()!=null)
            appSpec.configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution());
        
        app = mgmt.getEntityManager().createEntity(appSpec);
    }

    public static RegisteredType addRegisteredTypeBean(ManagementContext mgmt, String symName, String version, RegisteredType.TypeImplementationPlan plan) {
        RegisteredType rt = RegisteredTypes.bean(symName, version, plan);
        Collection<Throwable> errors = mgmt.getCatalog().validateType(rt, null, true);
        if (!errors.isEmpty()) Asserts.fail(Exceptions.propagate("Failed to validate", errors));
        // the resolved type is added, not necessarily type above which will be unresolved
        return mgmt.getTypeRegistry().get(symName, version);
    }

    public static RegisteredType addRegisteredTypeBean(ManagementContext mgmt, String symName, String version, Class<?> clazz) {
        return addRegisteredTypeBean(mgmt, symName, version,
            new BasicTypeImplementationPlan(BeanWithTypeUtils.FORMAT, "type: " + clazz.getName()));
            // should be identical:
            //new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, clazz.getName()));
    }
}
