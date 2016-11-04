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
package org.apache.brooklyn.core.mgmt.rebind;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class RebindManagerExceptionHandlerTest extends RebindTestFixtureWithApp {

    @Override
    protected TestApplication createApp() {
        TestApplication app = super.createApp();
        app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_MAP_THING.getName(), 
                     // misconfigured map value, should be a string key, but forced (by using a flag) so failure won't be enforced until persist/rebind
                    ImmutableMap.of("keyWithMapValue", ImmutableMap.of("minRam", 4))));
        return app;
    }

    @Test
    public void testAddConfigFailure() throws Throwable {
        try {
            rebind();
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "minRam=4", "keyWithMapValue");
        }
    }

    @Test
    public void testAddConfigContinue() throws Throwable {
        RebindOptions rebindOptions = RebindOptions.create().additionalProperties(ImmutableMap.of("rebind.failureMode.addConfig", "continue"));
        TestApplication rebindedApp = rebind(rebindOptions);
        EntityAsserts.assertConfigEquals(rebindedApp, TestEntity.CONF_MAP_THING, null);
    }

}
