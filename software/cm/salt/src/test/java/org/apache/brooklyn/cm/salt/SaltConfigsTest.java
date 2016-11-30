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
package org.apache.brooklyn.cm.salt;

import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.entity.TestApplication;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SaltConfigsTest {

    private TestApplication app = null;

    @AfterMethod(alwaysRun=true)
    public void tearDown() {
        if ( app != null) {
        	Entities.destroyAll(app.getManagementContext());
            app = null;
        }
    }

    @Test
    public void testAddToRunList() {
        TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        SaltConfigs.addToRunList(app, "a", "b");
        Set<? extends String> runs = app.getConfig(SaltConfig.START_STATES);
        Assert.assertEquals(runs, ImmutableSet.of("a", "b"));
    }

    @Test
    public void testAddLaunchAttributes() {
        TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        SaltConfigs.addLaunchAttributes(app, ImmutableMap.of("k1", "v1"));
        Map<String, Object> attribs = app.getConfig(SaltConfig.SALT_SSH_LAUNCH_ATTRIBUTES);
        Assert.assertEquals(attribs, ImmutableMap.of("k1", "v1"));
    }

    @Test
    public void testAddToFormulas() {
        TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        SaltConfigs.addToFormulas(app, "v1");
        SaltConfigs.addToFormulas(app, "v2");
        final Set<? extends String> formulas = app.getConfig(SaltConfig.SALT_FORMULAS);
        Assert.assertEquals(formulas, ImmutableSet.of("v1", "v2"));
    }

}
