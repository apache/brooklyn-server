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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.testng.annotations.Test;

public class BasicLocationNetworkInfoInitializerTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testInitializerSetsConfigKeyOnEntity() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .addInitializer(DefaultConnectivityResolver.class));
        final ConfigKey<Object> key = BrooklynConfigKeys.PROVISIONING_PROPERTIES.subKey(JcloudsLocationConfig.CONNECTIVITY_RESOLVER.getName());
        final Object value = entity.config().get(key);
        assertNotNull(value, "no value on " + entity + " for " + key);
        assertEquals(value.getClass(), DefaultConnectivityResolver.class);
    }

}
