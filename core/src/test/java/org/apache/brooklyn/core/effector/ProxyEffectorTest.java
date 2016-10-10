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

package org.apache.brooklyn.core.effector;

import static org.testng.Assert.assertEquals;

import java.util.NoSuchElementException;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class ProxyEffectorTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testHappyPath() {
        TestEntity a = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        ProxyEffector proxyEffector = new ProxyEffector(ImmutableMap.of(
                AddEffector.EFFECTOR_NAME, "proxy-effector",
                ProxyEffector.TARGET_ENTITY, a,
                ProxyEffector.TARGET_EFFECTOR_NAME, "identityEffector"));
        // BasicEntity doesn't have an identityEffector.
        EntityInternal b = Entities.deproxy(app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(proxyEffector)));
        Object output = b.invoke(b.getEffector("proxy-effector"), ImmutableMap.of("arg", "value"))
                .getUnchecked(Duration.ONE_MINUTE);
        assertEquals(output, "value");
    }

    @Test
    public void testThrowsIfTargetEffectorDoesntExist() {
        TestEntity a = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        ProxyEffector proxyEffector = new ProxyEffector(ImmutableMap.of(
                AddEffector.EFFECTOR_NAME, "proxy-effector",
                ProxyEffector.TARGET_ENTITY, a,
                ProxyEffector.TARGET_EFFECTOR_NAME, "kajnfksjdnfkjsdnf"));
        EntityInternal b = Entities.deproxy(app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(proxyEffector)));
        try {
            b.invoke(b.getEffector("proxy-effector"), ImmutableMap.of("arg", "value"))
                    .getUnchecked(Duration.ONE_MINUTE);
            Asserts.shouldHaveFailedPreviously("expected exception when invoking effector that does not exist");
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, NoSuchElementException.class);
        }
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testThrowsIfEntityUnset() {
        new ProxyEffector(MutableMap.of(
                AddEffector.EFFECTOR_NAME, "proxy-effector",
                ProxyEffector.TARGET_ENTITY, null,
                ProxyEffector.TARGET_EFFECTOR_NAME, "kajnfksjdnfkjsdnf"));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testThrowsIfTargetEffectorNameUnset() {
        new ProxyEffector(MutableMap.of(
                AddEffector.EFFECTOR_NAME, "proxy-effector",
                ProxyEffector.TARGET_ENTITY, app,
                ProxyEffector.TARGET_EFFECTOR_NAME, null));
    }

}