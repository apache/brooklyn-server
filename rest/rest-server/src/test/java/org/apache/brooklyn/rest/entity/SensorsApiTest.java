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
package org.apache.brooklyn.rest.entity;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.rest.BrooklynRestApiLauncherTestFixture;
import org.apache.brooklyn.util.net.UserAndHostAndPort;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SensorsApiTest extends BrooklynRestApiLauncherTestFixture {
    protected ManagementContext mgmt;
    protected TestApplication app;
    protected TestEntity entity;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        mgmt = LocalManagementContextForTests.builder(false).build();
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class)));
        entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        useServerForTest(baseLauncher()
                .managementContext(mgmt)
                .forceUseOfDefaultCatalogWithJavaClassPath(true)
                .start());
    }

    // TODO turn into unit tests, not spinning up real http server.
    @Test(groups = "Integration")
    public void testGetHostAndPortSensor() throws Exception {
        entity.sensors().set(Attributes.SSH_ADDRESS, UserAndHostAndPort.fromParts("testHostUser", "1.2.3.4", 22));

        String sensorGetPath = "/v1/applications/"+app.getId()+"/entities/"+entity.getId()+"/sensors/" + Attributes.SSH_ADDRESS.getName();
        assertEquals(httpGet(sensorGetPath), "\"testHostUser@1.2.3.4:22\"");

        String descendantSensorPath = "/v1/applications/"+app.getId()+"/descendants/sensor/" + Attributes.SSH_ADDRESS.getName();
        assertEquals(httpGet(descendantSensorPath), "{\"" + entity.getId() + "\":{\"user\":\"testHostUser\",\"hostAndPort\":{\"host\":\"1.2.3.4\",\"port\":22,\"hasBracketlessColons\":false}}}");
    }
}
