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

package org.apache.brooklyn.rest.test.entity;

import static org.apache.brooklyn.test.Asserts.assertTrue;
import static org.testng.Assert.assertNotNull;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.entity.brooklynnode.BrooklynNode;
import org.apache.brooklyn.entity.nosql.redis.RedisCluster;
import org.apache.brooklyn.entity.nosql.redis.RedisStore;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.rest.BrooklynRestApiLauncherTestFixture;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.yaml.Yamls;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class RedisClusterIntegrationTest extends BrooklynRestApiLauncherTestFixture {

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        useServerForTest(newServer());
    }

    @Test(groups = "Integration")
    public void testDeployRedisCluster() throws InterruptedException, ExecutionException, TimeoutException {
        final URI webConsoleUri = URI.create(getBaseUriRest());

        // Test setup
        final EntitySpec<BrooklynNode> spec = EntitySpec.create(BrooklynNode.class);
        final ManagementContext mgmt = getManagementContextFromJettyServerAttributes(server);
        final BrooklynNode node = mgmt.getEntityManager().createEntity(spec);
        node.sensors().set(BrooklynNode.WEB_CONSOLE_URI, webConsoleUri);

        // Deploy it.
        final String blueprint = "location: localhost\n" +
                "services:\n" +
                "- type: org.apache.brooklyn.entity.nosql.redis.RedisCluster";
        HttpToolResponse response = node.http().post(
                "/applications",
                ImmutableMap.of("Content-Type", "text/yaml"),
                blueprint.getBytes());
        HttpAsserts.assertHealthyStatusCode(response.getResponseCode());

        // Assert application is eventually running and not on fire.
        final Entity entity = mgmt.getApplications().iterator().next().getChildren().iterator().next();
        assertTrue(entity instanceof RedisCluster,
                "expected " + RedisCluster.class.getName() + ", found: " + entity);
        RedisCluster cluster = RedisCluster.class.cast(entity);
        Entities.dumpInfo(cluster);
        assertDownloadUrl(cluster.getMaster());
        for (Entity slave : cluster.getSlaves().getMembers()) {
            assertDownloadUrl(slave);
        }

        @SuppressWarnings("unchecked")
        String taskId = Strings.toString( ((Map<String,Object>) Yamls.parseAll(response.getContentAsString()).iterator().next()).get("id") );
        Task<?> task = mgmt.getExecutionManager().getTask(taskId);
        Assert.assertNotNull(task);
        
        task.get(Duration.minutes(20));
        
        Entities.dumpInfo(cluster);
        
        EntityAsserts.assertAttributeEquals(entity, SoftwareProcess.SERVICE_UP, true);
    }
    
    private void assertDownloadUrl(Entity entity) {
        assertNotNull(entity.config().get(RedisStore.DOWNLOAD_URL), "RedisStore.DOWNLOAD_URL");
        assertNotNull(entity.config().get(SoftwareProcess.DOWNLOAD_URL), "SoftwareProcess.DOWNLOAD_URL");
        assertNotNull(entity.config().get(Attributes.DOWNLOAD_URL), "Attributes.DOWNLOAD_URL");
    }

}