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
package org.apache.brooklyn.rest.entitlement;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementClass;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementManager;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.BasicTask;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.http.HttpToolResponse;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ActivityResourceEntitlementsTest extends AbstractRestApiEntitlementsTest {

    protected TestEntity otherEntity;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        otherEntity = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class).parent(app));
    }

    @Test
    public void readonlyCancelsRootStartedEffector() throws Exception {
        String path = "/v1/applications/" + app.getId() + "/entities/" + entity.getId();
        HttpToolResponse start = httpPost("myRoot", path + "/effectors/sleepEffector?timeout=always", "{\"duration\":\"60s\"}".getBytes("UTF-8"), ImmutableMap.of("Content-Type", "application/json"));
        Assert.assertEquals(start.getResponseCode(), 202);
        JsonNode data = new ObjectMapper().readTree(start.getContentAsString());
        String id = data.get("id").asText();
        Task<?> task = mgmt.getExecutionManager().getTask(id);
        Assert.assertNotNull(task);
        try {
            Assert.assertFalse(task.isDone());
            String list = httpGet("myReadonly", path + "/activities");
            Assert.assertTrue(list.contains(id));

            HttpToolResponse cancel = httpPost("myReadonly", "/v1/activities/" + id + "/cancel", new byte[0]);
            Assert.assertEquals(cancel.getResponseCode(), 403);
            Assert.assertFalse(task.isCancelled());
        } finally {
            task.cancel(true);
        }
    }

    @Test
    public void readonlyUserCancelsAnotherTask() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        Task<?> task = new BasicTask<Void>(MutableMap.of("displayName", "synthetic admin task", "tags", ImmutableList.of(BrooklynTaskTags.tagForContextEntity(entity))), (Callable<Void>) () -> {
            started.countDown();
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
            return null;
        });
        mgmt.getExecutionManager().submit(task);
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
        Assert.assertFalse(task.isDone());

        try {
            HttpToolResponse deny = httpPost("myReadonly", "/v1/applications/" + app.getId() + "/entities/" + entity.getId() + "/effectors/myEffector", "{}".getBytes("UTF-8"), ImmutableMap.of("Content-Type", "application/json"));
            Assert.assertEquals(deny.getResponseCode(), 403);
            String read = httpGet("myReadonly", "/v1/activities/" + task.getId());
            Assert.assertTrue(read.contains(task.getId()));

            HttpToolResponse cancel = httpPost("myReadonly", "/v1/activities/" + task.getId() + "/cancel", new byte[0]);
            Assert.assertEquals(cancel.getResponseCode(), 403);
            Assert.assertFalse(task.isCancelled());
            Assert.assertEquals(interrupted.getCount(), 1);
        } finally {
            task.cancel(true);
        }
    }

    @Test
    public void rootCanCancelTask() throws Exception {
        Task<?> rootTask = newCancellableTask(entity);
        HttpToolResponse rootCancel = httpPost("myRoot", "/v1/activities/" + rootTask.getId() + "/cancel", new byte[0]);
        Assert.assertEquals(rootCancel.getResponseCode(), 200);
        Assert.assertEquals(rootCancel.getContentAsString(), "true");
        Assert.assertTrue(rootTask.isCancelled());
    }

    @Test
    public void userMinimalAndUnrecognisedUsersCannotCancelTask() throws Exception {
        // `user` is excluded from MODIFY_ENTITY (Entitlements.user()), same as every other
        // direct entity-mutation endpoint (EntityResource's forced-stop, SensorResource,
        // EntityConfigResource) - cancel is no exception, even for a task it could have started itself.
        Task<?> userAttempt = newCancellableTask(entity);
        try {
            HttpToolResponse userCancel = httpPost("myUser", "/v1/activities/" + userAttempt.getId() + "/cancel", new byte[0]);
            Assert.assertEquals(userCancel.getResponseCode(), 403);
            Assert.assertFalse(userAttempt.isCancelled());
        } finally {
            userAttempt.cancel(true);
        }

        Task<?> minimalAttempt = newCancellableTask(entity);
        try {
            HttpToolResponse minimalCancel = httpPost("myMinimal", "/v1/activities/" + minimalAttempt.getId() + "/cancel", new byte[0]);
            Assert.assertEquals(minimalCancel.getResponseCode(), 403);
            Assert.assertFalse(minimalAttempt.isCancelled());
        } finally {
            minimalAttempt.cancel(true);
        }

        Task<?> unrecognisedAttempt = newCancellableTask(entity);
        try {
            HttpToolResponse unrecognisedCancel = httpPost("unrecognisedUser", "/v1/activities/" + unrecognisedAttempt.getId() + "/cancel", new byte[0]);
            Assert.assertEquals(unrecognisedCancel.getResponseCode(), 403);
            Assert.assertFalse(unrecognisedAttempt.isCancelled());
        } finally {
            unrecognisedAttempt.cancel(true);
        }
    }

    @Test
    public void fineGrainedModifyEntitlementGatesCancelPerEntity() throws Exception {
        Task<?> task = newCancellableTask(entity);
        try {
            // entitled to modify a DIFFERENT entity only -> still forbidden to cancel this task
            StaticDelegatingEntitlementManager.setDelegate(new ModifySelectiveEntity(otherEntity));
            HttpToolResponse denied = httpPost("myCustom", "/v1/activities/" + task.getId() + "/cancel", new byte[0]);
            Assert.assertEquals(denied.getResponseCode(), 403);
            Assert.assertFalse(task.isCancelled());

            // entitled to modify THIS entity -> now permitted
            StaticDelegatingEntitlementManager.setDelegate(new ModifySelectiveEntity(entity));
            HttpToolResponse allowed = httpPost("myCustom", "/v1/activities/" + task.getId() + "/cancel", new byte[0]);
            Assert.assertEquals(allowed.getResponseCode(), 200);
            Assert.assertTrue(task.isCancelled());
        } finally {
            task.cancel(true);
        }
    }

    @Test
    public void childrenAndRecurseFilterDescendantsByEntityVisibility() throws Exception {
        // tagForTargetEntity (not tagForContextEntity) is required here: a queued child's context-entity
        // tag gets silently stripped and replaced by the ambient/parent execution context's own entity
        // unless the child explicitly requests a context switch via tagForTargetEntity (see
        // BasicExecutionContext.checkUserSuppliedContext/getContextSwitchingTask).
        Task<Void> grandchildOnVisibleEntity = Tasks.<Void>builder().displayName("grandchild-on-visible-entity").tag(BrooklynTaskTags.tagForTargetEntity(entity)).body((Callable<Void>) () -> null).build();
        Task<Void> childOnHiddenEntity = Tasks.<Void>builder().displayName("child-on-hidden-entity").tag(BrooklynTaskTags.tagForTargetEntity(otherEntity)).body((Callable<Void>) () -> null).add(grandchildOnVisibleEntity).build();
        Task<Void> childOnVisibleEntity = Tasks.<Void>builder().displayName("child-on-visible-entity").tag(BrooklynTaskTags.tagForContextEntity(entity)).body((Callable<Void>) () -> null).build();
        Task<Void> parentTask = Tasks.<Void>builder().displayName("parent").tag(BrooklynTaskTags.tagForContextEntity(entity)).body((Callable<Void>) () -> null).add(childOnHiddenEntity).add(childOnVisibleEntity).build();
        ((EntityInternal) entity).getExecutionContext().submit(parentTask);
        parentTask.getUnchecked(Asserts.DEFAULT_LONG_TIMEOUT);

        String childrenPath = "/v1/activities/" + parentTask.getId() + "/children";
        String recursePath = "/v1/activities/" + parentTask.getId() + "/children/recurse";

        // built-in roles grant SEE_ENTITY blanket across all entities -> everything visible
        for (String user : ImmutableList.of("myRoot", "myUser", "myReadonly")) {
            Set<String> childrenIds = childrenArrayIds(httpGet(user, childrenPath));
            Assert.assertTrue(childrenIds.contains(childOnHiddenEntity.getId()), "user=" + user + "; children=" + childrenIds);
            Assert.assertTrue(childrenIds.contains(childOnVisibleEntity.getId()), "user=" + user + "; children=" + childrenIds);

            Set<String> recurseIds = recurseMapIds(httpGet(user, recursePath));
            Assert.assertTrue(recurseIds.contains(childOnHiddenEntity.getId()), "user=" + user + "; recurse=" + recurseIds);
            Assert.assertTrue(recurseIds.contains(childOnVisibleEntity.getId()), "user=" + user + "; recurse=" + recurseIds);
            Assert.assertTrue(recurseIds.contains(grandchildOnVisibleEntity.getId()), "user=" + user + "; recurse=" + recurseIds);
        }

        // fine-grained: entitled to see `entity` but not `otherEntity`
        StaticDelegatingEntitlementManager.setDelegate(new SeeSelectiveEntity(entity));

        Set<String> childrenIds = childrenArrayIds(httpGet("myCustom", childrenPath));
        Assert.assertFalse(childrenIds.contains(childOnHiddenEntity.getId()), "children=" + childrenIds);
        Assert.assertTrue(childrenIds.contains(childOnVisibleEntity.getId()), "children=" + childrenIds);

        // recurse: the hidden child itself is filtered out as its own entry, but traversal still
        // descends into its subtree, so a visible-entity grandchild nested beneath it is still reported
        // (its "submittedByTask" link may still name the hidden task's id/entity as metadata - that's a
        // separate, narrower gap in TaskTransformer's asLink(), not covered by this top-level filter)
        Set<String> recurseIds = recurseMapIds(httpGet("myCustom", recursePath));
        Assert.assertFalse(recurseIds.contains(childOnHiddenEntity.getId()), "recurse=" + recurseIds);
        Assert.assertTrue(recurseIds.contains(childOnVisibleEntity.getId()), "recurse=" + recurseIds);
        Assert.assertTrue(recurseIds.contains(grandchildOnVisibleEntity.getId()), "recurse=" + recurseIds);
    }

    /**
     * ids of the top-level entries in a {@code children} JSON array response (not any nested link metadata)
     */
    private Set<String> childrenArrayIds(String json) throws Exception {
        Set<String> ids = Sets.newLinkedHashSet();
        for (JsonNode node : new ObjectMapper().readTree(json)) {
            ids.add(node.get("id").asText());
        }
        return ids;
    }

    /**
     * ids of the top-level entries (map keys) in a {@code children/recurse} JSON object response
     */
    private Set<String> recurseMapIds(String json) throws Exception {
        Set<String> ids = Sets.newLinkedHashSet();
        new ObjectMapper().readTree(json).fieldNames().forEachRemaining(ids::add);
        return ids;
    }

    private Task<?> newCancellableTask(Entity contextEntity) throws Exception {
        final CountDownLatch started = new CountDownLatch(1);
        Task<Void> task = new BasicTask<Void>(MutableMap.of("displayName", "cancellable test task", "tags", ImmutableList.of(BrooklynTaskTags.tagForContextEntity(contextEntity))), (Callable<Void>) () -> {
            started.countDown();
            Thread.sleep(60000);
            return null;
        });
        mgmt.getExecutionManager().submit(task);
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
        return task;
    }

    public static class ModifySelectiveEntity implements EntitlementManager {
        private final Entity entity;

        public ModifySelectiveEntity(Entity entity) {
            this.entity = entity;
        }

        @Override
        public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> entitlementClass, T entitlementClassArgument) {
            String type = entitlementClass.entitlementClassIdentifier();
            if (Entitlements.MODIFY_ENTITY.entitlementClassIdentifier().equals(type)) {
                return entity.equals(entitlementClassArgument);
            } else {
                return true;
            }
        }
    }

    public static class SeeSelectiveEntity implements EntitlementManager {
        private final Entity entity;

        public SeeSelectiveEntity(Entity entity) {
            this.entity = entity;
        }

        @Override
        public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> entitlementClass, T entitlementClassArgument) {
            String type = entitlementClass.entitlementClassIdentifier();
            if (Entitlements.SEE_ENTITY.entitlementClassIdentifier().equals(type)) {
                return entity.equals(entitlementClassArgument);
            } else {
                return true;
            }
        }
    }
}
