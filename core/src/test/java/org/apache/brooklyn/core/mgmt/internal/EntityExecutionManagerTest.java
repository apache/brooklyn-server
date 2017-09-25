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
package org.apache.brooklyn.core.mgmt.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags.WrappedEntity;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags.WrappedItem;
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.ExecutionListener;
import org.apache.brooklyn.util.core.task.ScheduledTask;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Callables;

/** Includes many tests for {@link BrooklynGarbageCollector} */
@Test
public class EntityExecutionManagerTest extends BrooklynAppUnitTestSupport {
    
    private static final Logger LOG = LoggerFactory.getLogger(EntityExecutionManagerTest.class);

    public void testOnDoneCallback() throws InterruptedException {
        ExecutionManager em = mgmt.getExecutionManager();
        BasicExecutionManager bem = (BasicExecutionManager)em;
        final Map<Task<?>,Duration> completedTasks = MutableMap.of();
        final CountDownLatch latch = new CountDownLatch(2);
        bem.addListener(new ExecutionListener() {
            @Override
            public void onTaskDone(Task<?> task) {
                Assert.assertTrue(task.isDone());
                Object result = task.getUnchecked();
                if (result != null && result.equals("foo")) {
                    synchronized (completedTasks) {
                        completedTasks.put(task, Duration.sinceUtc(task.getEndTimeUtc()));
                    }
                    latch.countDown();
                }
            }
        });
        Task<String> t1 = em.submit(
            Tasks.<String>builder()
                .displayName("t1")
                .dynamic(false)
                .body(Callables.returning("foo"))
                .build());
        Task<String> t2 = em.submit(
            Tasks.<String>builder()
                .displayName("t2")
                .dynamic(false)
                .body(Callables.returning("foo"))
                .build());
        latch.await(Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        synchronized (completedTasks) {
            Assert.assertEquals(completedTasks.size(), 2, "completed tasks should be 2 but are: "+completedTasks);
            completedTasks.get(t1).isShorterThan(Duration.TEN_SECONDS);
            completedTasks.get(t2).isShorterThan(Duration.TEN_SECONDS);
        }
    }
    
    protected void forceGc() {
        ((LocalManagementContext)mgmt).getGarbageCollector().gcIteration();
    }

    protected static Task<?> runEmptyTaskWithNameAndTags(Entity target, String name, Object ...tags) {
        TaskBuilder<Object> tb = newEmptyTask(name);
        for (Object tag: tags) tb.tag(tag);
        Task<?> task = ((EntityInternal)target).getExecutionContext().submit(tb.build());
        task.getUnchecked();
        return task;
    }

    protected static TaskBuilder<Object> newEmptyTask(String name) {
        return Tasks.builder().displayName(name).dynamic(false).body(Callables.returning(null));
    }

    protected void assertImportantTaskCountForEntityEventually(final Entity entity, final int expectedCount) {
        // Dead task (and initialization task) should have been GC'd on completion.
        // However, the GC'ing happens in a listener, executed in a different thread - the task.get()
        // doesn't block for it. Therefore can't always guarantee it will be GC'ed by now.
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                forceGc();  
                Collection<Task<?>> tasks = removeSystemTasks(BrooklynTaskTags.getTasksInEntityContext(((EntityInternal)entity).getManagementContext().getExecutionManager(), entity));
                Assert.assertEquals(tasks.size(), expectedCount, "Tasks were "+tasks);
            }});
    }

    static Set<Task<?>> removeSystemTasks(Iterable<Task<?>> tasks) {
        Set<Task<?>> result = MutableSet.of();
        for (Task<?> t: tasks) {
            if (t instanceof ScheduledTask) continue;
            if (t.getTags().contains(BrooklynTaskTags.SENSOR_TAG)) continue;
            if (t.getDisplayName().contains("Validating")) continue;
            result.add(t);
        }
        return result;
    }

    // Needed because of https://issues.apache.org/jira/browse/BROOKLYN-401
    protected void assertTaskMaxCountForEntityEventually(final Entity entity, final int expectedMaxCount) {
        // Dead task (and initialization task) should have been GC'd on completion.
        // However, the GC'ing happens in a listener, executed in a different thread - the task.get()
        // doesn't block for it. Therefore can't always guarantee it will be GC'ed by now.
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                forceGc();
                Collection<Task<?>> tasks = removeSystemTasks( BrooklynTaskTags.getTasksInEntityContext(((EntityInternal)entity).getManagementContext().getExecutionManager(), entity) );
                Assert.assertTrue(tasks.size() <= expectedMaxCount,
                        "Expected tasks count max of " + expectedMaxCount + ". Tasks were "+tasks);
            }});
    }

    public void testGetTasksAndGcBoringTags() throws Exception {
        TestEntity e = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        final Task<?> task = runEmptyTaskWithNameAndTags(e, "should-be-kept", ManagementContextInternal.NON_TRANSIENT_TASK_TAG);
        runEmptyTaskWithNameAndTags(e, "should-be-gcd", ManagementContextInternal.TRANSIENT_TASK_TAG);
        
        assertImportantTaskCountForEntityEventually(e, 1);
        Collection<Task<?>> tasks = removeSystemTasks( BrooklynTaskTags.getTasksInEntityContext(app.getManagementContext().getExecutionManager(), e) );
        assertEquals(tasks, ImmutableList.of(task), "Mismatched tasks, got: "+tasks);
    }

    public void testGcTaskAtNormalTagLimit() throws Exception {
        TestEntity e = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        ((BrooklynProperties)app.getManagementContext().getConfig()).put(
            BrooklynGarbageCollector.MAX_TASKS_PER_TAG, 2);

        AtomicBoolean stopCondition = new AtomicBoolean();
        scheduleRecursiveTemporaryTask(stopCondition, e, "boring-tag");
        scheduleRecursiveTemporaryTask(stopCondition, e, "boring-tag");

        for (int count=0; count<5; count++)
            runEmptyTaskWithNameAndTags(e, "task"+count, ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag");

        // Makes sure there's a GC while the transient tasks are running
        forceGc();

        stopCondition.set(true);

        assertTaskMaxCountForEntityEventually(e, 2);
    }

    public void testGcTaskAtEntityLimit() throws Exception {
        final TestEntity e = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        ((BrooklynProperties)app.getManagementContext().getConfig()).put(
            BrooklynGarbageCollector.MAX_TASKS_PER_ENTITY, 2);
        
        AtomicBoolean stopCondition = new AtomicBoolean();
        scheduleRecursiveTemporaryTask(stopCondition, e, "boring-tag");
        scheduleRecursiveTemporaryTask(stopCondition, e, "boring-tag");
        scheduleRecursiveTemporaryTask(stopCondition, app, "boring-tag");
        scheduleRecursiveTemporaryTask(stopCondition, app, "boring-tag");

        for (int count=0; count<5; count++)
            runEmptyTaskWithNameAndTags(e, "task-e-"+count, ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag");
        for (int count=0; count<5; count++)
            runEmptyTaskWithNameAndTags(app, "task-app-"+count, ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag");

        // Makes sure there's a GC while the transient tasks are running
        forceGc();
        stopCondition.set(true);

        assertTaskMaxCountForEntityEventually(app, 2);
        assertTaskMaxCountForEntityEventually(e, 2);
    }

    public void testGcTaskWithTagAndEntityLimit() throws Exception {
        TestEntity e = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        ((BrooklynProperties)app.getManagementContext().getConfig()).put(
            BrooklynGarbageCollector.MAX_TASKS_PER_ENTITY, 6);
        ((BrooklynProperties)app.getManagementContext().getConfig()).put(
            BrooklynGarbageCollector.MAX_TASKS_PER_TAG, 2);

        AtomicBoolean stopCondition = new AtomicBoolean();
        scheduleRecursiveTemporaryTask(stopCondition, e, "boring-tag");
        scheduleRecursiveTemporaryTask(stopCondition, e, "boring-tag");
        scheduleRecursiveTemporaryTask(stopCondition, app, "boring-tag");
        scheduleRecursiveTemporaryTask(stopCondition, app, "boring-tag");

        int count=0;
        
        runEmptyTaskWithNameAndTags(app, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag");
        runEmptyTaskWithNameAndTags(e, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag");
        Time.sleep(Duration.ONE_MILLISECOND);
        // should keep the 2 below, because all the other borings get grace, but delete the ones above
        runEmptyTaskWithNameAndTags(e, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag");
        runEmptyTaskWithNameAndTags(e, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag");
        
        runEmptyTaskWithNameAndTags(e, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag", "another-tag-e");
        runEmptyTaskWithNameAndTags(e, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "boring-tag", "another-tag-e");
        // should keep both the above
        
        runEmptyTaskWithNameAndTags(e, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "another-tag");
        runEmptyTaskWithNameAndTags(e, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "another-tag");
        Time.sleep(Duration.ONE_MILLISECOND);
        runEmptyTaskWithNameAndTags(app, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "another-tag");
        // should keep the below since they have unique tags, but remove one of the e tasks above 
        runEmptyTaskWithNameAndTags(e, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "another-tag", "and-another-tag");
        runEmptyTaskWithNameAndTags(app, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "another-tag-app", "another-tag");
        runEmptyTaskWithNameAndTags(app, "task-"+(count++), ManagementContextInternal.NON_TRANSIENT_TASK_TAG, "another-tag-app", "another-tag");
        
        // Makes sure there's a GC while the transient tasks are running
        forceGc();
        stopCondition.set(true);

        assertTaskMaxCountForEntityEventually(e, 6);
        assertTaskMaxCountForEntityEventually(app, 3);
        
        // now with a lowered limit, we should remove one more e
        ((BrooklynProperties)app.getManagementContext().getConfig()).put(
            BrooklynGarbageCollector.MAX_TASKS_PER_ENTITY, 5);
        assertTaskMaxCountForEntityEventually(e, 5);
    }

    public void testGcDynamicTaskAtNormalTagLimit() throws Exception {
        TestEntity e = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        
        ((BrooklynProperties)app.getManagementContext().getConfig()).put(
            BrooklynGarbageCollector.MAX_TASKS_PER_TAG, 2);

        AtomicBoolean stopCondition = new AtomicBoolean();
        scheduleRecursiveTemporaryTask(stopCondition, e, "foo");
        scheduleRecursiveTemporaryTask(stopCondition, e, "foo");

        for (int count=0; count<5; count++) {
            TaskBuilder<Object> tb = Tasks.builder().displayName("task-"+count).dynamic(true).body(new Runnable() { @Override public void run() {}})
                .tag(ManagementContextInternal.NON_TRANSIENT_TASK_TAG).tag("foo");
            ((EntityInternal)e).getExecutionContext().submit(tb.build()).getUnchecked();
        }

        // Makes sure there's a GC while the transient tasks are running
        forceGc();
        stopCondition.set(true);

        assertTaskMaxCountForEntityEventually(e, 2);
    }

    public void testUnmanagedEntityCanBeGcedEvenIfPreviouslyTagged() throws Exception {
        TestEntity e = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        String eId = e.getId();
        
        e.invoke(TestEntity.MY_EFFECTOR, ImmutableMap.<String,Object>of()).get();
        Set<Task<?>> tasks = BrooklynTaskTags.getTasksInEntityContext(app.getManagementContext().getExecutionManager(), e);
        Task<?> task = Iterables.get(tasks, 0);
        assertTrue(task.getTags().contains(BrooklynTaskTags.tagForContextEntity(e)));

        Set<Object> tags = app.getManagementContext().getExecutionManager().getTaskTags();
        assertTrue(tags.contains(BrooklynTaskTags.tagForContextEntity(e)), "tags="+tags);
        
        Entities.destroy(e);
        forceGc();
        
        Set<Object> tags2 = app.getManagementContext().getExecutionManager().getTaskTags();
        for (Object tag : tags2) {
            if (tag instanceof Entity && ((Entity)tag).getId().equals(eId)) {
                fail("tags contains unmanaged entity "+tag);
            }
            if ((tag instanceof WrappedEntity) && ((WrappedEntity)tag).unwrap().getId().equals(eId) 
                    && ((WrappedItem<?>)tag).getWrappingType().equals(BrooklynTaskTags.CONTEXT_ENTITY)) {
                fail("tags contains unmanaged entity (wrapped) "+tag);
            }
        }
        return;
    }

    @Test(groups="Integration")
    public void testSubscriptionAndEffectorTasksGced() throws Exception {
        BasicExecutionManager em = (BasicExecutionManager) app.getManagementContext().getExecutionManager();
        // allow background enrichers to complete
        Time.sleep(Duration.ONE_SECOND);
        forceGc();
        Collection<Task<?>> t1 = em.getAllTasks();

        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        entity.sensors().set(TestEntity.NAME, "bob");
        entity.invoke(TestEntity.MY_EFFECTOR, ImmutableMap.<String,Object>of()).get();
        Entities.destroy(entity);
        Time.sleep(Duration.ONE_SECOND);
        forceGc();
        Collection<Task<?>> t2 = em.getAllTasks();

        // no tasks from first batch were GC'd
        Asserts.assertSize(MutableList.builder().addAll(t1).removeAll(t2).build(), 0);

        // and we expect just the add/remove cycle at parent, and service problems
        Set<String> newOnes = MutableList.<Task<?>>builder().addAll(t2).removeAll(t1).build().stream().map(
            (t) -> t.getDisplayName()).collect(Collectors.toSet());
        Function<String,String> prefix = (s) -> "sensor "+app.getId()+":"+s;
        Assert.assertEquals(newOnes, MutableSet.of(
            prefix.apply("entity.children.removed"), prefix.apply("entity.children.added"), prefix.apply("service.problems"))); 
    }

    /**
     * Invoke effector many times, where each would claim 10MB because it stores the return value.
     * If it didn't gc the tasks promptly, it would consume 10GB ram (so would OOME before that).
     */
    @Test(groups="Integration")
    public void testEffectorTasksGcedSoNoOome() throws Exception {
        String classAndMethodName = JavaClassNames.niceClassAndMethod();

        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put(BrooklynGarbageCollector.GC_PERIOD, Duration.ONE_MILLISECOND);
        brooklynProperties.put(BrooklynGarbageCollector.MAX_TASKS_PER_TAG, 2);

        replaceManagementContext(LocalManagementContextForTests.newInstance(brooklynProperties));
        setUpApp();
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        for (int i = 0; i < 1000; i++) {
            if (i%100==0) LOG.info(classAndMethodName+": iteration "+i);
            try {
                LOG.debug("testEffectorTasksGced: iteration="+i);
                entity.invoke(TestEntity.IDENTITY_EFFECTOR, ImmutableMap.of("arg", new BigObject(10*1000*1000))).get();

                Time.sleep(Duration.ONE_MILLISECOND); // Give GC thread a chance to run
                forceGc();
            } catch (OutOfMemoryError e) {
                LOG.warn(classAndMethodName+": OOME at iteration="+i);
                throw e;
            }
        }
    }

    @Test(groups="Integration")
    public void testUnmanagedEntityGcedOnUnmanageEvenIfEffectorInvoked() throws Exception {
        String classAndMethodName = JavaClassNames.niceClassAndMethod();
        BasicAttributeSensor<Object> byteArrayAttrib = new BasicAttributeSensor<Object>(Object.class, "test.byteArray", "");
        for (int i = 0; i < 1000; i++) {
            if (i<100 && i%10==0 || i%100==0) LOG.info(classAndMethodName+": iteration "+i);
            try {
                LOG.debug(classAndMethodName+": iteration="+i);
                TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
                entity.sensors().set(byteArrayAttrib, new BigObject(10*1000*1000));
                entity.invoke(TestEntity.MY_EFFECTOR, ImmutableMap.<String,Object>of()).get();

                // we get exceptions because tasks are still trying to publish after deployment;
                // this should prevent them
//                ((LocalEntityManager)app.getManagementContext().getEntityManager()).stopTasks(entity, Duration.ONE_SECOND);
//                Entities.destroy(entity);

                // alternatively if we 'unmanage' instead of destroy, there are usually not errors
                // (the errors come from the node transitioning to a 'stopping' state on destroy,
                // and publishing lots of info then)
                Entities.unmanage(entity);

                forceGc();
                // previously we did an extra System.gc() but it was crazy slow, shouldn't be needed
            } catch (OutOfMemoryError e) {
                LOG.warn(classAndMethodName+": OOME at iteration="+i);
                ExecutionManager em = app.getManagementContext().getExecutionManager();
                Collection<Task<?>> tasks = ((BasicExecutionManager)em).getAllTasks();
                LOG.info("TASKS count "+tasks.size()+": "+tasks);
                throw e;
            }
        }
    }

    @Test(groups={"Integration"})
    public void testEffectorTasksGcedForMaxPerTag() throws Exception {
        int maxNumTasks = 2;
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put(BrooklynGarbageCollector.GC_PERIOD, Duration.ONE_SECOND);
        brooklynProperties.put(BrooklynGarbageCollector.MAX_TASKS_PER_TAG, 2);

        replaceManagementContext(LocalManagementContextForTests.newInstance(brooklynProperties));
        setUpApp();
        final TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        List<Task<?>> tasks = Lists.newArrayList();

        for (int i = 0; i < (maxNumTasks+1); i++) {
            Task<?> task = entity.invoke(TestEntity.MY_EFFECTOR, ImmutableMap.<String,Object>of());
            task.get();
            tasks.add(task);

            // TASKS_OLDEST_FIRST_COMPARATOR is based on comparing EndTimeUtc; but two tasks executed in
            // rapid succession could finish in same millisecond
            // (especially when using System.currentTimeMillis, which can return the same time for several millisconds).
            Thread.sleep(10);
        }

        // Should initially have all tasks
        Set<Task<?>> storedTasks = app.getManagementContext().getExecutionManager().getTasksWithAllTags(
                ImmutableList.of(BrooklynTaskTags.tagForContextEntity(entity), ManagementContextInternal.EFFECTOR_TAG));
        assertEquals(storedTasks, ImmutableSet.copyOf(tasks), "storedTasks="+storedTasks+"; expected="+tasks);

        // Then oldest should be GC'ed to leave only maxNumTasks
        final List<Task<?>> recentTasks = tasks.subList(tasks.size()-maxNumTasks, tasks.size());
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                Set<Task<?>> storedTasks2 = app.getManagementContext().getExecutionManager().getTasksWithAllTags(
                       ImmutableList.of(BrooklynTaskTags.tagForContextEntity(entity), ManagementContextInternal.EFFECTOR_TAG));
                List<String> storedTasks2Str = FluentIterable
                        .from(storedTasks2)
                        .transform(new Function<Task<?>, String>() {
                            @Override public String apply(Task<?> input) {
                                return taskToVerboseString(input);
                            }})
                        .toList();
                assertEquals(storedTasks2, ImmutableSet.copyOf(recentTasks), "storedTasks="+storedTasks2Str+"; expected="+recentTasks);
            }});
    }

    private String taskToVerboseString(Task<?> t) {
        return MoreObjects.toStringHelper(t)
                .add("id", t.getId())
                .add("displayName", t.getDisplayName())
                .add("submitTime", t.getSubmitTimeUtc())
                .add("startTime", t.getStartTimeUtc())
                .add("endTime", t.getEndTimeUtc())
                .add("status", t.getStatusSummary())
                .add("tags", t.getTags())
                .toString();
    }

    @Test(groups="Integration")
    public void testEffectorTasksGcedForAge() throws Exception {
        Duration maxTaskAge = Duration.millis(100);
        Duration maxOverhead = Duration.millis(250);
        Duration earlyReturnGrace = Duration.millis(10);
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put(BrooklynGarbageCollector.GC_PERIOD, Duration.ONE_MILLISECOND);
        brooklynProperties.put(BrooklynGarbageCollector.MAX_TASK_AGE, maxTaskAge);

        replaceManagementContext(LocalManagementContextForTests.newInstance(brooklynProperties));
        setUpApp();
        final TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Stopwatch stopwatch = Stopwatch.createStarted();
        Task<?> oldTask = entity.invoke(TestEntity.MY_EFFECTOR, ImmutableMap.<String,Object>of());
        oldTask.get();

        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                Set<Task<?>> storedTasks = app.getManagementContext().getExecutionManager().getTasksWithAllTags(ImmutableList.of(
                        BrooklynTaskTags.tagForTargetEntity(entity),
                        ManagementContextInternal.EFFECTOR_TAG));
                assertEquals(storedTasks, ImmutableSet.of(), "storedTasks="+storedTasks);
            }});

        Duration timeToGc = Duration.of(stopwatch);
        assertTrue(timeToGc.isLongerThan(maxTaskAge.subtract(earlyReturnGrace)), "timeToGc="+timeToGc+"; maxTaskAge="+maxTaskAge);
        assertTrue(timeToGc.isShorterThan(maxTaskAge.add(maxOverhead)), "timeToGc="+timeToGc+"; maxTaskAge="+maxTaskAge);
    }

    private static class BigObject implements Serializable {
        private static final long serialVersionUID = -4021304829674972215L;
        private final int sizeBytes;
        private final byte[] data;
        
        BigObject(int sizeBytes) {
            this.sizeBytes = sizeBytes;
            this.data = new byte[sizeBytes];
        }
        
        @Override
        public String toString() {
            return "BigObject["+sizeBytes+"/"+data.length+"]";
        }
    }

    private Task<?> scheduleRecursiveTemporaryTask(final AtomicBoolean stopCondition, final Entity e, final Object... additionalTags) {
        // TODO Could alternate the test with expiring tasks in addition to transient
        TaskBuilder<Object> tb = Tasks.builder()
                .displayName("recursive")
                .dynamic(false)
                .tag(ManagementContextInternal.TRANSIENT_TASK_TAG)
                .body(new Runnable() {
                    @Override
                    public void run() {
                        if (!stopCondition.get()) {
                            scheduleRecursiveTemporaryTask(stopCondition, e, additionalTags);
                        }
                    }
                });
        for (Object t : additionalTags) {
            tb.tag(t);
        }
        return ((EntityInternal)e).getExecutionContext().submit(tb.build());
    }
}
