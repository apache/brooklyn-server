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
package org.apache.brooklyn.rest.resources;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.effector.SampleManyTasksEffector;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils.CreationResult;
import org.apache.brooklyn.core.mgmt.internal.TestEntityWithEffectors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.http.HttpAsserts;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

/** Tests {@link ActivityResource} and activity methods on {@link EntityResource} */
public class ActivityRestTest extends BrooklynRestResourceTest {

    private static final Logger log = LoggerFactory.getLogger(ActivityRestTest.class);
    
    /* a nice seed, initial run as follows;

Task[eatand]@J90TKfIX: Waiting on Task[eat-sleep-rave-repeat]@QPa5o4kF
  Task[eat-sleep-rave-repeat]@QPa5o4kF: Waiting on Task[rave]@yP9KjuWD
    Task[rave]@yP9KjuWD: Waiting on Task[repeat]@Dd1AqB7Q
      Task[repeat]@Dd1AqB7Q: Waiting on Task[repeat]@remQL5eD
        Task[repeat]@remQL5eD: Waiting on Task[repeat]@g1ReP4BP
          Task[sleep]@iV3iWg2N: Completed, result: slept 46ms
          Task[eat]@fpIttX07: Completed, result: eat
          Task[eat]@w6sxLefq: Completed, result: eat
          Task[repeat]@g1ReP4BP: Waiting on Task[sleep]@zRTOQ4ak
            Task[eat]@TvcdOUx7: Completed, result: eat
            Task[rave]@yJndzNLf: Completed, result: raved with 1 tasks
              Task[eat]@oiJ3eZZQ: Completed, result: eat
            Task[sleep]@zRTOQ4ak: sleeping 74ms
            Task[eat]@qoFRPEfM: Not submitted
            Task[sleep]@fNX16uvi: Not submitted

     */
    private final int SEED = 1;
    
    private Entity entity;
    private Effector<?> effector;

    private Task<?> lastTask;
    
    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer();
    }
    
    @BeforeMethod(alwaysRun = true)
    public void setUpOneTest() throws Exception {
        initEntity(SEED);
    }

    @SuppressWarnings("deprecation")
    protected void initEntity(int seed) {
        if (entity!=null) {
            Entities.destroy(entity.getApplication());
        }
        
        CreationResult<BasicApplication, Void> app = EntityManagementUtils.createStarting(getManagementContext(),
            EntitySpec.create(BasicApplication.class)
                .child(EntitySpec.create(TestEntityWithEffectors.class)) );
        app.blockUntilComplete();
        entity = Iterables.getOnlyElement( app.get().getChildren() );
        
        SampleManyTasksEffector manyTasksAdder = new SampleManyTasksEffector(ConfigBag.newInstance().configure(SampleManyTasksEffector.RANDOM_SEED, seed));
        effector = manyTasksAdder.getEffector();
        manyTasksAdder.apply((org.apache.brooklyn.api.entity.EntityLocal) entity);
    }

    /** finds a good seed, in case the effector changes */
    public static void main(String[] args) throws Exception {
        ActivityRestTest me = new ActivityRestTest();
        me.setUpClass();
        int i=0;
        do {
            me.initEntity(i);
            try {
                log.info("Trying seed "+i+"...");
                me.testGood(Duration.millis(200));
                break;
            } catch (Throwable e) {
                log.info("  "+Exceptions.collapseText(e));
                // e.printStackTrace();
                // continue
            }
            i++;
        } while (true);
        Tasks.dumpInfo(me.lastTask);
        log.info("Seed "+i+" is good ^");
    }
    
    @Test
    public void testGood() {
        testGood(Duration.ONE_SECOND);
    }
    
    void testGood(Duration timeout) {
        lastTask = entity.invoke(effector, null);
        Task<?> leaf = waitForCompletedDescendantWithChildAndSibling(lastTask, lastTask, CountdownTimer.newInstanceStarted(timeout), 0);
        Assert.assertTrue(depthOf(leaf)>=4, "Not deep enough: "+depthOf(leaf));
    }
    
    @Test
    public void testGetActivity() {
        Task<?> t = entity.invoke(effector, null);
        
        Response response = client().path("/activities/"+t.getId())
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        TaskSummary task = response.readEntity(TaskSummary.class);
        Assert.assertEquals(task.getId(), t.getId());
    }
    
    @Test
    public void testGetActivitiesChildren() {
        Task<?> t = entity.invoke(effector, null);
        Task<?> leaf = waitForCompletedDescendantWithChildAndSibling(t, t, CountdownTimer.newInstanceStarted(Duration.ONE_SECOND), 0);
        
        Response response = client().path("/activities/"+leaf.getSubmittedByTask().getId()+"/children")
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        List<TaskSummary> tasks = response.readEntity(new GenericType<List<TaskSummary>>() {});
        log.info("Tasks children: "+tasks.size());
        Assert.assertTrue(tasksContain(tasks, leaf), "tasks should have included leaf "+leaf+"; was "+tasks);
    }
    
    @Test
    public void testGetActivitiesRecursiveAndWithLimit() {
        Task<?> t = entity.invoke(effector, null);
        Task<?> leaf = waitForCompletedDescendantWithChildAndSibling(t, t, CountdownTimer.newInstanceStarted(Duration.ONE_SECOND), 0);
        Task<?> leafParent = leaf.getSubmittedByTask();
        Task<?> leafGrandparent = leafParent.getSubmittedByTask();
        
        Response response = client().path("/activities/"+leafGrandparent.getId()+"/children")
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        List<TaskSummary> tasksL = response.readEntity(new GenericType<List<TaskSummary>>() {});
        Assert.assertFalse(tasksContain(tasksL, leaf), "non-recursive tasks should not have included leaf "+leaf+"; was "+tasksL);
        Assert.assertTrue(tasksContain(tasksL, leafParent), "non-recursive tasks should have included leaf parent "+leafParent+"; was "+tasksL);
        Assert.assertFalse(tasksContain(tasksL, leafGrandparent), "non-recursive tasks should not have included leaf grandparent "+leafGrandparent+"; was "+tasksL);
        
        response = client().path("/activities/"+leafGrandparent.getId()+"/children/recurse")
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        Map<String,TaskSummary> tasks = response.readEntity(new GenericType<Map<String,TaskSummary>>() {});
        Assert.assertTrue(tasksContain(tasks, leaf), "recursive tasks should have included leaf "+leaf+"; was "+tasks);
        Assert.assertTrue(tasksContain(tasks, leafParent), "recursive tasks should have included leaf parent "+leafParent+"; was "+tasks);
        Assert.assertFalse(tasksContain(tasks, leafGrandparent), "recursive tasks should not have included leaf grandparent "+leafGrandparent+"; was "+tasks);
        
        response = client().path("/activities/"+leafGrandparent.getId()+"/children/recurse")
            .query("maxDepth", 1)
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        tasks = response.readEntity(new GenericType<Map<String,TaskSummary>>() {});
        Assert.assertFalse(tasksContain(tasks, leaf), "depth 1 recursive tasks should nont have included leaf "+leaf+"; was "+tasks);
        Assert.assertTrue(tasksContain(tasks, leafParent), "depth 1 recursive tasks should have included leaf parent "+leafParent+"; was "+tasks);

        response = client().path("/activities/"+leafGrandparent.getId()+"/children/recurse")
            .query("maxDepth", 2)
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        tasks = response.readEntity(new GenericType<Map<String,TaskSummary>>() {});
        Assert.assertTrue(tasksContain(tasks, leaf), "depth 2 recursive tasks should have included leaf "+leaf+"; was "+tasks);
        Assert.assertTrue(tasksContain(tasks, leafParent), "depth 2 recursive tasks should have included leaf parent "+leafParent+"; was "+tasks);
        Assert.assertFalse(tasksContain(tasks, leafGrandparent), "depth 2 recursive tasks should not have included leaf grandparent "+leafGrandparent+"; was "+tasks);
        
        Assert.assertTrue(children(leafGrandparent).size() >= 2, "children: "+children(leafGrandparent));
        response = client().path("/activities/"+leafGrandparent.getId()+"/children/recurse")
            .query("limit", children(leafGrandparent).size())
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        tasks = response.readEntity(new GenericType<Map<String,TaskSummary>>() {});
        Assert.assertEquals(tasks.size(), children(leafGrandparent).size());
        Assert.assertTrue(tasksContain(tasks, leafParent), "count limited recursive tasks should have included leaf parent "+leafParent+"; was "+tasks);
        Assert.assertFalse(tasksContain(tasks, leaf), "count limited recursive tasks should not have included leaf "+leaf+"; was "+tasks);
        
        response = client().path("/activities/"+leafGrandparent.getId()+"/children/recurse")
            .query("limit", children(leafGrandparent).size()+1)
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        tasks = response.readEntity(new GenericType<Map<String,TaskSummary>>() {});
        Assert.assertEquals(tasks.size(), children(leafGrandparent).size()+1);
        tasks = response.readEntity(new GenericType<Map<String,TaskSummary>>() {});
        Assert.assertTrue(tasksContain(tasks, leafParent), "count+1 limited recursive tasks should have included leaf parent "+leafParent+"; was "+tasks);
        Assert.assertTrue(tasksContain(tasks, leaf), "count+1 limited recursive tasks should have included leaf "+leaf+"; was "+tasks);
    }

    private boolean tasksContain(Map<String, TaskSummary> tasks, Task<?> leaf) {
        return tasks.keySet().contains(leaf.getId());
    }

    private List<Task<?>> children(Task<?> t) {
        return MutableList.copyOf( ((HasTaskChildren)t).getChildren() );
    }

    @Test
    public void testGetEntityActivitiesAndWithLimit() {
        Task<?> t = entity.invoke(effector, null);
        Task<?> leaf = waitForCompletedDescendantWithChildAndSibling(t, t, CountdownTimer.newInstanceStarted(Duration.ONE_SECOND), 0);
        
        Response response = client().path("/applications/"+entity.getApplicationId()+
                "/entities/"+entity.getId()+"/activities")
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        List<TaskSummary> tasks = response.readEntity(new GenericType<List<TaskSummary>>() {});
        log.info("Tasks now: "+tasks.size());
        Assert.assertTrue(tasks.size() > 4, "tasks should have been big; was "+tasks);
        Assert.assertTrue(tasksContain(tasks, leaf), "tasks should have included leaf "+leaf+"; was "+tasks);
        
        response = client().path("/applications/"+entity.getApplicationId()+
            "/entities/"+entity.getId()+"/activities")
            .query("limit", 3)
            .accept(MediaType.APPLICATION_JSON)
            .get();
        assertHealthy(response);
        tasks = response.readEntity(new GenericType<List<TaskSummary>>() {});
        log.info("Tasks limited: "+tasks.size());
        Assert.assertEquals(tasks.size(), 3, "tasks should have been limited; was "+tasks);
        Assert.assertFalse(tasksContain(tasks, leaf), "tasks should not have included leaf "+leaf+"; was "+tasks);
    }

    private void assertHealthy(Response response) {
        if (!HttpAsserts.isHealthyStatusCode(response.getStatus())) {
            Asserts.fail("Bad response: "+response.getStatus()+" "+response.readEntity(String.class));
        }
    }

    private static boolean tasksContain(List<TaskSummary> tasks, Task<?> leaf) {
        for (TaskSummary ts: tasks) {
            if (ts.getId().equals(leaf.getId())) return true;
        }
        return false;
    }

    private int depthOf(Task<?> t) {
        int depth = -1;
        while (t!=null) {
            t = t.getSubmittedByTask();
            depth++;
        }
        return depth;
    }
    
    private Task<?> waitForCompletedDescendantWithChildAndSibling(Task<?> tRoot, Task<?> t, CountdownTimer timer, int depthSoFar) {
        while (timer.isLive()) {
            Iterable<Task<?>> children = ((HasTaskChildren)t).getChildren();
            Iterator<Task<?>> ci = children.iterator();
            Task<?> bestFinishedDescendant = null;
            while (ci.hasNext()) {
                Task<?> tc = ci.next();
                Task<?> finishedDescendant = waitForCompletedDescendantWithChildAndSibling(tRoot, tc, timer, depthSoFar+1);
                if (depthOf(finishedDescendant) > depthOf(bestFinishedDescendant)) {
                    bestFinishedDescendant = finishedDescendant;
                }
                int finishedDescendantDepth = depthOf(bestFinishedDescendant);
                // log.info("finished "+tc+", depth "+finishedDescendantDepth);
                if (finishedDescendantDepth < 2) {
                    if (ci.hasNext()) continue;
                    throw new IllegalStateException("not deep enough: "+finishedDescendantDepth);
                }
                if (finishedDescendantDepth == depthSoFar+1) {
                    // child completed; now check we complete soon, and assert we have siblings
                    if (ci.hasNext()) continue;
                    if (!t.blockUntilEnded(timer.getDurationRemaining())) {
                        Entities.dumpInfo(tRoot);
                        // log.info("Incomplete after "+t+": "+t.getStatusDetail(false));
                        throw Exceptions.propagate( new TimeoutException("parent didn't complete after child depth "+finishedDescendantDepth) );
                    }
                }
                if (finishedDescendantDepth == depthSoFar+2) {
                    if (Iterables.size(children)<2) {
                        Entities.dumpInfo(tRoot);
                        throw new IllegalStateException("finished child's parent has no sibling");
                    }
                }
                
                return bestFinishedDescendant;
            }
            Thread.yield();
            
            // leaf nodeå
            if (t.isDone()) return t;
        }
        throw Exceptions.propagate( new TimeoutException("expired waiting for children") );
    }
    
}
