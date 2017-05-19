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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.annotation.EffectorParam;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class MethodEffectorTest extends BrooklynAppUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(MethodEffectorTest.class);
    
    private static final long TIMEOUT = Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds();

    @ImplementedBy(MyEntityImpl.class)
    public interface MyEntity extends Entity, EntityInternal {
        public static MethodEffector<String> EFFECTOR_WITH_DEFAULTS = new MethodEffector<String>(MyEntity.class, "effectorWithDefaults");
        public static MethodEffector<String> EFFECTOR_WITH_TYPED_ARGS = new MethodEffector<String>(MyEntity.class, "effectorWithTypedArgs");
        public static MethodEffector<String> EFFECTOR_WITH_OBJECT_ARGS = new MethodEffector<String>(MyEntity.class, "effectorWithObjectArgs");
        public static MethodEffector<String> OVERLOADED = new MethodEffector<String>(MyEntity.class, "overloaded");
        public static MethodEffector<String> CONCATENATE = new MethodEffector<String>(MyEntity.class, "concatenate");
        public static MethodEffector<Void> WAIT_A_BIT = new MethodEffector<Void>(MyEntity.class, "waitabit");
        public static MethodEffector<Void> SPAWN_CHILD = new MethodEffector<Void>(MyEntity.class, "spawnchild");
        
        @Effector(description="sample effector concatenating strings")
        public String concatenate(
                @EffectorParam(name="first", description="first argument") String first,
                @EffectorParam(name="second", description="2nd arg") String second) throws Exception;
        
        @Effector
        public String effectorWithDefaults(
                @EffectorParam(name="first", defaultValue="firstDefault") String first,
                @EffectorParam(name="second", defaultValue="secondDefault") String second) throws Exception;
        
        @Effector
        public String effectorWithTypedArgs(
                @EffectorParam(name="booleanArg") boolean booleanArg,
                @EffectorParam(name="byteArg") byte byteArg,
                @EffectorParam(name="shortArg") short shortArg,
                @EffectorParam(name="intArg") int intArg,
                @EffectorParam(name="longArg") long longArg,
                @EffectorParam(name="floatArg") float floatArg,
                @EffectorParam(name="doubleArg") double doubleArg) throws Exception;
        
        @Effector
        public String effectorWithObjectArgs(
                @EffectorParam(name="objectArg") Object objectArg) throws Exception;
        
        @Effector
        public String overloaded() throws Exception;
        
        @Effector
        public String overloaded(
                @EffectorParam(name="first") String first) throws Exception;
        
        @Effector
        public String overloaded(
                @EffectorParam(name="first") Integer first) throws Exception;
        
        @Effector
        public String overloaded(
                @EffectorParam(name="first") String first,
                @EffectorParam(name="second") String second) throws Exception;
        
        @Effector(description="sample effector doing some waiting")
        public void waitabit() throws Exception;
        
        @Effector(description="sample effector that spawns a child task that waits a bit")
        void spawnchild() throws Exception;
        

        /** The "current task" representing the effector currently executing */
        AtomicReference<Task<?>> getWaitingTask();
        
        /** latch is .countDown'ed by the effector at the beginning of the "waiting" point */
        CountDownLatch getNowWaitingLatch();
        
        /** latch is await'ed on by the effector when it is in the "waiting" point */
        CountDownLatch getContinueFromWaitingLatch();
    }
        
    public static class MyEntityImpl extends AbstractEntity implements MyEntity {

        private final AtomicReference<Task<?>> waitingTask = new AtomicReference<Task<?>>();
        
        private final CountDownLatch nowWaitingLatch = new CountDownLatch(1);
        
        private final CountDownLatch continueFromWaitingLatch = new CountDownLatch(1);
        
        @Override
        public String concatenate(String first, String second) throws Exception {
            return first+second;
        }
        
        @Override
        public void waitabit() throws Exception {
            waitingTask.set(Tasks.current());
            
            Tasks.setExtraStatusDetails("waitabit extra status details");
            
            Tasks.withBlockingDetails("waitabit.blocking", new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        nowWaitingLatch.countDown();
                        if (!continueFromWaitingLatch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                            fail("took too long to be told to continue");
                        }
                        return null;
                    }});
        }
        
        @Override
        public void spawnchild() throws Exception {
            // spawn a child, then wait
            BasicExecutionContext.getCurrentExecutionContext().submit(
                    MutableMap.of("displayName", "SpawnedChildName"),
                    new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            log.info("beginning spawned child response "+Tasks.current()+", with tags "+Tasks.current().getTags());
                            Tasks.setBlockingDetails("spawned child blocking details");
                            nowWaitingLatch.countDown();
                            if (!continueFromWaitingLatch.await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                                fail("took too long to be told to continue");
                            }
                            return null;
                        }});
        }
        
        @Override
        public AtomicReference<Task<?>> getWaitingTask() {
            return waitingTask;
        }
        
        @Override
        public CountDownLatch getNowWaitingLatch() {
            return nowWaitingLatch;
        }
        
        @Override
        public CountDownLatch getContinueFromWaitingLatch() {
            return continueFromWaitingLatch;
        }

        @Override
        public String effectorWithDefaults(String first, String second) throws Exception {
            return "effectorWithDefaults(String first="+first+", String second="+second+")";
        }

        @Override
        public String effectorWithTypedArgs(boolean booleanArg, byte byteArg, short shortArg, int intArg,
                long longArg, float floatArg, double doubleArg) throws Exception {
            return "effectorWithTypedArgs(boolean booleanArg="+booleanArg+", byte byteArg="+byteArg+", "
                    + "short shortArg="+shortArg+", int intArg="+intArg+", long longArg="+longArg+", "
                    + "float floatArg="+floatArg+", double doubleArg="+doubleArg+")";
        }

        @Override
        public String effectorWithObjectArgs(Object objectArg) throws Exception {
            return "effectorWithObjectArgs(Object objectArg="+objectArg+")";
        }

        @Override
        public String overloaded() throws Exception {
            return "overloaded()";
        }

        @Override
        public String overloaded(String first) throws Exception {
            return "overloaded(String first="+first+")";
        }

        @Override
        public String overloaded(Integer first) throws Exception {
            return "overloaded(Integer first="+first+")";
        }

        @Override
        public String overloaded(String first, String second) throws Exception {
            return "overloaded(String first="+first+", String second="+second+")";
        }
    }
            
    private MyEntity entity;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        entity = app.addChild(EntitySpec.create(MyEntity.class));
    }
    
    @Test
    public void testCanInvokeEffector() throws Exception {
        // invocation map syntax
        Task<String> task = entity.invoke(MyEntity.CONCATENATE, ImmutableMap.of("first", "a", "second", "b"));
        assertEquals(task.get(TIMEOUT, TimeUnit.MILLISECONDS), "ab");

        // method syntax
        assertEquals("xy", entity.concatenate("x", "y"));
    }
    
    @Test
    public void testDefaultArgs() throws Exception {
        String result = entity.invoke(MyEntity.EFFECTOR_WITH_DEFAULTS, ImmutableMap.<String, Object>of()).get();
        assertEquals(result, "effectorWithDefaults(String first=firstDefault, String second=secondDefault)");
        
        String result2 = entity.invoke(MyEntity.EFFECTOR_WITH_DEFAULTS, ImmutableMap.of("first", "myfirst")).get();
        assertEquals(result2, "effectorWithDefaults(String first=myfirst, String second=secondDefault)");
    }
    
    @Test
    public void testTypedArgs() throws Exception {
        String result1 = entity.invoke(MyEntity.EFFECTOR_WITH_TYPED_ARGS, 
                ImmutableMap.<String, Object>builder()
                        .put("booleanArg", true)
                        .put("byteArg", 1)
                        .put("shortArg", 2)
                        .put("intArg", 3)
                        .put("longArg", 4)
                        .put("floatArg", 5)
                        .put("doubleArg", 6)
                        .build())
                .get();
        assertEquals(result1, "effectorWithTypedArgs(boolean booleanArg=true, byte byteArg=1, short shortArg=2, "
                + "int intArg=3, long longArg=4, float floatArg=5.0, double doubleArg=6.0)");
        
        // method syntax
        String result1b = entity.effectorWithTypedArgs(true, (byte)1, (short)2, 3, 4L, 5F, 6D);
        assertEquals(result1, result1b);
    }
    
    @Test
    public void testObjectArgs() throws Exception {
        String result1 = entity.invoke(MyEntity.EFFECTOR_WITH_OBJECT_ARGS, ImmutableMap.of("objectArg", "myval")).get();
        assertEquals(result1, "effectorWithObjectArgs(Object objectArg=myval)");
        
        String result2 = entity.invoke(MyEntity.EFFECTOR_WITH_OBJECT_ARGS, ImmutableMap.of("objectArg", 1)).get();
        assertEquals(result2, "effectorWithObjectArgs(Object objectArg=1)");
        
        // method syntax
        String result1b = entity.effectorWithObjectArgs("myval");
        assertEquals(result1b, result1);
        
        String result2b = entity.effectorWithObjectArgs(1);
        assertEquals(result2b, result2);
    }
    
    // Always calls `overloaded()` - we don't support method overloading for effectors
    @Test(enabled=false, groups="Broken")
    public void testOverloaded() throws Exception {
        String result1 = entity.invoke(MyEntity.OVERLOADED, ImmutableMap.<String, Object>of()).get();
        assertEquals(result1, "overloaded()");
        
        String result2 = entity.invoke(MyEntity.OVERLOADED, ImmutableMap.of("first", "myfirst")).get();
        assertEquals(result2, "overloaded(String first=myfirst)");
        
        String result3 = entity.invoke(MyEntity.OVERLOADED, ImmutableMap.of("first", "myfirst", "second", "mysecond")).get();
        assertEquals(result3, "overloaded(String first=myfirst, String second=mysecond)");
        
        String result4 = entity.invoke(MyEntity.OVERLOADED, ImmutableMap.of("first", 1)).get();
        assertEquals(result4, "overloaded(Integer first=1)");
        
        // method syntax
        String result1b = entity.overloaded();
        assertEquals(result1b, result1);
        
        String result2b = entity.overloaded("myfirst");
        assertEquals(result2b, result2);
        
        String result3b = entity.overloaded("myfirst", "mysecond");
        assertEquals(result3b, result3);
        
        String result4b = entity.overloaded(1);
        assertEquals(result4b, result4);
    }
    
    @Test
    public void testReportsTaskDetails() throws Exception {
        final AtomicReference<String> result = new AtomicReference<String>();

        Thread bg = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // Expect "wait a bit" to tell us it's blocking 
                    if (!entity.getNowWaitingLatch().await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                        result.set("took too long for waitabit to be waiting");
                        return;
                    }

                    // Expect "wait a bit" to have retrieved and set its task
                    try {
                        Task<?> t = entity.getWaitingTask().get();
                        String status = t.getStatusDetail(true);
                        log.info("waitabit task says:\n"+status);
                        if (!status.contains("waitabit extra status details")) {
                            result.set("Status not in expected format: doesn't contain extra status details phrase 'My extra status details'\n"+status);
                            return;
                        }
                        if (!status.startsWith("waitabit.blocking")) {
                            result.set("Status not in expected format: doesn't start with blocking details 'waitabit.blocking'\n"+status);
                            return;
                        }
                    } finally {
                        entity.getContinueFromWaitingLatch().countDown();
                    }
                } catch (Throwable t) {
                    log.warn("Failure: "+t, t);
                    result.set("Failure: "+t);
                }
            }});
        bg.start();
    
        entity.invoke(MyEntity.WAIT_A_BIT, ImmutableMap.<String,Object>of())
                .get(TIMEOUT, TimeUnit.MILLISECONDS);
        
        bg.join(TIMEOUT*2);
        assertFalse(bg.isAlive());
        
        String problem = result.get();
        if (problem!=null) fail(problem);
    }
    
    @Test
    public void testReportsSpawnedTaskDetails() throws Exception {
        final AtomicReference<String> result = new AtomicReference<String>();

        Thread bg = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // Expect "spawned child" to tell us it's blocking 
                    if (!entity.getNowWaitingLatch().await(TIMEOUT, TimeUnit.MILLISECONDS)) {
                        result.set("took too long for spawnchild's sub-task to be waiting");
                        return;
                    }

                    // Expect spawned task to be have been tagged with entity
                    ExecutionManager em = entity.getManagementContext().getExecutionManager();
                    Task<?> subtask = Iterables.find(BrooklynTaskTags.getTasksInEntityContext(em, entity), new Predicate<Task<?>>() {
                        @Override
                        public boolean apply(Task<?> input) {
                            return "SpawnedChildName".equals(input.getDisplayName());
                        }
                    });
                    
                    // Expect spawned task to have correct "blocking details"
                    try {
                        String status = subtask.getStatusDetail(true);
                        log.info("subtask task says:\n"+status);
                        if (!status.contains("spawned child blocking details")) {
                            result.set("Status not in expected format: doesn't contain blocking details phrase 'spawned child blocking details'\n"+status);
                            return;
                        }
                    } finally {
                        entity.getContinueFromWaitingLatch().countDown();
                    }
                } catch (Throwable t) {
                    log.warn("Failure: "+t, t);
                    result.set("Failure: "+t);
                }
            }});
        bg.start();
    
        entity.invoke(MyEntity.SPAWN_CHILD, ImmutableMap.<String,Object>of())
                .get(TIMEOUT, TimeUnit.MILLISECONDS);
        
        bg.join(TIMEOUT*2);
        assertFalse(bg.isAlive());
        
        String problem = result.get();
        if (problem!=null) fail(problem);
    }
}
