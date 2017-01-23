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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.effector.ParameterType;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementClass;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementManager;
import org.apache.brooklyn.core.effector.EffectorTasks.EffectorTaskFactory;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.EntityAndItem;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.StringAndArgument;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.test.LogWatcher.EventPredicates;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;

import ch.qos.logback.classic.spi.ILoggingEvent;

public class EffectorExceptionLoggedTest extends BrooklynAppUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntity.class);

    private TestEntity entity;

    public static class ThrowingEntitlementManager implements EntitlementManager {
        @Override 
        public <T> boolean isEntitled(EntitlementContext context, EntitlementClass<T> entitlementClass, T entitlementClassArgument) {
            String type = entitlementClass.entitlementClassIdentifier();
            if (Entitlements.INVOKE_EFFECTOR.entitlementClassIdentifier().equals(type)) {
                @SuppressWarnings("unchecked")
                String effectorName = ((EntityAndItem<StringAndArgument>)entitlementClassArgument).getItem().getString();
                if ("myEffector".equals(effectorName)) {
                    LOG.info("Simulating NPE in entitlement manager");
                    throw new NullPointerException();
                }
            }
            return true;
        }
    }

    @Override
    protected BrooklynProperties getBrooklynProperties() {
        BrooklynProperties result = BrooklynProperties.Factory.newEmpty();
        result.put(Entitlements.GLOBAL_ENTITLEMENT_MANAGER, ThrowingEntitlementManager.class.getName());
        return result;
    }
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.addChild(EntitySpec.create(TestEntity.class));
    }
    
    @Test
    public void testInvokeEffectorDirectlyIncludesException() throws Exception {
        try {
            entity.myEffector();
        } catch (Exception e) {
            assertExceptionContainsIsEntitledStack(e);
        }
        
        try {
            Entities.invokeEffector(app, entity, TestEntity.MY_EFFECTOR).get();
        } catch (Exception e) {
            assertExceptionContainsIsEntitledStack(e);
        }
    }

    @Test
    public void testInvokeViaOtherEffectorIncludesException() throws Exception {
        EffectorTaskFactory<Void> body = new EffectorTaskFactory<Void>() {
            @Override public Task<Void> newTask(final Entity entity, final Effector<Void> effector, final ConfigBag parameters) {
                return Tasks.<Void>builder()
                    .body(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            EffectorExceptionLoggedTest.this.entity.myEffector();
                            return null;
                        }})
                    .build();
            }
        };
        EffectorAndBody<Void> effector = new EffectorAndBody<Void>("callingEffector", Void.class, ImmutableList.<ParameterType<?>>of(), "my description", body);

        try {
            Entities.invokeEffector(app, app, effector).get();
        } catch (Exception e) {
            assertExceptionContainsIsEntitledStack(e);
        }
    }
    
    /**
     * This task-invocation pattern matches that used in AutoScaler.
     * 
     * Confirm that we log the real stacktrace of the exception.
     * 
     * This requires adding a mock appender, asserting it gets called with our desired 
     * class+method name, and then removing the appender!
     */
    @Test
    public void testInvokeInTask() throws Exception {
        String loggerName = EffectorUtils.class.getName();
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.DEBUG;
        Predicate<ILoggingEvent> filter = Predicates.and(EventPredicates.containsMessage("Error invoking myEffector"), 
                EventPredicates.containsExceptionStackLine(ThrowingEntitlementManager.class, "isEntitled"));
        LogWatcher watcher = new LogWatcher(loggerName, logLevel, filter);

        watcher.start();
        try {
            Entities.submit(entity, Tasks.<Void>builder().displayName("Effector-invoker")
                    .description("Invoke in task")
                    .tag(BrooklynTaskTags.NON_TRANSIENT_TASK_TAG)
                    .body(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            entity.myEffector();
                            return null;
                        }
                    }).build())
                    .blockUntilEnded();

            watcher.assertHasEventEventually();
        } finally {
            watcher.close();
        }
    }
    
    private void assertExceptionContainsIsEntitledStack(Exception e) throws Exception {
        String expected = ThrowingEntitlementManager.class.getSimpleName()+".isEntitled";
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(baos);
        e.printStackTrace(writer);
        writer.flush();
        String eString = new String(baos.toByteArray());
        if (!eString.contains(expected)) {
            throw new Exception("Original exception does not contain '"+expected+"'", e);
        }
    }
}
