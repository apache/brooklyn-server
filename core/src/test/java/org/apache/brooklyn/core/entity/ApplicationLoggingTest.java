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
package org.apache.brooklyn.core.entity;

import static org.apache.brooklyn.test.LogWatcher.EventPredicates.containsMessage;
import static org.apache.brooklyn.test.LogWatcher.EventPredicates.matchingRegexes;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.entity.trait.StartableMethods;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestApplicationImpl;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import ch.qos.logback.classic.Level;

@Test
public class ApplicationLoggingTest extends BrooklynAppUnitTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationLoggingTest.class);

    @ImplementedBy(TestApplicationWithLoggingImpl.class)
    public interface TestApplicationWithLogging extends TestApplication {

    }

    public static class TestApplicationWithLoggingImpl extends TestApplicationImpl
        implements TestApplicationWithLogging {

        @Override
        protected void initEnrichers() {
            super.initEnrichers();
            ServiceStateLogic.newEnricherFromChildrenUp()
                .requireUpChildren(QuorumCheck.QuorumChecks.all())
                .addTo(this);
        }

        @Override
        protected void doStart(Collection<? extends Location> locations) {
            super.doStart(locations);
            LOG.info("Hello world");
        }

        @Override
        protected void doStop() {
            LOG.info("Goodbye cruel world");
            super.doStop();
        }
    }

    @ImplementedBy(TestEntityWithLoggingImp.class)
    public interface TestEntityWithLogging extends TestEntity {
    }

    public static final class TestEntityWithLoggingImp extends TestEntityImpl implements TestEntityWithLogging {

        private String getIndent() {
            String indent = "";
            Entity e = this;
            while (e.getParent() != null) {
                indent += "  ";
                e = e.getParent();
            }
            return indent;
        }

        @Override
        protected void initEnrichers() {
            super.initEnrichers();
            ServiceStateLogic.newEnricherFromChildrenUp()
                .requireUpChildren(QuorumCheck.QuorumChecks.all())
                .addTo(this);
        }

        @Override
        public void start(Collection<? extends Location> locs) {
            super.start(locs);
            try {
                StartableMethods.start(this, locs);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw new RuntimeException(e);
            }
            LOG.info(getIndent() + "Hello from entity {}", getId());
        }

        @Override
        public void stop() {
            LOG.info(getIndent() + "Goodbye from entity {}", getId());
            StartableMethods.stop(this);
        }
    }

    @Override
    protected void setUpApp() {
        LOG.info("setUpApp");
        EntitySpec<TestApplicationWithLogging> appSpec = EntitySpec.create(TestApplicationWithLogging.class);
        if (shouldSkipOnBoxBaseDirResolution()!=null)
            appSpec.configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution());

        app = mgmt.getEntityManager().createEntity(appSpec);
    }

    @Test
    public void testLogging() throws Exception {
        String loggerName = ApplicationLoggingTest.class.getName();
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.INFO;

        Deque<String> ids = new ArrayDeque<>();
        ids.push(app.getId());
        final TestEntityWithLogging entity = app.createAndManageChild(EntitySpec.create(TestEntityWithLogging.class));
        final TestEntityWithLogging child = entity.addChild(EntitySpec.create(EntitySpec.create(TestEntityWithLogging.class)));
        LogWatcher watcher = new LogWatcher(loggerName, logLevel, containsMessage(app.getId()));

        watcher.start();
        try {
            app.start(ImmutableList.of(app.newSimulatedLocation()));
            assertHealthEventually(app, Lifecycle.RUNNING, true);
            final TaskAdaptable<Void> stopTask = Effectors.invocation(app, Startable.STOP, ImmutableMap.of());
            final String stopId = stopTask.asTask().getId();
            LOG.info("Stop task id is {}", stopId);
            final ExecutionContext executionContext = mgmt.getExecutionContext(app);
            executionContext.submit(stopTask);
            assertHealthEventually(app, Lifecycle.STOPPED, false);

            // Look for output like
//          2018-02-05 16:23:11,485 INFO  K09KEX1U-[y4lgil3hya,xn0fmqrhzd,khx0py82ba]     Hello from entity khx0py82ba
//          2018-02-05 16:23:11,488 INFO  kD8Q76x0-[y4lgil3hya,xn0fmqrhzd]   Hello from entity xn0fmqrhzd
//          2018-02-05 16:23:11,488 INFO  ZsU4OGEp-[y4lgil3hya] Hello world
//          2018-02-05 16:23:11,496 INFO  - Stop task id is pOp03ybS
//          2018-02-05 16:23:11,498 INFO  pOp03ybS-[y4lgil3hya] Goodbye cruel world
//          2018-02-05 16:23:11,499 INFO  c1Pcn3FR-[y4lgil3hya,xn0fmqrhzd]   Goodbye from entity xn0fmqrhzd
//          2018-02-05 16:23:11,500 INFO  eq9akWgK-[y4lgil3hya,xn0fmqrhzd,khx0py82ba]     Goodbye from entity khx0py82ba
            watcher.assertHasEvent(containsMessage(stopId + "-"));
            watcher.assertHasEvent(matchingRegexes(".*" + app.getApplicationId() + ".*Hello world.*"));;
            watcher.assertHasEvent(matchingRegexes(".*" +
                ImmutableList.of(app.getId(), entity.getId()).toString()
                + ".*from entity.*" + entity.getId() + ".*"));
            watcher.assertHasEvent(matchingRegexes(".*" +
                ImmutableList.of(app.getId(), entity.getId(), child.getId()).toString()
                + ".*from entity.*" + child.getId() + ".*"));
        } finally {
            watcher.close();
        }
    }

    private void assertHealthEventually(Entity entity, Lifecycle expectedState, Boolean expectedUp) {
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, expectedState);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, expectedUp);
    }

}
