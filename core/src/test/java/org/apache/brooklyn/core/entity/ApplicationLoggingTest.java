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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
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
import static org.apache.brooklyn.test.LogWatcher.EventPredicates.containsMessage;
import static org.apache.brooklyn.test.LogWatcher.EventPredicates.matchingRegexes;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

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

    @ImplementedBy(TestEntityWithLoggingImpl.class)
    public interface TestEntityWithLogging extends TestEntity {
    }

    public static final class TestEntityWithLoggingImpl extends TestEntityImpl implements TestEntityWithLogging {

        private String getIndent() {
            // no need for indent is there?
            String indent = "";
//            Entity e = this;
//            while (e.getParent() != null) {
//                indent += "  ";
//                e = e.getParent();
//            }
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
            DynamicTasks.queue(Tasks.parallel("Queued parallel in start", Tasks.builder().body(()->LOG.info("task-qp1")).displayName("Task QP1").build()));
            DynamicTasks.submit(Tasks.builder().body(()->LOG.info("task-s1")).displayName("Simple task").build(), this).blockUntilEnded();
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

        try (LogWatcher watcher = new LogWatcher(loggerName, logLevel, containsMessage(app.getId()))) {
            app.start(ImmutableList.of(app.newSimulatedLocation()));
            assertHealthEventually(app, Lifecycle.RUNNING, true);
            final TaskAdaptable<Void> stopTask = Effectors.invocation(app, Startable.STOP, ImmutableMap.of());
            final String stopId = stopTask.asTask().getId();
            LOG.info("Stop task id is {}", stopId);
            final ExecutionContext executionContext = mgmt.getExecutionContext(app);
            executionContext.submit(stopTask);
            assertHealthEventually(app, Lifecycle.STOPPED, false);

            watcher.dumpLog();

            // Was like this
//            2021-08-25 01:05:57,300 INFO  NRVV5rRl-[xm8hb6sl61,v030wunwkf,nlj2rrkoun] Hello from entity nlj2rrkoun
//            2021-08-25 01:05:57,303 INFO  - task-qp1
//            2021-08-25 01:05:57,304 INFO  - task-s1
//            2021-08-25 01:05:57,305 INFO  zTpsUVf8-[xm8hb6sl61,v030wunwkf] Hello from entity v030wunwkf
//            2021-08-25 01:05:57,305 INFO  - task-qp1
//            2021-08-25 01:05:57,305 INFO  - task-s1
//            2021-08-25 01:05:57,306 INFO  wv5xPCQx-[xm8hb6sl61] Hello world
//            2021-08-25 01:05:57,315 INFO  - Stop task id is Tads2TKn
//            2021-08-25 01:05:57,316 INFO  Tads2TKn-[xm8hb6sl61] Goodbye cruel world
//            2021-08-25 01:05:57,317 INFO  s4kkzRju-[xm8hb6sl61,v030wunwkf] Goodbye from entity v030wunwkf
//            2021-08-25 01:05:57,318 INFO  OirHlnKp-[xm8hb6sl61,v030wunwkf,nlj2rrkoun] Goodbye from entity nlj2rrkoun
            // Now like this
//            2021-08-25 03:25:09,225 AUxQncSH-[jsgii01x6z,k482sbdld8] INFO  o.a.b.c.e.ApplicationLoggingTest                  Hello from entity k482sbdld8
//            2021-08-25 03:25:09,228 GOxbPmDI-[jsgii01x6z,k482sbdld8] INFO  o.a.b.c.e.ApplicationLoggingTest                  task-qp1
//            2021-08-25 03:25:09,229 IsZ6JACj-[jsgii01x6z,k482sbdld8] INFO  o.a.b.c.e.ApplicationLoggingTest                  task-s1
//            2021-08-25 03:25:09,230 UtPwBp4Y-[jsgii01x6z,ry2o7ppr19] INFO  o.a.b.c.e.ApplicationLoggingTest                  Hello from entity ry2o7ppr19
//            2021-08-25 03:25:09,230 UvIkYT5x-[jsgii01x6z,ry2o7ppr19] INFO  o.a.b.c.e.ApplicationLoggingTest                  task-qp1
//            2021-08-25 03:25:09,231 asSZuFLg-[jsgii01x6z,ry2o7ppr19] INFO  o.a.b.c.e.ApplicationLoggingTest                  task-s1
//            2021-08-25 03:25:09,231 yeMZ29he-[jsgii01x6z,jsgii01x6z] INFO  o.a.b.c.e.ApplicationLoggingTest                  Hello world
//            2021-08-25 03:25:09,240         -                        INFO  o.a.b.c.e.ApplicationLoggingTest                  Stop task id is J5gxh53d
//            2021-08-25 03:25:09,242 J5gxh53d-[jsgii01x6z,jsgii01x6z] INFO  o.a.b.c.e.ApplicationLoggingTest                  Goodbye cruel world
//            2021-08-25 03:25:09,243 c1x0JDL5-[jsgii01x6z,ry2o7ppr19] INFO  o.a.b.c.e.ApplicationLoggingTest                  Goodbye from entity ry2o7ppr19
//            2021-08-25 03:25:09,244 McOOTM2B-[jsgii01x6z,k482sbdld8] INFO  o.a.b.c.e.ApplicationLoggingTest                  Goodbye from entity k482sbdld8

            watcher.assertHasEvent(containsMessage(stopId + "-"));
            watcher.assertHasEvent(matchingRegexes(".*" + app.getApplicationId() + ".*Hello world.*"));;
            watcher.assertHasEvent(matchingRegexes(".*" +
                    Strings.join(ImmutableList.of(app.getId(), entity.getId()), ",")+"\\]"
                    + ".*from entity.*" + entity.getId() + ".*"));
            watcher.assertHasEvent(matchingRegexes(".*" +
                    Strings.join(ImmutableList.of(app.getId(), child.getId()), ",")
                    + ".*from entity.*" + child.getId() + ".*"));

            // these fail
            watcher.assertHasEvent(matchingRegexes(".*" +
                    Strings.join(ImmutableList.of(app.getId(), entity.getId()), ",")+"\\]"
                    + ".*task-qp1.*"));
            watcher.assertHasEvent(matchingRegexes(".*" +
                    Strings.join(ImmutableList.of(app.getId(), entity.getId()), ",")+"\\]"
                    + ".*task-s1.*"));

        }
    }

    private void assertHealthEventually(Entity entity, Lifecycle expectedState, Boolean expectedUp) {
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, expectedState);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, expectedUp);
    }

}
