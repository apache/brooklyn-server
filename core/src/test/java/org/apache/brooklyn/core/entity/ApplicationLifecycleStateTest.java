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

import java.util.Collection;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.FailingEntity;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Test
public class ApplicationLifecycleStateTest extends BrooklynMgmtUnitTestSupport {

    public void testHappyPathEmptyApp() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);
    }
    
    public void testHappyPathWithChild() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class)));
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);
    }
    
    public void testOnlyChildFailsToStartCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        FailingEntity child = (FailingEntity) Iterables.get(app.getChildren(), 0);
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(child, Lifecycle.ON_FIRE, false);
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }
    
    public void testSomeChildFailsOnStartCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }
    
    public void testOnlyChildFailsToStartThenRecoversCausesAppToRecover() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        FailingEntity child = (FailingEntity) Iterables.get(app.getChildren(), 0);
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
        
        child.sensors().set(Attributes.SERVICE_UP, true);
        child.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertUpAndRunningEventually(app);
    }
    
    public void testSomeChildFailsToStartThenRecoversCausesAppToRecover() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        FailingEntity child = (FailingEntity) Iterables.find(app.getChildren(), Predicates.instanceOf(FailingEntity.class));
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
        
        child.sensors().set(Attributes.SERVICE_UP, true);
        child.sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertUpAndRunningEventually(app);
    }
    
    public void testStartsThenOnlyChildFailsCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class)));
        TestEntity child = (TestEntity) Iterables.get(app.getChildren(), 0);
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);

        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(child, "myIndicator", "Simulate not-up of child");
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }

    public void testStartsThenSomeChildFailsCausesAppToFail() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(TestEntity.class)));
        TestEntity child = (TestEntity) Iterables.get(app.getChildren(), 0);
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);

        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(child, "myIndicator", "Simulate not-up of child");
        assertHealthEventually(app, Lifecycle.ON_FIRE, false);
    }

    public void testChildFailuresOnStartButWithQuorumCausesAppToSucceed() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .configure(StartableApplication.UP_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .configure(StartableApplication.RUNNING_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        
        startAndAssertException(app, ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);
    }
    
    public void testStartsThenChildFailsButWithQuorumCausesAppToSucceed() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .configure(StartableApplication.UP_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .configure(StartableApplication.RUNNING_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(TestEntity.class)));
        TestEntity child = (TestEntity) Iterables.get(app.getChildren(), 0);
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);

        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(child, "myIndicator", "Simulate not-up of child");
        assertHealthContinually(app, Lifecycle.RUNNING, true);
    }

    public void testStartsThenChildFailsButWithQuorumCausesAppToStayHealthy() throws Exception {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .configure(StartableApplication.UP_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .configure(StartableApplication.RUNNING_QUORUM_CHECK, QuorumCheck.QuorumChecks.atLeastOne())
                .child(EntitySpec.create(TestEntity.class))
                .child(EntitySpec.create(TestEntity.class)));
        TestEntity child = (TestEntity) Iterables.get(app.getChildren(), 0);
        
        app.start(ImmutableList.<Location>of());
        assertUpAndRunningEventually(app);

        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(child, "myIndicator", "Simulate not-up of child");
        assertUpAndRunningEventually(app);
    }

    private void assertHealthEventually(Entity entity, Lifecycle expectedState, Boolean expectedUp) {
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, expectedState);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, expectedUp);
    }
    
    private void assertHealthContinually(Entity entity, Lifecycle expectedState, Boolean expectedUp) {
        // short wait, so unit tests don't take ages
        Map<String, ?> flags = ImmutableMap.of("timeout", ValueResolver.REAL_QUICK_WAIT);
        EntityAsserts.assertAttributeEqualsContinually(flags, entity, Attributes.SERVICE_STATE_ACTUAL, expectedState);
        EntityAsserts.assertAttributeEqualsContinually(flags, entity, Attributes.SERVICE_UP, expectedUp);
    }
    
    private void assertUpAndRunningEventually(Entity entity) {
        try {
            EntityAsserts.assertAttributeEventually(entity, Attributes.SERVICE_NOT_UP_INDICATORS, CollectionFunctionals.<String>mapEmptyOrNull());
            EntityAsserts.assertAttributeEventually(entity, ServiceStateLogic.SERVICE_PROBLEMS, CollectionFunctionals.<String>mapEmptyOrNull());
            EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
            EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
        } catch (Throwable t) {
            Entities.dumpInfo(entity);
            String err = "(Dumped entity info - see log); entity=" + entity + "; " + 
                    "state=" + entity.sensors().get(Attributes.SERVICE_STATE_ACTUAL) + "; " + 
                    "up="+entity.sensors().get(Attributes.SERVICE_UP) + "; " +
                    "notUpIndicators="+entity.sensors().get(Attributes.SERVICE_NOT_UP_INDICATORS) + "; " +
                    "serviceProblems="+entity.sensors().get(Attributes.SERVICE_PROBLEMS);
            throw new AssertionError(err, t);
        }
    }
    
    private void startAndAssertException(TestApplication app, Collection<? extends Location> locs) {
        try {
            app.start(locs);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Error invoking start");
        }
    }
}
