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
package org.apache.brooklyn.camp.brooklyn;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.policy.TestPolicy;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicApplicationImpl;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class WorkflowApplicationModelTest extends AbstractYamlTest {

    protected void loadTypes() {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt());
    }

    BasicApplication lastApp;
    Object runStep(Object step, Consumer<BasicApplication> appFunction) {
        return runSteps(MutableList.<Object>of(step), appFunction);
    }
    Object runSteps(List<Object> steps, Consumer<BasicApplication> appFunction) {
        return runSteps(steps, appFunction, null);
    }
    Object runSteps(List<Object> steps, Consumer<BasicApplication> appFunction, ConfigBag defaultConfig) {
        loadTypes();
        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.lastApp = app;
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, steps)
                .putAll(defaultConfig)
        );
        if (appFunction!=null) appFunction.accept(app);
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        return invocation.getUnchecked();
    }

    @Test
    public void testDeployApp() {
        Object result = runSteps(MutableList.of(
                MutableMap.of(
                        "step", "deploy-application",
                        "blueprint", MutableMap.of(
                                "name", "Deploy App Test",
                                "services", MutableList.of(
                                    MutableMap.of("type", BasicEntity.class.getName(), "name", "Test")
                        )))
        ), null);

        Entity appMade = mgmt().getEntityManager().getEntities().stream().filter(e -> "Deploy App Test".equals(e.getDisplayName())).findAny().get();
        Asserts.assertEquals( ((Map)result).get("app"), appMade);
    }

    @Test
    public void testAddEntity() {
        Object result = runSteps(MutableList.of(
                MutableMap.of(
                        "step", "add-entity",
                        "blueprint", MutableMap.of(
                                "name", "Add Entity Test",
                                "services", MutableList.of(
                                        MutableMap.of("type", BasicEntity.class.getName(), "name", "Test")
                                )))
        ), null);

        Entity childMade = Iterables.getOnlyElement(lastApp.getChildren());
        Asserts.assertEquals( ((Map)result).get("entity"), childMade);
        Asserts.assertEquals( ((Map)result).get("entities"), MutableList.of(childMade));
    }

    @Test
    public void testDeleteEntity() {
        Object result = runSteps(MutableList.of(
                MutableMap.of(
                        "step", "add-entity",
                        "blueprint", MutableMap.of("type", BasicEntity.class.getName())),
                "delete-entity ${entity}"

        ), null);

        Asserts.assertSize(lastApp.getChildren(), 0);
        Asserts.assertThat( (Entity) ((Map)result).get("entity"), Entities::isUnmanagingOrNoLongerManaged );

        // and using an id

        result = runSteps(MutableList.of(
                MutableMap.of(
                        "step", "add-entity",
                        "blueprint", MutableMap.of("type", BasicEntity.class.getName(), "id", "child_to_kill")),
                "delete-entity child_to_kill"

        ), null);

        Asserts.assertSize(lastApp.getChildren(), 0);
        Asserts.assertThat( (Entity) ((Map)result).get("entity"), Entities::isUnmanagingOrNoLongerManaged );

        result = runSteps(MutableList.of(
                "delete-entity $brooklyn:self()"
        ), null);
        Asserts.assertFalse(Entities.isManagedActive(lastApp));
    }

    @Test
    public void testReparentEntity() {
        Object result = runSteps(MutableList.of(
                MutableMap.of(
                        "step", "add-entity",
                        "blueprint", MutableList.of(
                                MutableMap.of("type", BasicEntity.class.getName(), "name", "one"),
                                MutableMap.of("type", BasicEntity.class.getName(), "name", "too"))),
                "reparent-entity child ${entity.children[1]} under ${entity.children[0]}"

        ), null);

        Entity child = Iterables.getOnlyElement(lastApp.getChildren());
        Entity grandchild = Iterables.getOnlyElement(child.getChildren());
        Asserts.assertEquals(grandchild.getDisplayName(), "too");
    }

    final static AtomicBoolean GATE = new AtomicBoolean();

    public static class BlockingStartApp extends BasicApplicationImpl {
        @Override
        public void start(Collection<? extends Location> locations) {
            super.start(locations);
            synchronized (GATE) {
                try {
                    System.out.println("Blocking: "+ Tasks.current()+" / "+Thread.currentThread()+" - "+Thread.currentThread().isInterrupted());
                    while (!GATE.get()) GATE.wait(200);
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            }
        }
    }

    @Test(groups="Integration")
    public void testDeployAppIdempotent() {
        loadTypes();
        GATE.set(false);

        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowExecutionContext w1 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "steps:",
                " - step: deploy-application",
                "   start: sync",
                "   blueprint:",
                "     name: Deploy Idempotent",
                "     services:",
                "     - type: " + BlockingStartApp.class.getName()
        ), null);
        Task<Object> wt = w1.getTask(false).get();
        Asserts.assertFalse(wt.blockUntilEnded(Duration.millis(500)));
        wt.cancel(true);
        Asserts.assertTrue(wt.blockUntilEnded(Duration.millis(2000), true));

        Asserts.assertEquals(w1.getStatus(), WorkflowExecutionContext.WorkflowStatus.ERROR_CANCELLED);

        Task<Object> resume = Entities.submit(app, w1.factory(false).createTaskReplaying(w1.factory(false).makeInstructionsForReplayResuming("test", false)));
        Asserts.assertTrue(resume.blockUntilEnded(Duration.millis(500))); // doesn't wait on gate because start method not called again

        Asserts.assertEquals(w1.getStatus(), WorkflowExecutionContext.WorkflowStatus.SUCCESS);

        List<Entity> apps = mgmt().getEntityManager().getEntities().stream().filter(e -> "Deploy Idempotent".equals(e.getDisplayName())).collect(Collectors.toList());
        Asserts.assertSize(apps, 1);
        Asserts.assertEquals(apps.iterator().next(), ((Map)resume.getUnchecked()).get("app"));
    }

    @Test(groups="Integration")
    public void testAddEntitySeveralIdempotent() {
        loadTypes();
        GATE.set(false);

        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowExecutionContext w1 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "steps:",
                " - step: add-entity "+BlockingStartApp.class.getName(),
                "   start: sync"
        ), null);

        Task<Object> wt = w1.getTask(false).get();
        Asserts.assertFalse(wt.blockUntilEnded(Duration.millis(500)));
        wt.cancel(true);
        Asserts.assertTrue(wt.blockUntilEnded(Duration.millis(2000), true));

        Asserts.assertEquals(w1.getStatus(), WorkflowExecutionContext.WorkflowStatus.ERROR_CANCELLED);

        Task<Object> resume = Entities.submit(app, w1.factory(false).createTaskReplaying(w1.factory(false).makeInstructionsForReplayResuming("test", false)));
        Asserts.assertTrue(resume.blockUntilEnded(Duration.millis(1000), true)); // doesn't wait on gate because start method not called again

        Asserts.assertEquals(w1.getStatus(), WorkflowExecutionContext.WorkflowStatus.SUCCESS);

        Object result = resume.getUnchecked();

        Entity childMade = Iterables.getOnlyElement(app.getChildren());
        Asserts.assertEquals( ((Map)result).get("entity"), childMade);
        Asserts.assertEquals( ((Map)result).get("entities"), MutableList.of(childMade));
    }

    @Test
    public void testAddAndDeletePolicyAndOtherAdjuncts() {
        loadTypes();
        doTestAddAndDeletePolicyAndOtherAdjuncts(true, true);
        doTestAddAndDeletePolicyAndOtherAdjuncts(true, false);
        doTestAddAndDeletePolicyAndOtherAdjuncts(false, true);
        doTestAddAndDeletePolicyAndOtherAdjuncts(false, false);
    }

    void doTestAddAndDeletePolicyAndOtherAdjuncts(boolean shorthand, boolean explicitUniqueTag) {
        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        Object step1 = shorthand ? "add-policy " + TestPolicy.class.getName() + (explicitUniqueTag ? " unique-tag my-policy" : "")
                : "\n"+Strings.indent(2, Strings.lines(
                        "type: add-policy",
                        "blueprint:",
                        "  type: "+TestPolicy.class.getName(),
                        "  "+TestPolicy.CONF_NAME.getName()+": my-policy-name",
                        explicitUniqueTag ? "  uniqueTag: my-policy" : ""));
        WorkflowExecutionContext w1 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "steps:",
                "- "+step1,
                "- return ${policy.uniqueTag}"
        ), null);
        Object t1 = w1.getTask(false).get().getUnchecked();

        Asserts.assertSize(app.policies().asList(), 1);

        Task<Object> replay = Entities.submit(app, w1.factory(false).createTaskReplaying(w1.factory(false)
                .makeInstructionsForReplayingFromStep(0, "check idempotency", true)));
        Object t2 = replay.getUnchecked();

        // should get same unique id?
        Asserts.assertSize(app.policies().asList(), 1);
        if (!shorthand) {
            String pn = Iterables.getOnlyElement(app.policies()).config().get(TestPolicy.CONF_NAME);
            Asserts.assertEquals(pn, "my-policy-name");
        }

        String ut = Iterables.getOnlyElement(app.policies()).getUniqueTag();
        if (explicitUniqueTag) Asserts.assertEquals(ut, "my-policy");
        else Asserts.assertEquals(ut, w1.getWorkflowId()+"-1");
        Asserts.assertEquals(t1, ut);
        Asserts.assertEquals(t2, ut);

        WorkflowExecutionContext w2 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "steps:",
                " - step: delete-policy "+ut
        ), null);
        w2.getTask(false).get().getUnchecked();

        Asserts.assertSize(app.policies().asList(), 0);
    }

    @Test
    public void testApplyInitializer() {
        loadTypes();
        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowExecutionContext w1 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "steps:",
                " - step: apply-initializer",
                "   blueprint:",
                "     type: workflow-effector",
                "     name: say-hi",
                "     steps:",
                "       - return hi"
        ), null);
        w1.getTask(false).get().getUnchecked();

        Maybe<Effector<?>> eff = app.getEntityType().getEffectorByName("say-hi");
        Asserts.assertPresent(eff);
        Asserts.assertEquals(app.invoke(eff.get(), null).getUnchecked(), "hi");
    }

}
