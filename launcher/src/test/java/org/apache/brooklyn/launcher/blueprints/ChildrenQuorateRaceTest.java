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
package org.apache.brooklyn.launcher.blueprints;

import com.google.common.collect.ImmutableSet;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.entity.stock.WorkflowStartable;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers.UP_QUORUM_CHECK;

/** This does rebind. See SimpleBlueprintNonRebindTest for an example with rebind disabled. */
public class ChildrenQuorateRaceTest extends AbstractBlueprintTest {

    private static final Logger log = LoggerFactory.getLogger(ChildrenQuorateRaceTest.class);

    @Override
    protected ManagementContext decorateManagementContext(ManagementContext mgmt) {
        mgmt = super.decorateManagementContext(mgmt);
        // make workflow step types available
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        return mgmt;
    }

    @Override
    protected boolean isViewerEnabled() {
        return true;
    }

    @Override
    protected boolean isUsingNewViewerForRebind() {
        return true;
    }

    public Duration randomNormalishJitter(Duration base, Duration jitterMax) {
        boolean increase = base.isLongerThan(Duration.ZERO) ? Math.random() > 0.5 : true;
        if (!increase && jitterMax.isLongerThan(base)) jitterMax = base;
        Duration jitterActual = jitterMax.multiply(Math.random() * Math.random());
        if (!increase) jitterActual = jitterActual.multiply(-1);
        jitterActual = Duration.millis(jitterActual.toMilliseconds());
        return base.add(jitterActual);
    }
    public Duration randomNormalishJitter(String base, String jitterMax) {
        return randomNormalishJitter(Duration.of(base), Duration.of(jitterMax));
    }

    public Duration taskStartStopDelay() {
        return randomNormalishJitter("100ms", "1s");
    }
    public Duration taskWorkflowDelay() {
        return Duration.of("10s").add(randomNormalishJitter("0", "5s"));
    }

    @Test(groups="Integration") // because slow
    public void testRace() throws Exception {
        BasicApplication app = EntityManagementUtils.createUnstarted(mgmt, EntitySpec.create(BasicApplication.class));
        int N=5;
        // at N=5 we see: Recompute determined BasicStartableImpl{id=k7meoqgkqg} is up, after 17s 391ms
        for (int i=0; i<N; i++) app.addChild(descendantSpec(N, N));

//        Dumper.dumpInfo(app);

        decorateExecutionWithChaosMonkeySleepAndLog((BasicExecutionManager) mgmt.getExecutionManager(),
                () -> onTaskStart(this::taskStartStopDelay), () -> onTaskEnd(this::taskStartStopDelay));

        recurse(app, Entity::getChildren, this::initStrict);
        app.start(null);

        // stick a breakpoint on the following line (make sure it is thread-only, not all-threads!)
        // then connect a UI eg brooklyn-ui/app-inspector `make dev` to the API endpoint used
        decorateExecutionWithChaosMonkeySleepAndLog((BasicExecutionManager) mgmt.getExecutionManager(), null, null);
        EntityAsserts.assertAttributeEquals(app, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
    }

    private <T> void recurse(T root, Function<T,? extends Iterable<T>> next, Consumer<T> action) {
        action.accept(root);
        next.apply(root).forEach(child -> recurse(child, next, action));
    }

    private void initStrict(Entity target) {
        if (target.getChildren().isEmpty()) return;

        // some things set this, but not in this test
//        ServiceStateLogic.ServiceNotUpLogic.updateNotUpIndicator(target, Attributes.SERVICE_STATE_ACTUAL,
//                "created but not yet started, at "+Time.makeDateString());

        ServiceStateLogic.newEnricherFromChildren().checkChildrenAndMembers()
                .uniqueTag("children-service-up-contraindicators")
                .requireUpChildren(QuorumCheck.QuorumChecks.allAndAtLeastOne())
                .configure(ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers.DERIVE_SERVICE_PROBLEMS, false)
                .configure(ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers.IGNORE_ENTITIES_WITH_THESE_SERVICE_STATES,
                        // override _not_ to ignore "starting" children
                        // (only applies for collections, which shouldn't say up unless starting children are up;
                        // other nodes default to an "alwaysHealthy" quorum so won't be affected,
                        // unless they have a custom up-ness quorum-check, in which case their upness will also be a function of starting children;
                        // means eg restarting a child of a collection will make the collection say service down, which is wanted for it;
                        // ideally this might be configurable for other nodes or depending on state of collection,
                        // or we can tie in with state derivation ... but that can be an enhancement for another day)
                        ImmutableSet.of(
                                //Lifecycle.STARTING, Lifecycle.STARTED,
                                //Lifecycle.STOPPING, Lifecycle.STOPPED,
                                Lifecycle.DESTROYED))
                .addTo(target);

        ServiceStateLogic.newEnricherFromChildren().checkChildrenAndMembers()
                .uniqueTag("children-service-problems-indicators")
                .requireRunningChildren(QuorumCheck.QuorumChecks.allAndAtLeastOne())
                .configure(ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers.DERIVE_SERVICE_NOT_UP, false)
                .addTo(target);

        target.enrichers().add(ServiceStateLogic.newEnricherForServiceStateFromProblemsAndUp());
        target.enrichers().add(ServiceStateLogic.ServiceNotUpLogic.newEnricherForServiceUpIfNotUpIndicatorsEmpty());
    }

    private EntitySpec descendantSpec(int... sizes) {
        if (sizes.length==0) return
                EntitySpec.create(WorkflowStartable.class)
                        .configure((ConfigKey<Object>)(ConfigKey) WorkflowStartable.START_WORKFLOW, MutableMap.of(
                                "steps", MutableList.of("sleep "+taskWorkflowDelay().toMilliseconds()+"ms")))
                        .configure((ConfigKey<Object>)(ConfigKey) WorkflowStartable.STOP_WORKFLOW, MutableMap.of(
                                "steps", MutableList.of("let x = 0")));
        EntitySpec<BasicStartable> result = EntitySpec.create(BasicStartable.class);
        int[] nextSizes = new int[sizes.length-1];
        for (int i=1; i<sizes.length; i++) nextSizes[i-1] = sizes[i];
        for (int i=0; i<sizes[0]; i++) result.child(descendantSpec(nextSizes));
        return result;
    }

    private void decorateExecutionWithChaosMonkeySleepAndLog(BasicExecutionManager ctx, Runnable onTaskStart, Runnable onTaskEnd) {
        ctx.getAutoFlagsLive().put(BasicExecutionManager.TASK_START_CALLBACK_TAG, onTaskStart);
        ctx.getAutoFlagsLive().put(BasicExecutionManager.TASK_END_CALLBACK_TAG, onTaskEnd);
    }

    private boolean includeForTaskDecoration(Task t) {
        if (t==null) return false;
        if (!Tasks.isNonProxyTask(t)) return false;
        if (t.getDisplayName().equals("periodic-persister"))
            return false;
        return true;
    }

    private void onTaskStart(Supplier<Duration> delay) {
        Task<?> current = BasicExecutionManager.getPerThreadCurrentTask().get();
        if (includeForTaskDecoration(current)) {
            // above necessary to prevent this running from
            Duration d = delay.get();
//            log.info("TASK " + current+" delay "+d+" before start");
            Time.sleep(d);
//            log.info("TASK " + current+" start");
        }
    }
    private void onTaskEnd(Supplier<Duration> delay) {
        Task<?> current = BasicExecutionManager.getPerThreadCurrentTask().get();
        if (includeForTaskDecoration(current)) {
            // above necessary to prevent this running from
//            log.info("TASK " + current+" delay before end");
            Time.sleep(delay.get());
//            log.info("TASK " + current+" end");
        }
    }

}
