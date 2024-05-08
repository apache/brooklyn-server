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
package org.apache.brooklyn.core.workflow;

import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.feed.PollConfig;
import org.apache.brooklyn.core.feed.PollHandler;
import org.apache.brooklyn.core.feed.Poller;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.AbstractAddTriggerableSensor;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext.WorkflowContextType;
import org.apache.brooklyn.core.workflow.WorkflowSensor.WorkflowPollCallable;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/** 
 * Configurable policy which runs workflow according to schedule and/or trigger.
 */
@Beta
public class WorkflowPolicy<T> extends AbstractPolicy implements WorkflowCommonConfig {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowPolicy.class);

    public static final ConfigKey<Duration> POLICY_PERIOD = AbstractAddTriggerableSensor.SENSOR_PERIOD;
    public static final ConfigKey<Object> POLICY_TRIGGERS_SENSORS =  AbstractAddTriggerableSensor.SENSOR_TRIGGERS;
    public static final ConfigKey<Boolean> SKIP_INITIAL_RUN = AbstractAddTriggerableSensor.SKIP_INITIAL_RUN;

    public static final ConfigKey<DslPredicates.DslPredicate> CONDITION = ConfigKeys.newConfigKey(DslPredicates.DslPredicate.class, "condition", "Optional condition required for this sensor feed to run");

    public static final ConfigKey<String> UNIQUE_TAG_CAMEL = WorkflowSensor.UNIQUE_TAG_CAMEL;
    public static final ConfigKey<String> UNIQUE_TAG_UNDERSCORE = WorkflowSensor.UNIQUE_TAG_UNDERSCORE;
    public static final ConfigKey<String> UNIQUE_TAG_DASH = WorkflowSensor.UNIQUE_TAG_DASH;

    protected transient Poller<Object> poller;
    protected transient WorkflowPollCallable pollCallable;

    // ? - do we need to have an option not to run when added?

    public WorkflowPolicy() {}
    public WorkflowPolicy(Map<?,?> params) {
        super(params);
    }

    class PolicyNoOpPollHandler<V> implements PollHandler<V> {
        @Override public boolean checkSuccess(V v) { return true; }

        @Override
        public void onSuccess(V val) {}

        @Override
        public void onFailure(V val) {}

        @Override
        public void onException(Exception exception) {
            throw Exceptions.propagate(exception);
        }

        @Override
        public String getDescription() {
            return WorkflowPolicy.this.getDescription();
        }
    }

    class ConditionSupplierFromAdjunct implements Supplier<DslPredicates.DslPredicate> {
        ConditionSupplierFromAdjunct() {
        }

        @Override
        public DslPredicates.DslPredicate get() {
            return WorkflowPolicy.this.config().get(CONDITION);
        }
    }

    public String getDescription() {
        // used for poll handler; could be set later
        return null;
    }

    @Override
    protected String getDefaultDisplayName() {
        return "Workflow policy";
    }

    @Override
    public void init() {
        super.init();
    }

    public String initUniqueTag() {
        return getUniqueTag();
    }

    @Override
    public String getUniqueTag() {
        if (uniqueTag==null) {
            uniqueTag = WorkflowSensor.getUniqueTag(config().getBag(), null);
            if (uniqueTag==null) {
                uniqueTag = "workflow-policy-hash-" + Objects.hash(getDisplayName(), config().getBag());
            }
        }
        return super.getUniqueTag();
    }

    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        poller = new Poller<>(getEntity(), this, false);

        PollConfig pc = new PollConfig( (AttributeSensor) null )
                .period(getConfig(POLICY_PERIOD))
                .skipInitialRun(getConfig(SKIP_INITIAL_RUN))
                .otherTriggers(getConfig(POLICY_TRIGGERS_SENSORS))
                .condition(new ConditionSupplierFromAdjunct());

        Set<PollConfig> pollConfigs = MutableSet.of(pc);
        pollCallable = newWorkflowPollCallable();
        poller.schedulePoll(this, pollConfigs, pollCallable, new PolicyNoOpPollHandler());

        if (!isSuspended()) resume();
    }

    protected WorkflowPollCallable newWorkflowPollCallable() {
        return new WorkflowPollCallable(WorkflowContextType.POLICY, getDisplayName() + " (policy)", config().getBag(), this);
    }

    @Override
    public void suspend() {
        super.suspend();

        if (poller.isRunning()) poller.stop();
    }

    @Override
    public void resume() {
        boolean needsStarting = !poller.isRunning() || isSuspended();
        super.resume();
        if (needsStarting) poller.start();
    }

    // we could add an API for this, so arbitrary policies can be re-run from the UI
    public Object runOnceNow() {
        try {
            return pollCallable.call();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
}

