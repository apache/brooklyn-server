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
public class WorkflowPolicy<T> extends AbstractPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowPolicy.class);

    public static final ConfigKey<Duration> POLICY_PERIOD = ConfigKeys.newConfigKey(Duration.class, "period", "Period, including units e.g. 1m or 5s or 200ms", null);
    public static final ConfigKey<Object> POLICY_TRIGGERS_SENSORS = ConfigKeys.newConfigKey(new TypeToken<Object>() {}, "triggers",
            "Sensors which should trigger this policy, supplied with list of maps containing sensor (name or sensor instance) and entity (ID or entity instance), or just sensor names or just one sensor");

    public static final ConfigKey<DslPredicates.DslPredicate> CONDITION = ConfigKeys.newConfigKey(DslPredicates.DslPredicate.class, "condition", "Optional condition required for this sensor feed to run");

    public static final ConfigKey<Map<String,Object>> INPUT = WorkflowCommonConfig.INPUT;
    public static final ConfigKey<List<Object>> STEPS = WorkflowCommonConfig.STEPS;

    protected transient Poller<Object> poller;

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
        initUniqueTag();
        super.init();
    }

    public String initUniqueTag() {
        if (uniqueTag==null) uniqueTag = "workflow-policy-hash-"+ Objects.hash(getDisplayName(), config().getBag());
        return super.getUniqueTag();
    }

    @Override
    public void setEntity(EntityLocal entity) {
        initUniqueTag();
        super.setEntity(entity);

        poller = new Poller<>(getEntity(), this, false);

        PollConfig pc = new PollConfig( (AttributeSensor) null )
                .period(getConfig(POLICY_PERIOD))
                .otherTriggers(getConfig(POLICY_TRIGGERS_SENSORS))
                .condition(new ConditionSupplierFromAdjunct());

        Set<PollConfig> pollConfigs = MutableSet.of(pc);
        poller.schedulePoll(this, pollConfigs, new WorkflowSensor.WorkflowPollCallable(
                getDisplayName() + " (workflow)", config().getBag(), this), new PolicyNoOpPollHandler());

        if (!isSuspended()) resume();
    }

    @Override
    public void suspend() {
        super.suspend();

        poller.stop();
    }

    @Override
    public void resume() {
        boolean wasSuspended = isSuspended();
        super.resume();
        if (!wasSuspended) poller.start();
    }

}

