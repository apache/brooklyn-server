/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.policy.action;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.DurationPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

@Beta
public abstract class AbstractScheduledEffectorPolicy extends AbstractPolicy implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractScheduledEffectorPolicy.class);

    public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    protected static DateFormat FORMATTER = new SimpleDateFormat(TIME_FORMAT);

    public static final ConfigKey<String> EFFECTOR = ConfigKeys.builder(String.class)
            .name("effector")
            .description("The effector to be executed by this policy")
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<Map<String, Object>> EFFECTOR_ARGUMENTS = ConfigKeys.builder(new TypeToken<Map<String, Object>>() { })
            .name("args")
            .description("The effector arguments and their values")
            .constraint(Predicates.notNull())
            .defaultValue(ImmutableMap.<String, Object>of())
            .build();

    public static final ConfigKey<String> TIME = ConfigKeys.builder(String.class)
            .name("time")
            .description("An optional time when this policy should be first executed")
            .build();

    public static final ConfigKey<Duration> WAIT = ConfigKeys.builder(Duration.class)
            .name("wait")
            .description("An optional duration after which this policy should be first executed. The time config takes precedence if prese")
            .constraint(Predicates.or(Predicates.isNull(), DurationPredicates.positive()))
            .build();

    protected Effector<?> effector;
    protected ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    protected Object mutex = new Object[0];

    public AbstractScheduledEffectorPolicy() {
        this(MutableMap.<String,Object>of());
    }

    public AbstractScheduledEffectorPolicy(Map<String,?> props) {
        super(props);
    }

    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);
        effector = getEffector();
    }

    @Override
    public void destroy(){
        super.destroy();
        executor.shutdownNow();
    }

    protected Effector<?> getEffector() {
        String effectorName = config().get(EFFECTOR);
        Maybe<Effector<?>> effector = entity.getEntityType().getEffectorByName(effectorName);
        if (effector.isAbsent()) {
            throw new IllegalStateException("Cannot find effector " + effectorName);
        }
        return effector.get();
    }

    @Override
    public void run() {
        LOG.debug("{}: task tags before: {}", this, Iterables.toString(Tasks.current().getTags()));
        ConfigBag bag = ResolvingConfigBag.newInstanceExtending(getManagementContext(), config().getBag());
        Map<String, Object> args = EntityInitializers.resolve(bag, EFFECTOR_ARGUMENTS);
        TaskTags.addTagDynamically(Tasks.current(), BrooklynTaskTags.tagForContextEntity(entity)); // WHY?
        LOG.debug("{}: task tags after: {}", this, Iterables.toString(Tasks.current().getTags()));
        bag.putAll(args);
        Map<String, Object> resolved = Maps.newLinkedHashMap();
        for (String key : args.keySet()) {
            resolved.put(key, bag.getStringKey(key));
        }

        LOG.debug("{} invoking effector on {}, effector={}, args={}", new Object[] { this, entity, effector.getName(), resolved });
        entity.invoke(effector, args).getUnchecked();
    }
}
