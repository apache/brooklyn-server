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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.DurationPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

@Beta
public abstract class AbstractScheduledEffectorPolicy extends AbstractPolicy implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractScheduledEffectorPolicy.class);

    public static final String TIME_FORMAT = "HH:mm:ss";
    public static final String NOW = "now";
    public static final String IMMEDIATELY = "immediately";

    private static final DateFormat FORMATTER = SimpleDateFormat.getTimeInstance();

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
            .description("An optional time when this policy should be first executed, formatted as HH:mm:ss")
            .build();

    public static final ConfigKey<Duration> WAIT = ConfigKeys.builder(Duration.class)
            .name("wait")
            .description("An optional duration after which this policy should be first executed. The time config takes precedence if present")
            .constraint(Predicates.or(Predicates.isNull(), DurationPredicates.positive()))
            .build();

    protected final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    protected final Object mutex = new Object[0];

    protected Effector<?> effector;

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
        if (effector.isAbsentOrNull()) {
            throw new IllegalStateException("Cannot find effector " + effectorName);
        }
        return effector.get();
    }

    protected Duration getWaitUntil(String time) {
        if (time.equalsIgnoreCase(NOW) || time.equalsIgnoreCase(IMMEDIATELY)) {
            return Duration.ZERO;
        }
        try {
            Calendar now = Calendar.getInstance();
            Calendar when = Calendar.getInstance();
            boolean formatted = time.contains(":"); // FIXME deprecated TimeDuration coercion
            Date parsed = formatted ? FORMATTER.parse(time) : new Date(Long.parseLong(time) * 1000);
            when.setTime(parsed);
            when.set(now.get(Calendar.YEAR), now.get(Calendar.MONTH), now.get(Calendar.DATE));
            if (when.before(now)) {
                when.add(Calendar.DATE, 1);
            }
            return Duration.millis(Math.max(0, when.getTimeInMillis() - now.getTimeInMillis()));
        } catch (ParseException | NumberFormatException e) {
            LOG.warn("{}: Time should be formatted as {}: {}", new Object[] { this, TIME_FORMAT, e.getMessage() });
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public void run() {
        synchronized (mutex) {
            try {
                ConfigBag bag = ResolvingConfigBag.newInstanceExtending(getManagementContext(), config().getBag());
                Map<String, Object> args = EntityInitializers.resolve(bag, EFFECTOR_ARGUMENTS);
                LOG.debug("{}: Resolving arguments for {}: {}", new Object[] { this, effector.getName(), Iterables.toString(args.keySet()) });
                Map<String, Object> resolved = (Map) Tasks.resolving(args, Object.class)
                        .deep(true)
                        .context(entity)
                        .get();

                LOG.debug("{}: Invoking effector on {}, {}({})", new Object[] { this, entity, effector.getName(), resolved });
                Object result = entity.invoke(effector, resolved).getUnchecked();
                LOG.debug("{}: Effector {} returned {}", new Object[] { this, effector.getName(), result });
            } catch (Throwable t) {
                LOG.warn("{}: Exception running {}: {}", new Object[] { this, effector.getName(), t.getMessage() });
                Exceptions.propagate(t);
            }
        }
    }
}
