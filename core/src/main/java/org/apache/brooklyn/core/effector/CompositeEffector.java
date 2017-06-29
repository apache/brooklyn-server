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

import static org.apache.brooklyn.core.entity.trait.Startable.START;
import static org.apache.brooklyn.core.entity.trait.Startable.STOP;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.effector.composite.ReplaceEffector;
import org.apache.brooklyn.core.effector.composite.SequenceEffector;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

/**
 * @deprecated use a combintaion of {@link ReplaceEffector} and {@link SequenceEffector} instead
 */
@Beta
@Deprecated
public class CompositeEffector extends AddEffector {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeEffector.class);
    private static final String ORIGINAL_PREFIX = "original-";

    @SuppressWarnings("serial")
    public static final ConfigKey<List<String>> EFFECTORS = ConfigKeys.newConfigKey(
            new TypeToken<List<String>>() {}, 
            "effectors",
            "Effector names to be chained together in the composite effector",
            ImmutableList.<String>of());

    public static final ConfigKey<Boolean> OVERRIDE = ConfigKeys.newBooleanConfigKey(
            "override",
            "Wheter additional defined effectors should override pre-existing effector with same name or not (default: false)",
            Boolean.FALSE);

    public CompositeEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public CompositeEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<List> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<List> eff = AddEffector.newEffectorBuilder(List.class, params);
        eff.impl(new Body(eff.buildAbstract(), params));
        return eff;
    }

    @Override
    public void apply(EntityLocal entity) {
        Maybe<Effector<?>> effectorMaybe = entity.getEntityType().getEffectorByName(effector.getName());
        if (!effectorMaybe.isAbsentOrNull()) {
            Effector<?> original = Effectors.effector(effectorMaybe.get()).name(ORIGINAL_PREFIX + effector.getName()).build();
            ((EntityInternal) entity).getMutableEntityType().addEffector(original);
        }
        super.apply(entity);
    }

    protected static class Body extends EffectorBody<List> {
        private final Effector<?> effector;
        private final ConfigBag params;

        public Body(Effector<?> eff, ConfigBag params) {
            this.effector = eff;
            Preconditions.checkNotNull(params.getAllConfigRaw().get(EFFECTORS.getName()), "Effector names must be supplied when defining this effector");
            this.params = params;
        }

        @Override
        public List<Object> call(final ConfigBag params) {
            ConfigBag allConfig = ConfigBag.newInstanceCopying(this.params).putAll(params);
            final List<String> effectorNames = EntityInitializers.resolve(allConfig, EFFECTORS);
            final Boolean override = allConfig.get(OVERRIDE);

            List<Object> results = Lists.newArrayList();

            if (!override && isStartRedefined()) {
                results.add(invokeEffectorNamed(ORIGINAL_PREFIX + START.getName(), params));
            }
            for (String eff : effectorNames) {
                results.add(invokeEffectorNamed(eff, params));
            }
            if (!override && isStopRedefined()) {
                results.add(invokeEffectorNamed(ORIGINAL_PREFIX + STOP.getName(), params));
            }
            return results;
        }

        private boolean isStartRedefined() {
            return isEffectorRedefined(ORIGINAL_PREFIX + START.getName());
        }

        private boolean isStopRedefined() {
            return isEffectorRedefined(ORIGINAL_PREFIX + STOP.getName());
        }

        private boolean isEffectorRedefined(String effectorName) {
            return entity().getEntityType().getEffectorByName(effectorName).isPresent();
        }

        private Object invokeEffectorNamed(String effectorName, ConfigBag params) {
            LOG.info("{} invoking effector on {}, effector={}, parameters={}",
                    new Object[]{this, entity(), effectorName, params});
            Maybe<Effector<?>> effector = entity().getEntityType().getEffectorByName(effectorName);
            if (effector.isAbsent()) {
                throw new IllegalStateException("Cannot find effector " + effectorName);
            }
            return entity().invoke(effector.get(), params.getAllConfig()).getUnchecked();
        }

    }

}
