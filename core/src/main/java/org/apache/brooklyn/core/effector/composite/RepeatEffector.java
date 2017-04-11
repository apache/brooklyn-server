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
package org.apache.brooklyn.core.effector.composite;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.math.MathPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Execute an effector a selected number of times.
 *
 * @since 0.11.0
 */
@Beta
public class RepeatEffector extends AbstractCompositeEffector {

    private static final Logger LOG = LoggerFactory.getLogger(RepeatEffector.class);

    public static final ConfigKey<Object> REPEAT = ConfigKeys.builder(Object.class)
            .name("repeat")
            .description("Effector details list for the repeat effector")
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<Integer> COUNT = ConfigKeys.builder(Integer.class)
            .name("count")
            .description("Number of times to repeat the effector")
            .constraint(MathPredicates.greaterThan(0d))
            .defaultValue(1)
            .build();

    public RepeatEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public RepeatEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<List> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<List> eff = AddEffector.newEffectorBuilder(List.class, params);
        EffectorBody<List> body = new Body(eff.buildAbstract(), params);
        eff.impl(body);
        return eff;
    }

    protected static class Body extends AbstractCompositeEffector.Body<List> {

        public Body(Effector<?> eff, ConfigBag config) {
            super(eff, config);
            Preconditions.checkNotNull(config.getAllConfigRaw().get(REPEAT.getName()), "Repeat effector details must be supplied when defining this effector");
        }

        @Override
        public List call(final ConfigBag params) {
            synchronized (mutex) {
                LOG.debug("{} called with config {}, params {}", new Object[] { this, config, params });
                Object effectorDetails = EntityInitializers.resolve(config, REPEAT);
                Integer count = EntityInitializers.resolve(config, COUNT);

                String effectorName = getEffectorName(effectorDetails);
                String inputArgument = getInputArgument(effectorDetails);
                String inputParameter = getInputParameter(effectorDetails);
                Entity targetEntity = getTargetEntity(effectorDetails);
                LOG.debug("{} executing {}({}:{}) on {}", new Object[] { this, effectorName, inputArgument, inputParameter, targetEntity });

                if (inputArgument == null) {
                    throw new IllegalArgumentException("Input is not set for this effector: " + effectorDetails);
                }
                if (inputParameter == null) {
                    Object input = params.getStringKey(inputArgument);
                    params.putStringKey(inputArgument, input);
                } else {
                    Object input = params.getStringKey(inputParameter);
                    params.putStringKey(inputArgument, input);
                }

                List<Object> results = Lists.newArrayList();
                for (int i = 0; i < count ; i++) {
                    Object result = invokeEffectorNamed(targetEntity, effectorName, params);
                    results.add(result);
                }

                LOG.debug("{} effector {} returned {}", new Object[] { this, effector.getName(), Iterables.toString(results) });
                return results;
            }
        }
    }
}
