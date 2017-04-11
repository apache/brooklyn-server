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
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;

/**
 * Execute an effector, and depending on the result, execute either
 * the success or failure effector.
 *
 * @since 0.11.0
 */
@Beta
public class ChoiceEffector extends AbstractCompositeEffector {

    private static final Logger LOG = LoggerFactory.getLogger(ChoiceEffector.class);

    public static final ConfigKey<String> INPUT = ConfigKeys.builder(String.class)
            .name("input")
            .description("Choice input parameter")
            .build();

    public static final ConfigKey<Object> CHOICE = ConfigKeys.builder(Object.class)
            .name("choice")
            .description("Effector details for the choice effector")
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<Object> SUCCESS = ConfigKeys.builder(Object.class)
            .name("success")
            .description("Effector details for the success effector")
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<Object> FAILURE = ConfigKeys.builder(Object.class)
            .name("failure")
            .description("Effector details for the failure effector")
            .build();

    public ChoiceEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public ChoiceEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<Object> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Object> eff = AddEffector.newEffectorBuilder(Object.class, params);
        EffectorBody<Object> body = new Body(eff.buildAbstract(), params);
        eff.impl(body);
        return eff;
    }

    protected static class Body extends AbstractCompositeEffector.Body<Object> {

        public Body(Effector<?> eff, ConfigBag config) {
            super(eff, config);
            Preconditions.checkNotNull(config.getAllConfigRaw().get(CHOICE.getName()), "Choice effector details must be supplied when defining this effector");
            Preconditions.checkNotNull(config.getAllConfigRaw().get(SUCCESS.getName()), "Success effector details must be supplied when defining this effector");
        }

        @Override
        public Object call(final ConfigBag params) {
            synchronized (mutex) {
                LOG.debug("{} called with config {}, params {}", new Object[] { this, config, params });
                String input = config.get(INPUT);
                Object inputObject = params.getStringKey(input);

                Object choiceDetails = EntityInitializers.resolve(config, CHOICE);
                String choiceEffectorName = getEffectorName(choiceDetails);
                String choiceInputArgument = getInputArgument(choiceDetails);
                Entity choiceTargetEntity = getTargetEntity(choiceDetails);
                LOG.debug("{} executing {}({}) on {}", new Object[] { this, choiceEffectorName, choiceInputArgument, choiceTargetEntity });

                if (choiceInputArgument == null) {
                    throw new IllegalArgumentException("Input is not set for choice effector: " + choiceDetails);
                }
                params.putStringKey(choiceInputArgument, inputObject);

                Object output = invokeEffectorNamed(choiceTargetEntity, choiceEffectorName, params);
                Boolean success = Boolean.parseBoolean(Strings.toString(output));
                LOG.debug("{} result of {} was {}/{}", new Object[] { this, choiceEffectorName, Strings.toString(output), success });

                Object effectorDetails = EntityInitializers.resolve(config, success ? SUCCESS : FAILURE);

                if (!success && effectorDetails == null) {
                    return null;
                }

                String effectorName = getEffectorName(effectorDetails);
                String inputArgument = getInputArgument(effectorDetails);
                Entity targetEntity = getTargetEntity(effectorDetails);
                LOG.debug("{} executing {}({}) on {}", new Object[] { this, effectorName, inputArgument, targetEntity });

                if (inputArgument == null) {
                    throw new IllegalArgumentException("Input is not set for effector: " + effectorDetails);
                }
                params.putStringKey(inputArgument, inputObject);
                Object result = invokeEffectorNamed(targetEntity, effectorName, params);

                LOG.debug("{} effector {} returned {}", new Object[] { this, effector.getName(), result });
                return result;
            }
        }
    }
}