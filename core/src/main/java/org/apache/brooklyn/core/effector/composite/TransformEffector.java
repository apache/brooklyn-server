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
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.reflect.TypeToken;

/**
 * Execute a {@link Function} as an effector body.
 *
 * @since 0.11.0
 */
@Beta
public class TransformEffector extends AbstractCompositeEffector {

    private static final Logger LOG = LoggerFactory.getLogger(TransformEffector.class);

    public static final ConfigKey<String> INPUT = ConfigKeys.builder(String.class)
            .name("input")
            .description("Transformer input parameter")
            .build();

    public static final ConfigKey<Function<Object,Object>> FUNCTION = ConfigKeys.builder(new TypeToken<Function<Object,Object>>() { })
            .name("function")
            .description("Transformer function to apply")
            .constraint(Predicates.notNull())
            .defaultValue(Functions.identity())
            .build();

    public TransformEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public TransformEffector(Map<?, ?> params) {
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
            Preconditions.checkNotNull(config.getAllConfigRaw().get(FUNCTION.getName()), "Transform function must be supplied when defining this effector");
        }

        @Override
        public Object call(final ConfigBag params) {
            synchronized (mutex) {
                LOG.debug("{} called with config {}, params {}", new Object[] { this, config, params });
                Function<Object,Object> function = EntityInitializers.resolve(config, FUNCTION);

                String input = config.get(INPUT);
                Object value = params.getStringKey(input);
                Object result = function.apply(value);

                LOG.debug("{} effector {} returned {}", new Object[] { this, effector.getName(), result });
                return result;
            }
        }
    }
}