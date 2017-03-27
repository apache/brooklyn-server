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
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

@Beta
public final class ReplaceEffector extends AbstractCompositeEffector {

    private static final Logger LOG = LoggerFactory.getLogger(TransformEffector.class);

    public enum ReplaceAction {
        PRE,
        POST,
        OVERRIDE
    }

    public static final String ORIGINAL = "original-";

    public static final ConfigKey<String> PREFIX = ConfigKeys.newStringConfigKey(
            "prefix",
            "Prefix for replaced original effector",
            ORIGINAL);

    public static final ConfigKey<ReplaceAction> ACTION = ConfigKeys.newConfigKey(
            ReplaceAction.class,
            "action",
            "Action to take with the replaced effector",
            ReplaceAction.OVERRIDE);

    public static final ConfigKey<Object> REPLACE = ConfigKeys.newConfigKey(
            Object.class,
            "replace",
            "Effector details for the replace effector");

    public ReplaceEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public ReplaceEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<Object> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Object> eff = AddEffector.newEffectorBuilder(Object.class, params);
        EffectorBody<Object> body = new Body(eff.buildAbstract(), params);
        eff.impl(body);
        return eff;
    }

    @Override
    public void apply(EntityLocal entity) {
        Maybe<Effector<?>> effectorMaybe = entity.getEntityType().getEffectorByName(effector.getName());
        if (effectorMaybe.isPresentAndNonNull()) {
            // String prefix = config.get(PREFIX);
            String prefix = ORIGINAL;
            Effector<?> original = Effectors.effector(effectorMaybe.get()).name(prefix + effector.getName()).build();
            ((EntityInternal) entity).getMutableEntityType().addEffector(original);
        }
        super.apply(entity);
    }

    protected static class Body extends AbstractCompositeEffector.Body<Object> {

        public Body(Effector<?> eff, ConfigBag config) {
            super(eff, config);
            Preconditions.checkNotNull(config.getAllConfigRaw().get(REPLACE.getName()), "Effector details must be supplied when defining this effector");
        }

        @Override
        public Object call(final ConfigBag params) {
            LOG.info("{} called with config {}", new Object[] { this, config });
            ReplaceAction action = config.get(ACTION);
            Object effectorDetails = EntityInitializers.resolve(config, REPLACE);

            String effectorName = getEffectorName(effectorDetails);
            String inputArgument = getInputArgument(effectorDetails);
            Entity targetEntity = getTargetEntity(effectorDetails);
            LOG.info("{} executing {}({}) on {}", new Object[] { this, effectorName, inputArgument, targetEntity });

            if (inputArgument != null) {
                Object input = config.getStringKey(inputArgument);
                params.putStringKey(inputArgument, input);
            }

            if (action == ReplaceAction.POST) {
                invokeEffectorNamed(targetEntity, ORIGINAL + effectorName, params);
            }
            Object result = invokeEffectorNamed(targetEntity, effectorName, params);
            if (action == ReplaceAction.PRE) {
                invokeEffectorNamed(targetEntity, ORIGINAL + effectorName, params);
            }

            return result;
        }
    }
}
