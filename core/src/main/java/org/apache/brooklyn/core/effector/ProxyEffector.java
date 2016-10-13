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

import java.util.Map;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ProxyEffector extends AddEffector {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyEffector.class);

    public static final ConfigKey<Entity> TARGET_ENTITY = ConfigKeys.newConfigKey(Entity.class,
            "targetEntity", "The proxy target");

    public static final ConfigKey<String> TARGET_EFFECTOR_NAME = ConfigKeys.newStringConfigKey(
            "targetEffector", "The effector to invoke on the target entity");

    public ProxyEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public ProxyEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<Object> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Object> eff = AddEffector.newEffectorBuilder(Object.class, params);
        eff.impl(new Body(eff.buildAbstract(), params));
        return eff;
    }

    protected static class Body extends EffectorBody<Object> {

        private final Object target;
        private final String effectorName;

        public Body(Effector<?> eff, ConfigBag params) {
            // Don't use getConfig(TARGET_ENTITY) because DslComponents can't be
            // coerced to entities at this point.
            this.target = Preconditions.checkNotNull(params.getAllConfigRaw().get(TARGET_ENTITY.getName()),
                    "Target entity must be supplied when defining this effector");
            this.effectorName = Preconditions.checkNotNull(params.get(TARGET_EFFECTOR_NAME), "Target effector name must be supplied when defining this effector");
        }

        @Override
        public Object call(ConfigBag params) {
            Entity target = resolveTarget().get();
            return invokeEffectorNamed(target, effectorName, params);
        }

        private Maybe<Entity> resolveTarget() {
            return Tasks.resolving(target, Entity.class)
                    .context(entity())
                    .getMaybe();
        }

        private Object invokeEffectorNamed(Entity target, String effectorName, ConfigBag params) {
            LOG.debug("{} forwarding effector invocation on {} to entity={}, effector={}, parameters={}",
                    new Object[]{this, entity(), target, effectorName, params});
            Effector<?> effector = EffectorUtils.findEffectorDeclared(target, effectorName).get();
            return target.invoke(effector, params.getAllConfig()).getUnchecked();
        }
    }


}
