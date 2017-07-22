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
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

/**
 * @since 0.11.0
 */
@Beta
public class RemoveEntityEffector extends AddEffector {

    private static final Logger LOG = LoggerFactory.getLogger(RemoveEntityEffector.class);

    public static final ConfigKey<String> ENTITY_ID = ConfigKeys.builder(String.class)
            .name("entityId")
            .description("The id of the entity to be removed")
            .build();

    public static final ConfigKey<Predicate<Entity>> ENTITY_PREDICATE = ConfigKeys.builder(new TypeToken<Predicate<Entity>>() { })
            .name("predicate")
            .description("A predicate that will match the entity to be removed")
            .build();

    public RemoveEntityEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public RemoveEntityEffector(Map<String,String> params) {
        this(ConfigBag.newInstance(params));
    }

    public static EffectorBuilder<Boolean> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Boolean> eff = (EffectorBuilder) AddEffector.newEffectorBuilder(Boolean.class, params);
        eff.impl(new Body(eff.buildAbstract(), params));
        return eff;
    }

    protected static class Body extends EffectorBody<Boolean> {
        protected final Effector<?> effector;
        protected final ConfigBag config;

        protected Object mutex = new Object[0];

        public Body(Effector<?> eff, ConfigBag config) {
            this.effector = eff;
            this.config = config;
        }

        @Override
        public Boolean call(final ConfigBag params) {
            synchronized (mutex) {
                ConfigBag all = ConfigBag.newInstanceCopying(config).putAll(params);
                Predicate<Entity> predicate = EntityInitializers.resolve(all, ENTITY_PREDICATE);
                if (predicate == null) {
                    String entityId = EntityInitializers.resolve(all, ENTITY_ID);
                    predicate = EntityPredicates.idEqualTo(entityId);
                }
                Optional<Entity> child = Iterables.tryFind(entity().getChildren(), predicate);
                if (child.isPresent()) {
                    boolean success = entity().removeChild(child.get());
                    if (success) {
                        LOG.debug("{}: Removed child {} from {}", new Object[] { this, child.get(), entity() });
                        return true;
                    }
                }
                LOG.warn("{}: Could not find child of {} using {}", new Object[] { this, entity(), predicate });
                return false;
            }
        }
    }

}
