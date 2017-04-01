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

import java.util.Collection;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@Beta
public class LoopEffector extends AbstractCompositeEffector {

    private static final Logger LOG = LoggerFactory.getLogger(LoopEffector.class);

    public static final ConfigKey<String> INPUT = ConfigKeys.newStringConfigKey(
            "input",
            "Loop input parameter");

    public static final ConfigKey<Object> LOOP = ConfigKeys.newConfigKey(
            Object.class,
            "loop",
            "Effector details for the loop effector");

    public LoopEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public LoopEffector(Map<?, ?> params) {
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
            Preconditions.checkNotNull(config.getAllConfigRaw().get(LOOP.getName()), "Effector names must be supplied when defining this effector");
        }

        @Override
        public List call(final ConfigBag params) {
            synchronized (mutex) {
                LOG.debug("{} called with config {}, params {}", new Object[] { this, config, params });
                Object effectorDetails = EntityInitializers.resolve(config, LOOP);
                String input = config.get(INPUT);
                Object inputObject = params.getStringKey(input);
                if (!(inputObject instanceof Collection)) {
                    throw new IllegalArgumentException("Input to loop is not a collection: " + inputObject);
                }
                Collection<?> inputCollection = (Collection) inputObject;

                List<Object> result = Lists.newArrayList();

                String effectorName = getEffectorName(effectorDetails);
                String inputArgument = getInputArgument(effectorDetails);
                Entity targetEntity = getTargetEntity(effectorDetails);
                LOG.debug("{} executing {}({}) on {}", new Object[] { this, effectorName, inputArgument, targetEntity });

                if (inputArgument == null) {
                    throw new IllegalArgumentException("Input is not set for this effector: " + effectorDetails);
                }

                for (Object inputEach : inputCollection) {
                    params.putStringKey(inputArgument, inputEach);

                    result.add(invokeEffectorNamed(targetEntity, effectorName, params));
                }

                LOG.debug("{} effector {} returned {}", new Object[] { this, effector.getName(), result });
                return result;
            }
        }
    }
}
