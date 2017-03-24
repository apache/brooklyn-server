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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

@Beta
public class SequenceEffector extends AbstractCompositeEffector {

    private static final Logger LOG = LoggerFactory.getLogger(SequenceEffector.class);

    public static final ConfigKey<List<Object>> SEQUENCE = ConfigKeys.newConfigKey(
            new TypeToken<List<Object>>() { },
            "sequence",
            "Effector details list for the sequence effector",
            ImmutableList.<Object>of());

    public SequenceEffector(ConfigBag params) {
        super(newEffectorBuilder(params).build());
    }

    public SequenceEffector(Map<?, ?> params) {
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
            Preconditions.checkNotNull(config.getAllConfigRaw().get(SEQUENCE.getName()), "Effector names must be supplied when defining this effector");
        }

        @Override
        public Object call(final ConfigBag params) {
            List<Object> effectors = EntityInitializers.resolve(config, SEQUENCE);

            Object result = null;

            for (Object effectorDetails : effectors) {
                String effectorName = getEffectorName(effectorDetails);
                String inputArgument = getInputArgument(effectorDetails);
                Entity targetEntity = getTargetEntity(effectorDetails);

                if (inputArgument == null) {
                    throw new IllegalArgumentException("Input is not set for this effector: " + effectorDetails);
                }
                Object input = config.getStringKey(inputArgument);
                params.putStringKey(inputArgument, input);

                result = invokeEffectorNamed(targetEntity, effectorName, params);
            }

            return result;
        }
    }

}
