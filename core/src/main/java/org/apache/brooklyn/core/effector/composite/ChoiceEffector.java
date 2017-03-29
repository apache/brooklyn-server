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

@Beta
public class ChoiceEffector extends AbstractCompositeEffector {

    private static final Logger LOG = LoggerFactory.getLogger(ChoiceEffector.class);

    public static final ConfigKey<String> INPUT = ConfigKeys.newStringConfigKey(
            "input",
            "Choice input parameter");

    public static final ConfigKey<Object> CHOICE = ConfigKeys.newConfigKey(
            Object.class,
            "choice",
            "Effector details for the choice effector");

    public static final ConfigKey<Object> SUCCESS = ConfigKeys.newConfigKey(
            Object.class,
            "success",
            "Effector details for the success effector");

    public static final ConfigKey<Object> FAILURE = ConfigKeys.newConfigKey(
            Object.class,
            "failure",
            "Effector details for the failure effector");

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
            Boolean result = Boolean.parseBoolean(Strings.toString(output));

            if (result) {
                Object successDetails = EntityInitializers.resolve(config, SUCCESS);
                String successEffectorName = getEffectorName(successDetails);
                String successInputArgument = getInputArgument(successDetails);
                Entity successTargetEntity = getTargetEntity(successDetails);
                LOG.debug("{} executing {}({}) on {}", new Object[] { this, successEffectorName, successInputArgument, successTargetEntity });

                if (successInputArgument == null) {
                    throw new IllegalArgumentException("Input is not set for success effector: " + successDetails);
                }
                params.putStringKey(successInputArgument, inputObject);

                return invokeEffectorNamed(successTargetEntity, successEffectorName, params);
            } else {
                Object failureDetails = EntityInitializers.resolve(config, FAILURE);
                if (failureDetails == null) {
                    return null;
                }
                String failureEffectorName = getEffectorName(failureDetails);
                String failureInputArgument = getInputArgument(failureDetails);
                Entity failureTargetEntity = getTargetEntity(failureDetails);
                LOG.debug("{} executing {}({}) on {}", new Object[] { this, failureEffectorName, failureInputArgument, failureTargetEntity });

                if (failureInputArgument == null) {
                    throw new IllegalArgumentException("Input is not set for success effector: " + failureDetails);
                }
                params.putStringKey(failureInputArgument, inputObject);

                return invokeEffectorNamed(failureTargetEntity, failureEffectorName, params);
            }
        }
    }

}
