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
import java.util.Set;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.collect.Iterables;

@Beta
public abstract class AbstractCompositeEffector extends AddEffector {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCompositeEffector.class);

    public AbstractCompositeEffector(Effector<?> effector) {
        super(effector);
    }

    protected static abstract class Body<T> extends EffectorBody<T> {
        protected final Effector<?> effector;
        protected final ConfigBag config;

        protected Object mutex = new Object[0];

        public Body(Effector<?> eff, ConfigBag config) {
            this.effector = eff;
            this.config = config;
        }

        @Override
        public abstract T call(final ConfigBag params);

        protected String getEffectorName(Object effectorDetails) {
            String effectorName = null;
            if (effectorDetails instanceof String) {
                effectorName = (String) effectorDetails;
            } else if (effectorDetails instanceof Map) {
                Map<String,Object> effectorMap = (Map) effectorDetails;
                Set<String> keys = effectorMap.keySet();
                if (keys.size() != 1) {
                    throw new IllegalArgumentException("Effector parameter cannot be parsed: " + effectorDetails);
                }
                effectorName = Iterables.getOnlyElement(keys);
            } else {
                effectorName = Strings.toString(effectorDetails);
            }
            return effectorName;
        }

        protected Entity getTargetEntity(Object effectorDetails) {
            Entity targetEntity = entity();
            if (effectorDetails instanceof Map) {
                Map<String,Object> effectorMap = (Map) effectorDetails;
                Set<String> keys = effectorMap.keySet();
                if (keys.size() != 1) {
                    throw new IllegalArgumentException("Effector parameter cannot be parsed: " + effectorDetails);
                }
                String effectorName = Iterables.getOnlyElement(keys);
                Object effectorParams = effectorMap.get(effectorName);
                if (effectorParams instanceof Map) {
                    Map<String,Object> effectorParamsMap = (Map) effectorParams;
                    if (effectorParamsMap.containsKey("target")) {
                        Object targetObject = effectorParamsMap.get("target");
                        if (targetObject instanceof Entity) {
                            targetEntity = (Entity) targetObject;
                        } else {
                            throw new IllegalArgumentException("Effector target is not an Entity: " + targetObject);
                        }
                    }
                }
            }
            return targetEntity;
        }

        protected String getInputArgument(Object effectorDetails) {
            String inputArgument = null;
            if (effectorDetails instanceof Map) {
                Map<String,Object> effectorMap = (Map) effectorDetails;
                Set<String> keys = effectorMap.keySet();
                if (keys.size() != 1) {
                    throw new IllegalArgumentException("Effector parameter cannot be parsed: " + effectorDetails);
                }
                String effectorName = Iterables.getOnlyElement(keys);
                Object effectorParams = effectorMap.get(effectorName);
                if (effectorParams instanceof Map) {
                    Map<String,Object> effectorParamsMap = (Map) effectorParams;
                    if (effectorParamsMap.containsKey("input")) {
                        Object inputDetails = effectorParamsMap.get("input");
                        if (inputDetails instanceof String) {
                            inputArgument = (String) inputDetails;
                        } else if (inputDetails instanceof Map) {
                            Map<String,Object> inputMap = (Map) inputDetails;
                            Set<String> inputKeys = inputMap.keySet();
                            if (inputKeys.size() != 1) {
                                throw new IllegalArgumentException("Effector input cannot be parsed: " + inputDetails);
                            }
                            inputArgument = Iterables.getOnlyElement(inputKeys);
                        } else {
                            inputArgument = Strings.toString(inputDetails);
                        }
                    }
                }
            }
            return inputArgument;
        }

        protected String getInputParameter(Object effectorDetails) {
            String inputArgument = getInputArgument(effectorDetails);
            String inputParameter = null;
            if (inputArgument != null && effectorDetails instanceof Map) {
                Map<String,Object> effectorMap = (Map) effectorDetails;
                Set<String> keys = effectorMap.keySet();
                if (keys.size() != 1) {
                    throw new IllegalArgumentException("Effector parameter cannot be parsed: " + effectorDetails);
                }
                String effectorName = Iterables.getOnlyElement(keys);
                Object effectorParams = effectorMap.get(effectorName);
                if (effectorParams instanceof Map) {
                    Map<String,Object> effectorParamsMap = (Map) effectorParams;
                    if (effectorParamsMap.containsKey("input")) {
                        Object inputDetails = effectorParamsMap.get("input");
                        if (inputDetails instanceof Map) {
                            Map<String,Object> inputMap = (Map) inputDetails;
                            if (inputMap.size() != 1) {
                                throw new IllegalArgumentException("Effector input cannot be parsed: " + inputDetails);
                            }
                            inputParameter = Strings.toString(inputMap.get(inputArgument));
                        }
                    }
                }
            }
            return inputParameter;
        }

        protected Object invokeEffectorNamed(Entity target, String effectorName, ConfigBag params) {
            LOG.debug("{} invoking {} with params {}", new Object[] { this, effectorName, params });
            Maybe<Effector<?>> effector = target.getEntityType().getEffectorByName(effectorName);
            if (effector.isAbsent()) {
                throw new IllegalStateException("Cannot find effector " + effectorName);
            }
            return target.invoke(effector.get(), params.getAllConfig()).getUnchecked();
        }

    }

}
