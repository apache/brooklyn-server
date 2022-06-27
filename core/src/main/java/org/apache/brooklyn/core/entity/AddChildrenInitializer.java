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
package org.apache.brooklyn.core.entity;

import com.google.common.annotations.Beta;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.brooklyn.util.core.BrooklynEntityUtils.addChildrenToEntity;
import static org.apache.brooklyn.util.core.BrooklynEntityUtils.parseBlueprintYaml;

/**
 * Entity initializer which adds children to an entity.
 * <p>
 * One of the config keys {@link #BLUEPRINT_YAML} (containing a YAML blueprint (map or string))
 * or {@link #BLUEPRINT_TYPE} (containing a string referring to a catalog type) should be supplied, but not both.
 * Parameters defined here are supplied as config during the entity creation.
 * <p>
 * Acts similar as {@link org.apache.brooklyn.core.effector.AddChildrenEffector}.
 */
@Beta
public class AddChildrenInitializer extends EntityInitializers.InitializerPatternWithConfigKeys {

    private static final Logger log = LoggerFactory.getLogger(AddChildrenInitializer.class);

    public static final ConfigKey<Object> BLUEPRINT_YAML = ConfigKeys.newConfigKey(Object.class, "blueprint_yaml");
    public static final ConfigKey<String> BLUEPRINT_TYPE = ConfigKeys.newStringConfigKey("blueprint_type");
    public static final ConfigKey<Boolean> AUTO_START = ConfigKeys.newBooleanConfigKey("auto_start");

    private AddChildrenInitializer() {}
    public AddChildrenInitializer(ConfigBag params) { super(params); }

    @Override
    public void apply(EntityLocal entity) {
        ConfigBag params = initParams();
        Object yaml = params.get(BLUEPRINT_YAML);
        String blueprintType = params.get(BLUEPRINT_TYPE);
        String blueprint = parseBlueprintYaml(yaml, blueprintType);
        addChildrenToEntity(entity, params, blueprint, params.get(AUTO_START));
    }
}