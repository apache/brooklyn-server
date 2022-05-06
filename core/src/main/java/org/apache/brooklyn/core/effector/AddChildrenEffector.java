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

import com.google.common.annotations.Beta;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.brooklyn.util.core.BrooklynEntityUtils.addChildrenToEntity;
import static org.apache.brooklyn.util.core.BrooklynEntityUtils.parseBlueprintYaml;

/** Entity initializer which defines an effector which adds a child blueprint to an entity.
 * <p>
 * One of the config keys {@link #BLUEPRINT_YAML} (containing a YAML blueprint (map or string)) 
 * or {@link #BLUEPRINT_TYPE} (containing a string referring to a catalog type) should be supplied, but not both.
 * Parameters defined here are supplied as config during the entity creation.
 * 
 * @since 0.7.0*/
@Beta
public class AddChildrenEffector extends AddEffectorInitializerAbstract {
    
    private static final Logger log = LoggerFactory.getLogger(AddChildrenEffector.class);
    
    public static final ConfigKey<Object> BLUEPRINT_YAML = ConfigKeys.newConfigKey(Object.class, "blueprint_yaml");
    public static final ConfigKey<String> BLUEPRINT_TYPE = ConfigKeys.newStringConfigKey("blueprint_type");
    public static final ConfigKey<Boolean> AUTO_START = ConfigKeys.newBooleanConfigKey("auto_start");
    
    private AddChildrenEffector() {}
    public AddChildrenEffector(ConfigBag params) { super(params); }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public EffectorBuilder<List<String>> newEffectorBuilder() {
        EffectorBuilder<List<String>> eff = (EffectorBuilder) newAbstractEffectorBuilder(List.class);
        eff.impl(new Body(eff.buildAbstract(), initParams()));
        return eff;
    }
    
    protected static class Body extends EffectorBody<List<String>> {

        private final Effector<?> effector;
        private final String blueprintBase;
        private final Boolean autostart;

        public Body(Effector<?> eff, ConfigBag params) {
            this.effector = eff;
            Object yaml = params.get(BLUEPRINT_YAML);
            String blueprintType = params.get(BLUEPRINT_TYPE);
            blueprintBase = parseBlueprintYaml(yaml, blueprintType);
            autostart = params.get(AUTO_START);
        }

        @Override
        public List<String> call(ConfigBag params) {
            ConfigBag paramsMerged = getMergedParams(effector, params);
            return addChildrenToEntity(entity(), paramsMerged, blueprintBase, autostart);
        }
    }
}
