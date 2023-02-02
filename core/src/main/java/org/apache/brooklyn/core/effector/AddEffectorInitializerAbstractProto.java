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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.effector.ParameterType;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Beta
public abstract class AddEffectorInitializerAbstractProto extends EntityInitializers.InitializerPatternWithConfigKeys {

    public static final ConfigKey<String> EFFECTOR_NAME = ConfigKeys.newStringConfigKey("name");
    public static final ConfigKey<String> EFFECTOR_DESCRIPTION = ConfigKeys.newStringConfigKey("description");

    public static final ConfigKey<Map<String,Object>> EFFECTOR_PARAMETER_DEFS = new MapConfigKey<Object>(Object.class, "parameters");

    protected AddEffectorInitializerAbstractProto(ConfigBag params) {
        super(params);
    }
    // JSON deserialization constructor
    protected AddEffectorInitializerAbstractProto() {}

    protected abstract Effector<?> effector();

    @Override
    public void apply(EntityLocal entity) {
        ((EntityInternal)entity).getMutableEntityType().addEffector(effector());
    }

    /** returns a ConfigBag containing the merger of the supplied parameters with default values on the effector-defined parameters */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static ConfigBag getMergedParams(Effector<?> eff, ConfigBag params) {
        ConfigBag result = ConfigBag.newInstanceCopying(params);
        for (ParameterType<?> param: eff.getParameters()) {
            ConfigKey key = Effectors.asConfigKey(param);
            if (!result.containsKey(key))
                result.configure(key, params.get(key));
        }
        return result;
    }

    protected static <T> EffectorBuilder<T> newEffectorBuilder(Class<T> type, ConfigBag params) {
        String name = Preconditions.checkNotNull(params.get(EFFECTOR_NAME), "name must be supplied when defining an effector: %s", params);
        EffectorBuilder<T> eff = Effectors.effector(type, name);
        eff.description(params.get(EFFECTOR_DESCRIPTION));

        BrooklynClassLoadingContext loader = null;  // we need an entity or mgmt context to get a loader, but we don't need to resolve the types so no big deal
        Effectors.parseParameters(params.get(EFFECTOR_PARAMETER_DEFS), loader).forEach(p -> eff.parameter(p));

        return eff;
    }

}
