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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Function;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.objs.BasicConfigurableObject;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ConfigKeySelfExtracting;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;

import com.google.common.collect.ImmutableList;

public class EntityInitializers {

    public static class AddTags implements EntityInitializer {
        public final List<Object> tags;

        private AddTags() { tags = null; }
        public AddTags(Object... tags) {
            this.tags = ImmutableList.copyOf(tags);
        }
        
        @Override
        public void apply(EntityLocal entity) {
            for (Object tag: tags)
                entity.tags().addTag(tag);
        }
    }

    public static EntityInitializer addingTags(Object... tags) {
        return new AddTags(tags);
    }

    /**
     * Resolves key in the
     * {@link BasicExecutionContext#getCurrentExecutionContext current execution context}.
     * @see #resolve(ConfigBag, ConfigKey, ExecutionContext)
     */
    public static <T> T resolve(ConfigBag configBag, ConfigKey<T> key) {
        return resolve(configBag, key, BasicExecutionContext.getCurrentExecutionContext());
    }

    /**
     * Gets the value for key from configBag.
     * <p>
     * If key is an instance of {@link ConfigKeySelfExtracting} and executionContext is
     * not null then its value will be retrieved per the key's implementation of
     * {@link ConfigKeySelfExtracting#extractValue extractValue}. Otherwise, the value
     * will be retrieved from configBag directly.
     */
    public static <T> T resolve(ConfigBag configBag, ConfigKey<T> key, ExecutionContext executionContext) {
        if (key instanceof ConfigKeySelfExtracting && executionContext != null) {
            ConfigKeySelfExtracting<T> ckse = ((ConfigKeySelfExtracting<T>) key);
            T result = ckse.extractValue(configBag.getAllConfigAsConfigKeyMap(), executionContext);
            return (result != null || configBag.containsKey(key.getName())) ? result : key.getDefaultValue();
        }
        return configBag.get(key);
    }

    /**
     * Pattern for an abstract superclass for EntityInitializers like {@link InitializerPatternWithConfigKeys}
     * but for where the config map should not be kept or accessible after initialization,
     * and the code prefers to use fields.
     * <p>
     * An implementer of this pattern should declare config keys as per {@link InitializerPatternWithConfigKeys},
     * and also an initializer block invoking {@link #addInitConfigMapping(ConfigKey, Function)} for each config key,
     * giving a setter function (eg <code>v -> myField = v</code>).
     * Subsequently they access their fields directly because there is no access to the parameters after instantiation.
     * <p>
     * The main reason to do this instead of {@link InitializerPatternWithConfigKeys} is legacy code that depended on fields,
     * but it might allow for cleaner code to have fields.
     **/
    public abstract static class InitializerPatternWithFieldsFromConfigKeys extends InitializerPatternHandlingConfigNeatlyOnSerializeAndDeserialize {
        protected InitializerPatternWithFieldsFromConfigKeys() {}
        protected InitializerPatternWithFieldsFromConfigKeys(ConfigBag params) { super(params); }

        @JsonIgnore
        private List<BiConsumer<ConfigBag,Boolean>> configKeyInitRules = MutableList.of();
        protected <T> void addInitConfigMapping(ConfigKey<T> key, Function<T,T> setter) {
            addInitConfigRule( (ConfigBag config, Boolean isFirst) -> {
                config.ifPresent(key).transformNow((Function)setter)
                        .or(()->{
                            if (isFirst && key.hasDefaultValue()) {
                                setter.apply(key.getDefaultValue());
                            }
                            return null;
                        });
            });
        }
        protected <T> void addInitConfigRule(BiConsumer<ConfigBag,Boolean> rule) {
            configKeyInitRules.add(rule);
            rule.accept(initParams(), true);
        }

        protected void initFromConfigBag(ConfigBag params) {
            if (configKeyInitRules!=null) configKeyInitRules.forEach( c -> c.accept(params, false) );
            super.initFromConfigBag(params);
        }

        /** convenience for where {@link #addInitConfigMapping(ConfigKey, Function)} and {@link #addInitConfigRule(BiConsumer)} are being used,
         * to allow clients to check there is no unused config supplied, and fail if so;
         * note this must not be done in any constructor or initializer block which might be subclassed
         * as the subclass blocks which add new rules may not have been defined yet.
         * thus it is recommended to call this only on usage, not during construction.
         * if rules/mappings are not being used, the constraints are stronger:
         * it should only be called once code expects to have accessed all supported parameters.
         **/
        protected void initParamsFailIfAnyUnused() {
            if (!initParams().getUnusedConfig().isEmpty()) {
                throw new IllegalArgumentException("Unused parameters in "+this+": "+ initParams().getUnusedConfig());
            }
        }
    }
    /**
     * Preferred pattern for an abstract superclass for EntityInitializers that provide conveniences for working with config keys,
     * This ensures json / bean-with-type creation works, either in brooklyn.config or not, under a <code>brooklyn.initializers</code> block.
     * <p>
     * In many cases simply extending <code>EntityInitializer</code> directly and not using this pattern is fine.
     * However config keys are useful for declarative reflection, programmatic initialization, and additional coercion.
     * This superclass pattern does all the wiring for serialization and deserialization.
     * <p>
     * Note that fields should not be used in classes that extend this; not a big deal if they are but it can
     * cause some surprises during deserialization (from CAMP YAML) or during persistence.
     * See {@link InitializerPatternWithFieldsFromConfigKeys} for support for fields.
     * <p>
     * An implementer of this pattern should declare config keys, then
     * access them with {@link #initParam(ConfigKey)} for one or {@link #initParams()} for the bag.
     * <p>
     * The implementer should provide two constructors as per the ones here (one no-arg for json / bean-with-type creation,
     * and one taking a ConfigBag for programmatic use and prior convention).
     * <p>
     * Developers can then write the {@link #apply(EntityLocal)} method from {@link EntityInitializer}
     * as normal, using fields of {@link #initParam(ConfigKey)}.
     **/
    public abstract static class InitializerPatternWithConfigKeys extends InitializerPatternHandlingConfigNeatlyOnSerializeAndDeserialize {
        protected InitializerPatternWithConfigKeys() {}
        protected InitializerPatternWithConfigKeys(ConfigBag params) { super(params); }

        protected <T> T initParam(ConfigKey<T> key) { return initParams().get(key); }
        protected ConfigBag initParams() { return super.initParams(); }

        @JsonProperty("brooklyn.config")
        protected Map<String, Object> getBrooklynConfig() {
            return initParams().getAllConfig();
        }
    }
    // internal superclass for the above two
    private abstract static class InitializerPatternHandlingConfigNeatlyOnSerializeAndDeserialize implements EntityInitializer, Serializable {
        protected InitializerPatternHandlingConfigNeatlyOnSerializeAndDeserialize() {}
        protected InitializerPatternHandlingConfigNeatlyOnSerializeAndDeserialize(ConfigBag params) { initFromConfigBag(params); }

        protected void initFromConfigBag(ConfigBag params) {
            if (params!=null && !params.isEmpty()) {
                initParams.putAll(params);
            }
        }

        @JsonIgnore
        private ConfigBag initParams = ConfigBag.newInstance();

        ConfigBag initParams() { return initParams; }

        @JsonAnySetter @JsonDeserialize(contentUsing = JsonPassThroughDeserializer.class)
        protected void initField(String key, Object value) {
            initFromConfigBag(ConfigBag.newInstance(MutableMap.of(key, value)));
        }

        @JsonProperty("brooklyn.config") @JsonDeserialize(contentUsing = JsonPassThroughDeserializer.class)
        private void initBrooklynConfig(Map<String,Object> params) {
            initFromConfigBag(ConfigBag.newInstance(params));
        }

        // introduced in 1.1 for legacy compatibility
        protected Object readResolve() {
            if (initParams==null) initParams = ConfigBag.newInstance();
            return this;
        }
    }

    /**
     * Pattern for an abstract superclass for EntityInitializers that provides a {@link org.apache.brooklyn.api.objs.Configurable}
     * interface.  Other than the availability of {@link #config()} this acts as per {@link InitializerPatternWithFieldsFromConfigKeys}.
     **/
    public abstract static class InitializerPatternForConfigurable extends BasicConfigurableObject implements EntityInitializer {
        protected InitializerPatternForConfigurable() {}
        protected InitializerPatternForConfigurable(ConfigBag params) { initFromConfigBag(params); }

        protected void initFromConfigBag(ConfigBag params) {
            if (params!=null) params.forEach((k,v)->
                config().set(ConfigKeys.newConfigKey(Object.class, k), v) );
        }

        @JsonAnySetter
        private void initField(String key, Object value) {
            config().set(ConfigKeys.newConfigKey(Object.class, key), value);
            initFromConfigBag(ConfigBag.newInstance(MutableMap.of(key, value)));
        }
        @JsonProperty("brooklyn.config")
        private void initBrooklynConfig(Map<String,Object> params) {
            initFromConfigBag(ConfigBag.newInstance(params));
        }
    }

}
