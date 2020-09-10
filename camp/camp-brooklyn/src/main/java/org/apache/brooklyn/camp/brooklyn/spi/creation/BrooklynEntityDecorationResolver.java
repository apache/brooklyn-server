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
package org.apache.brooklyn.camp.brooklyn.spi.creation;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampReservedKeys;
import org.apache.brooklyn.camp.brooklyn.spi.creation.BrooklynYamlTypeInstantiator.InstantiatorFromKey;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.objs.BasicSpecParameter;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

/**
 * Pattern for resolving "decorations" on service specs / entity specs, such as policies, enrichers, etc.
 *
 * @since 0.7.0
 */
public abstract class BrooklynEntityDecorationResolver<DT> {

    public final BrooklynYamlTypeInstantiator.Factory instantiator;

    protected BrooklynEntityDecorationResolver(BrooklynYamlTypeInstantiator.Factory instantiator) {
        this.instantiator = instantiator;
    }

    /** @deprected since 0.10.0 - use {@link #decorate(EntitySpec<?>, ConfigBag, Set<String>)} */
    @Deprecated
    public final void decorate(EntitySpec<?> entitySpec, ConfigBag attrs) {
        decorate(entitySpec, attrs, ImmutableSet.<String>of());
    }

    public abstract void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds);

    protected List<? extends DT> buildListOfTheseDecorationsFromEntityAttributes(ConfigBag attrs) {
        Object value = getDecorationAttributeJsonValue(attrs); 
        if (value==null) { return MutableList.of(); }
        if (value instanceof Iterable) {
            return buildListOfTheseDecorationsFromIterable((Iterable<?>)value);
        } else {
            // in future may support types other than iterables here, 
            // e.g. a map short form where the key is the type
            throw new IllegalArgumentException(getDecorationKind()+" body should be iterable, not " + value.getClass());
        }
    }

    protected Map<?,?> checkIsMap(Object decorationJson) {
        if (!(decorationJson instanceof Map)) {
            throw new IllegalArgumentException(getDecorationKind()+" value must be a Map, not " + 
                (decorationJson==null ? null : decorationJson.getClass()) );
        }
        return (Map<?,?>) decorationJson;
    }

    protected List<DT> buildListOfTheseDecorationsFromIterable(Iterable<?> value) {
        List<DT> decorations = MutableList.of();
        for (Object decorationJson : value) {
            addDecorationFromJsonMap(checkIsMap(decorationJson), decorations);
        }
        return decorations;
    }

    protected abstract String getDecorationKind();

    protected abstract Object getDecorationAttributeJsonValue(ConfigBag attrs);

    /**
     * Creates and adds decorations from the given json to the given collection; 
     * default impl requires a map and calls {@link #addDecorationFromJsonMap(Map, List)}
     */
    protected void addDecorationFromJson(Object decorationJson, List<DT> decorations) {
        addDecorationFromJsonMap(checkIsMap(decorationJson), decorations);
    }

    protected abstract void addDecorationFromJsonMap(Map<?,?> decorationJson, List<DT> decorations);

    public static class PolicySpecResolver extends BrooklynEntityDecorationResolver<PolicySpec<?>> {

        public PolicySpecResolver(BrooklynYamlTypeInstantiator.Factory loader) { super(loader); }

        @Override
        protected String getDecorationKind() { return "Policy"; }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            entitySpec.policySpecs(buildListOfTheseDecorationsFromEntityAttributes(attrs));
        }

        @Override
        protected Object getDecorationAttributeJsonValue(ConfigBag attrs) {
            return attrs.getStringKey(BrooklynCampReservedKeys.BROOKLYN_POLICIES);
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<PolicySpec<?>> decorations) {
            InstantiatorFromKey decoLoader = instantiator.from(decorationJson).prefix("policy");

            String policyType = decoLoader.getTypeName().get();
            ManagementContext mgmt = instantiator.loader.getManagementContext();

            Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(policyType), RegisteredTypeLoadingContexts.spec(Policy.class));
            PolicySpec<?> spec;
            if (!item.isNull()) {
                // throw error if absent for any reason other than null
                spec = mgmt.getTypeRegistry().createSpec(item.get(), null, PolicySpec.class);
            } else {
                Class<? extends Policy> type = decoLoader.getType(Policy.class);
                spec = PolicySpec.create(type)
                        .parameters(BasicSpecParameter.fromClass(mgmt, type));
            }
            spec.configure(decoLoader.getConfigMap());
            decorations.add(spec);
        }
    }

    public static class EnricherSpecResolver extends BrooklynEntityDecorationResolver<EnricherSpec<?>> {

        public EnricherSpecResolver(BrooklynYamlTypeInstantiator.Factory loader) { super(loader); }

        @Override
        protected String getDecorationKind() { return "Enricher"; }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            entitySpec.enricherSpecs(buildListOfTheseDecorationsFromEntityAttributes(attrs));
        }

        @Override
        protected Object getDecorationAttributeJsonValue(ConfigBag attrs) {
            return attrs.getStringKey(BrooklynCampReservedKeys.BROOKLYN_ENRICHERS);
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<EnricherSpec<?>> decorations) {
            InstantiatorFromKey decoLoader = instantiator.from(decorationJson).prefix("enricher");
            Class<? extends Enricher> type = decoLoader.getType(Enricher.class);
            decorations.add(EnricherSpec.create(type)
                .configure(decoLoader.getConfigMap())
                .parameters(BasicSpecParameter.fromClass(instantiator.loader.getManagementContext(), type)));
        }
    }

    public static class InitializerResolver extends BrooklynEntityDecorationResolver<EntityInitializer> {

        public InitializerResolver(BrooklynYamlTypeInstantiator.Factory loader) { super(loader); }

        @Override 
        protected String getDecorationKind() { return "Entity initializer"; }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            entitySpec.addInitializers(buildListOfTheseDecorationsFromEntityAttributes(attrs));
        }

        @Override
        protected Object getDecorationAttributeJsonValue(ConfigBag attrs) {
            return attrs.getStringKey(BrooklynCampReservedKeys.BROOKLYN_INITIALIZERS);
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<EntityInitializer> decorations) {
            decorations.add(instantiator.from(decorationJson).prefix("initializer").newInstance(EntityInitializer.class));
        }
    }

    // Not much value from extending from BrooklynEntityDecorationResolver, but let's not break the convention
    public static class SpecParameterResolver extends BrooklynEntityDecorationResolver<SpecParameter<?>> {

        private Function<Object, Object> transformer;

        protected SpecParameterResolver(BrooklynYamlTypeInstantiator.Factory instantiator) { super(instantiator); }

        @Override
        protected String getDecorationKind() { return "Spec Parameter initializer"; }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            transformer = new BrooklynComponentTemplateResolver.SpecialFlagsTransformer(instantiator.loader, encounteredRegisteredTypeIds);
            // entitySpec is the parent
            List<? extends SpecParameter<?>> explicitParams = buildListOfTheseDecorationsFromEntityAttributes(attrs);
            BasicSpecParameter.initializeSpecWithExplicitParameters(entitySpec, explicitParams, instantiator.loader);
        }

        public void decorateDefaultVals(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            // If entitySpec has explicit config values, those override the 'defaults' in the SpecParameter.
            // Therefore in our type, we should report these as the 'defaults'.
            // This is particularly important for subtypes that set values for inherited config keys.
            // e.g. MySuperType declares required config 'maxSize', and MySubType sets a value for that in its
            //    `brooklyn.config` section. A user of `MySubType` in the UI would be forced to also set a value
            //    for 'maxSize' (because 'required' and no default) if we didn't do the code below.
            
            List<SpecParameter<?>> params = entitySpec.getParameters();
            Map<ConfigKey<?>, Object> configs = entitySpec.getConfig();
            
            List<SpecParameter<?>> decoratedParameters = new ArrayList<>();
            for (SpecParameter<?> param : params) {
                SpecParameter<?> decoratedParam;
                if (configs.containsKey(param.getConfigKey())) {
                    Object explicitConfigVal = configs.get(param.getConfigKey());
                    ConfigKey<?> newConfigKey = ConfigKeys.newConfigKeyWithDefault((ConfigKey)param.getConfigKey(), explicitConfigVal);
                    decoratedParam = new BasicSpecParameter(param.getLabel(), param.isPinned(), newConfigKey, param.getSensor());
                } else {
                    decoratedParam = param;
                }
                decoratedParameters.add(decoratedParam);
                
            }
            // but now we need to filter non-reinheritable, and merge where params are extended/overridden 
            entitySpec.parametersReplace(decoratedParameters);
        }
        
        @Override
        protected List<SpecParameter<?>> buildListOfTheseDecorationsFromIterable(Iterable<?> value) {
            // returns definitions, used only by #decorate method above
            return BasicSpecParameter.parseParameterDefinitionList(ImmutableList.copyOf(value), transformer, instantiator.loader);
        }

        @Override
        protected Object getDecorationAttributeJsonValue(ConfigBag attrs) {
            return attrs.getStringKey(BrooklynCampReservedKeys.BROOKLYN_PARAMETERS);
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<SpecParameter<?>> decorations) {
            throw new UnsupportedOperationException("SpecParameterResolver.addDecorationFromJsonMap should never be called.");
        }
    }

    public static class TagsResolver extends BrooklynEntityDecorationResolver<Iterable<Object>> {
        protected TagsResolver(BrooklynYamlTypeInstantiator.Factory instantiator) {
            super(instantiator);
        }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            Iterable<Object> decorationAttributeJsonValue = getDecorationAttributeJsonValue(attrs);
            if (decorationAttributeJsonValue != null) {
                entitySpec.tagsAdd(decorationAttributeJsonValue);
            }
            String iconUrl = attrs.get(BrooklynConfigKeys.ICON_URL);
            if (iconUrl!=null) {
                entitySpec.tagsAdd(MutableList.of(BrooklynTags.newIconUrlTag(iconUrl)));
            }
        }

        @Override
        protected String getDecorationKind() {
            return "Brooklyn Tags";
        }

        @Override
        protected Iterable<Object> getDecorationAttributeJsonValue(ConfigBag attrs) {
            Object brooklynTags = attrs.getStringKey(BrooklynCampReservedKeys.BROOKLYN_TAGS);
            if (brooklynTags == null) {
                return null;
            } else if (!(brooklynTags instanceof List)) {
                throw new IllegalArgumentException(BrooklynCampReservedKeys.BROOKLYN_TAGS + " should be a List of String elements. You supplied " + brooklynTags);
            } else {
                checkArgument(Iterables.all((List<?>) brooklynTags, new Predicate<Object>() {
                    @Override
                    public boolean apply(Object input) {
                        return !(input instanceof DeferredSupplier);
                    }
                }), BrooklynCampReservedKeys.BROOKLYN_TAGS + " should not contain DeferredSupplier. A DeferredSupplier is made when using $brooklyn:attributeWhenReady. You supplied " + brooklynTags);
                @SuppressWarnings("unchecked")
                List<Object> result = (List<Object>)brooklynTags;
                return result;
            }
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<Iterable<Object>> decorations) {
            throw new UnsupportedOperationException("TagsResolver.addDecorationFromJsonMap should never be called.");
        }
    }
}
