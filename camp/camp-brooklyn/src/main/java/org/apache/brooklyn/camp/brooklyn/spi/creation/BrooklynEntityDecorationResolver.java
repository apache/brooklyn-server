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

import com.google.common.reflect.TypeToken;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampReservedKeys;
import org.apache.brooklyn.camp.brooklyn.spi.creation.BrooklynYamlTypeInstantiator.Factory;
import org.apache.brooklyn.camp.brooklyn.spi.creation.BrooklynYamlTypeInstantiator.InstantiatorFromKey;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.objs.BasicSpecParameter;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pattern for resolving "decorations" on service specs / entity specs, such as policies, enrichers, etc.
 *
 * @since 0.7.0
 */
public abstract class BrooklynEntityDecorationResolver<DT> {

    private static final Logger log = LoggerFactory.getLogger(BrooklynEntityDecorationResolver.class);

    final BrooklynYamlTypeInstantiator.Factory instantiator;
    final String decorationKind;
    final String typeKeyPrefix;
    final String objectKey;

    protected BrooklynEntityDecorationResolver(Factory instantiator, String decorationKind, String typeKeyPrefix, String objectKey) {
        this.instantiator = instantiator;
        this.decorationKind = decorationKind;
        this.typeKeyPrefix = typeKeyPrefix;
        this.objectKey = objectKey;
    }

    /** @deprecated since 0.10.0 - use {@link #decorate(EntitySpec, ConfigBag, Set)} */
    @Deprecated
    public final void decorate(EntitySpec<?> entitySpec, ConfigBag attrs) {
        decorate(entitySpec, attrs, ImmutableSet.of());
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

    protected final String getDecorationKind() { return decorationKind; }

    protected Object getDecorationAttributeJsonValue(ConfigBag attrs) {
        return attrs.getStringKey(objectKey);
    }

    protected abstract void addDecorationFromJsonMap(Map<?,?> decorationJson, List<DT> decorations);

    public abstract static class BrooklynObjectSpecResolver<DTInterface extends BrooklynObject,DTSpec extends AbstractBrooklynObjectSpec<DTInterface,DTSpec>> extends BrooklynEntityDecorationResolver<DTSpec> {
        final BrooklynObjectType boType;

        protected BrooklynObjectSpecResolver(Factory instantiator, String decorationKind, String typeKeyPrefix, String objectKey, BrooklynObjectType boType) {
            super(instantiator, decorationKind, typeKeyPrefix, objectKey);
            this.boType = boType;
        }

        protected DTSpec instantiateSpec(Map<?, ?> decorationJson, Function<Class<DTInterface>,DTSpec> classFactory) {
            InstantiatorFromKey decoLoader = instantiator.from(decorationJson).prefix(typeKeyPrefix);

            String typeName = decoLoader.getTypeName().get();
            ManagementContext mgmt = instantiator.loader.getManagementContext();

            Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt.getTypeRegistry().get(typeName), RegisteredTypeLoadingContexts.spec(boType.getInterfaceType()));
            DTSpec spec;
            if (!item.isNull()) {
                // throw error if absent for any reason other than null
                spec = mgmt.getTypeRegistry().createSpec(item.get(), null, (Class<DTSpec>) boType.getSpecType());
            } else {
                Class<? extends BrooklynObject> type = decoLoader.getType(boType.getInterfaceType());
                spec = classFactory.apply((Class<DTInterface>)type).parameters(BasicSpecParameter.fromClass(mgmt, type));
            }
            spec.configure(decoLoader.getConfigMap());
            return spec;
        }

        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<DTSpec> decorations, Function<Class<DTInterface>,DTSpec> classFactory) {
            decorations.add(instantiateSpec(decorationJson, classFactory));
        }
    }

    public static class PolicySpecResolver extends BrooklynObjectSpecResolver<Policy,PolicySpec<Policy>> {

        public PolicySpecResolver(BrooklynYamlTypeInstantiator.Factory initializer) { super(initializer, "Policy", "policy", BrooklynCampReservedKeys.BROOKLYN_POLICIES, BrooklynObjectType.POLICY); }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            entitySpec.policySpecs(buildListOfTheseDecorationsFromEntityAttributes(attrs));
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<PolicySpec<Policy>> decorations) {
            addDecorationFromJsonMap(decorationJson, decorations, type -> PolicySpec.create(type));
        }
    }

    public static class EnricherSpecResolver extends BrooklynObjectSpecResolver<Enricher,EnricherSpec<Enricher>> {

        public EnricherSpecResolver(BrooklynYamlTypeInstantiator.Factory initializer) { super(initializer, "Enricher", "enricher", BrooklynCampReservedKeys.BROOKLYN_ENRICHERS, BrooklynObjectType.ENRICHER); }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            entitySpec.enricherSpecs(buildListOfTheseDecorationsFromEntityAttributes(attrs));
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<EnricherSpec<Enricher>> decorations) {
            addDecorationFromJsonMap(decorationJson, decorations, type -> EnricherSpec.create(type));
        }
    }

    public static class InitializerResolver extends BrooklynEntityDecorationResolver<EntityInitializer> {

        public InitializerResolver(BrooklynYamlTypeInstantiator.Factory instantiator) { super(instantiator, "Entity initializer", "initializer", BrooklynCampReservedKeys.BROOKLYN_INITIALIZERS); }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            entitySpec.addInitializers(buildListOfTheseDecorationsFromEntityAttributes(attrs));
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<EntityInitializer> decorations) {
            EntityInitializer result;
            try {
                result = BeanWithTypeUtils.convert(instantiator.getClassLoadingContext().getManagementContext(), decorationJson, TypeToken.of(EntityInitializer.class),
                        true, instantiator.getClassLoadingContext(), true);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                // fall back to the old way, eg if caller specifies initializerType, or for some other reason bean-with-type fails
                Object type = decorationJson.get("type");
                try {
                    result = instantiator.from(decorationJson).prefix(typeKeyPrefix).newInstance(EntityInitializer.class);
                    if (type!=null) {
                        // make a note of this because the new syntax should do everything the old one does except if the type is specified as 'initializerType'
                        log.debug("Initializer for type '"+type+"' instantiated via old syntax (due to "+e+")");
                    }
                } catch (Exception e2) {
                    Exceptions.propagateIfFatal(e2);
                    throw Exceptions.propagate("Error instantiating " + typeKeyPrefix +
                            (type!=null ? " '"+type+"'" : "") +
                            " (stack "+Arrays.asList(CampResolver.currentlyCreatingSpec.get())+")", Arrays.asList(e, e2));
                }
            }
            decorations.add(result);
        }
    }

    // Not much value from extending from BrooklynEntityDecorationResolver, but let's not break the convention
    public static class SpecParameterResolver extends BrooklynEntityDecorationResolver<SpecParameter<?>> {

        private Function<Object, Object> transformer;

        protected SpecParameterResolver(BrooklynYamlTypeInstantiator.Factory instantiator) { super(instantiator, "Spec Parameter initializer", null, BrooklynCampReservedKeys.BROOKLYN_PARAMETERS); }

        @Override
        public void decorate(EntitySpec<?> entitySpec, ConfigBag attrs, Set<String> encounteredRegisteredTypeIds) {
            transformer = new BrooklynComponentTemplateResolver.SpecialFlagsTransformer(instantiator.loader, encounteredRegisteredTypeIds);
            // entitySpec is the parent
            List<? extends SpecParameter<?>> explicitParams = buildListOfTheseDecorationsFromEntityAttributes(attrs);
            BasicSpecParameter.initializeSpecWithExplicitParameters(entitySpec, explicitParams, instantiator.loader);
        }

        @Override
        protected List<SpecParameter<?>> buildListOfTheseDecorationsFromIterable(Iterable<?> value) {
            // returns definitions, used only by #decorate method above
            return BasicSpecParameter.parseParameterDefinitionList(ImmutableList.copyOf(value), transformer, instantiator.loader);
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<SpecParameter<?>> decorations) {
            throw new UnsupportedOperationException("SpecParameterResolver.addDecorationFromJsonMap should never be called.");
        }
    }

    public static class TagsResolver extends BrooklynEntityDecorationResolver<Iterable<Object>> {
        protected TagsResolver(BrooklynYamlTypeInstantiator.Factory instantiator) {
            super(instantiator, "Brooklyn Tags", null, BrooklynCampReservedKeys.BROOKLYN_TAGS);
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
        protected Iterable<Object> getDecorationAttributeJsonValue(ConfigBag attrs) {
            Object brooklynTags = super.getDecorationAttributeJsonValue(attrs);
            if (brooklynTags == null) {
                return null;
            } else if (!(brooklynTags instanceof List)) {
                throw new IllegalArgumentException(BrooklynCampReservedKeys.BROOKLYN_TAGS + " should be a List of String elements. You supplied " + brooklynTags);
            } else {
                checkArgument(((List<?>) brooklynTags).stream().noneMatch(input -> input instanceof DeferredSupplier),
                        BrooklynCampReservedKeys.BROOKLYN_TAGS + " should not contain DeferredSupplier. A DeferredSupplier is made when using $brooklyn:attributeWhenReady. You supplied " + brooklynTags);
                return (List<Object>)brooklynTags;
            }
        }

        @Override
        protected void addDecorationFromJsonMap(Map<?, ?> decorationJson, List<Iterable<Object>> decorations) {
            throw new UnsupportedOperationException("TagsResolver.addDecorationFromJsonMap should never be called.");
        }
    }
}
