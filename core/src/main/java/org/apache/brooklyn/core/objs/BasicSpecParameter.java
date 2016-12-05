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
package org.apache.brooklyn.core.objs;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.catalog.CatalogConfig;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynType;
import org.apache.brooklyn.api.objs.SpecParameter;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.config.BasicConfigKey.Builder;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.sensor.PortAttributeSensorAndConfigKey;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

public class BasicSpecParameter<T> implements SpecParameter<T>{
    private static final long serialVersionUID = -4728186276307619778L;

    private final String label;

    /** pinning may become a priority or other more expansive indicator */
    @Beta
    private final boolean pinned;

    private final ConfigKey<T> configKey;
    private final AttributeSensor<?> sensor;

    // For backwards compatibility of persisted state.
    // Automatically called by xstream (which is used under the covers by XmlMementoSerializer).
    // Required for those who have state from a version between
    // 29th October 2015 and 21st January 2016 (when this class was introduced, and then when it was changed).
    /** @deprecated since 0.9.0 can be removed, along with readResolve, when field not used any further */
    private ConfigKey<T> type;
    private Object readResolve() {
        if (type != null && configKey == null) {
            return new BasicSpecParameter<T>(label, pinned, type, sensor);
        } else {
            return this;
        }
    }

    @Beta
    public BasicSpecParameter(String label, boolean pinned, ConfigKey<T> config) {
        this(label, pinned, config, null);
    }

    @Beta // TBD whether "sensor" stay
    public <SensorType> BasicSpecParameter(String label, boolean pinned, ConfigKey<T> config, AttributeSensor<SensorType> sensor) {
        this.label = label;
        this.pinned = pinned;
        this.configKey = config;
        this.sensor = sensor;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public boolean isPinned() {
        return pinned;
    }

    @Override
    public ConfigKey<T> getConfigKey() {
        return configKey;
    }

    @Override
    public AttributeSensor<?> getSensor() {
        return sensor;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(configKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BasicSpecParameter<?> other = (BasicSpecParameter<?>) obj;
        return Objects.equal(configKey, other.configKey);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("label", label)
                .add("pinned", pinned)
                .add("type", configKey)
                .toString();
    }

    // Not CAMP specific since it's used to get the parameters from catalog items meta which are syntax independent.
    public static List<SpecParameter<?>> fromConfigList(List<?> obj, Function<Object, Object> specialFlagsTransformer, BrooklynClassLoadingContext loader) {
        return ParseYamlInputs.parseParameters(obj, specialFlagsTransformer, loader);
    }

    public static List<SpecParameter<?>> fromClass(ManagementContext mgmt, Class<?> type) {
        return ParseClassParameters.parseParameters(getImplementedBy(mgmt, type));
    }

    public static List<SpecParameter<?>> fromSpec(ManagementContext mgmt, AbstractBrooklynObjectSpec<?, ?> spec) {
        if (!spec.getParameters().isEmpty()) {
            return spec.getParameters();
        }
        Class<?> type = null;
        if (spec instanceof EntitySpec) {
            EntitySpec<?> entitySpec = (EntitySpec<?>)spec;
            if (entitySpec.getImplementation() != null) {
                type = entitySpec.getImplementation();
            }
        }
        if (type == null) {
            type = getImplementedBy(mgmt, spec.getType());
        }
        return ParseClassParameters.parseParameters(getImplementedBy(mgmt, type));
    }

    private static Class<?> getImplementedBy(ManagementContext mgmt, Class<?> type) {
        if (Entity.class.isAssignableFrom(type) && type.isInterface()) {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Entity> uncheckedType = ParseClassParameters.getEntityImplementedBy(mgmt, (Class<Entity>)type);
                return uncheckedType;
            } catch(IllegalArgumentException e) {
                // NOP, no implementation for type, use whatever we have
            }
        }
        return type;
    }

    private static final class ParseYamlInputs {
        private static final String DEFAULT_TYPE = "string";
        private static final Map<String, Class<?>> BUILT_IN_TYPES = ImmutableMap.<String, Class<?>>builder()
                .put(DEFAULT_TYPE, String.class)
                .put("bool", Boolean.class)
                .put("boolean", Boolean.class)
                .put("byte", Byte.class)
                .put("char", Character.class)
                .put("character", Character.class)
                .put("short", Short.class)
                .put("integer", Integer.class)
                .put("int", Integer.class)
                .put("long", Long.class)
                .put("float", Float.class)
                .put("double", Double.class)
                .put("duration", Duration.class)
                .put("timestamp", Date.class)
                .put("port", PortRange.class)
                .build();

        private static final Map<String, Predicate<?>> BUILT_IN_CONSTRAINTS = ImmutableMap.<String, Predicate<?>>of(
                "required", StringPredicates.isNonBlank());

        public static List<SpecParameter<?>> parseParameters(List<?> inputsRaw, Function<Object, Object> specialFlagTransformer, BrooklynClassLoadingContext loader) {
            if (inputsRaw == null) return ImmutableList.of();
            List<SpecParameter<?>> inputs = new ArrayList<>(inputsRaw.size());
            for (Object obj : inputsRaw) {
                inputs.add(parseParameter(obj, specialFlagTransformer, loader));
            }
            return inputs;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static SpecParameter<?> parseParameter(Object obj, Function<Object, Object> specialFlagTransformer, BrooklynClassLoadingContext loader) {
            Map inputDef;
            if (obj instanceof String) {
                inputDef = ImmutableMap.of("name", obj);
            } else if (obj instanceof Map) {
                inputDef = (Map) obj;
            } else {
                throw new IllegalArgumentException("Catalog input definition expected to be a map, but is " + obj.getClass() + " instead: " + obj);
            }
            String name = (String)inputDef.get("name");
            String label = (String)inputDef.get("label");
            String description = (String)inputDef.get("description");
            String type = (String)inputDef.get("type");
            Boolean pinned = (Boolean)inputDef.get("pinned");
            boolean hasDefaultValue = inputDef.containsKey("default");
            Object defaultValue = inputDef.get("default");
            if (specialFlagTransformer != null) {
                defaultValue = specialFlagTransformer.apply(defaultValue);
            }
            boolean hasConstraints = inputDef.containsKey("constraints");
            Predicate<?> constraint = parseConstraints(inputDef.get("constraints"), loader);

            ConfigInheritance runtimeInheritance;
            boolean hasRuntimeInheritance;
            if (inputDef.containsKey("inheritance.runtime")) {
                hasRuntimeInheritance = true;
                runtimeInheritance = parseInheritance(inputDef.get("inheritance.runtime"), loader);
            } else if (inputDef.containsKey("inheritance.parent")) {
                // this alias will be deprecated
                hasRuntimeInheritance = true;
                runtimeInheritance = parseInheritance(inputDef.get("inheritance.parent"), loader);
            } else {
                hasRuntimeInheritance = false;
                runtimeInheritance = parseInheritance(null, loader);
            }

            boolean hasTypeInheritance = inputDef.containsKey("inheritance.type");
            ConfigInheritance typeInheritance = parseInheritance(inputDef.get("inheritance.type"), loader);

            if (name == null) {
                throw new IllegalArgumentException("'name' value missing from input definition " + obj + " but is required. Check for typos.");
            }

            ConfigKey configType;
            AttributeSensor sensorType = null;

            boolean hasType = type!=null;

            TypeToken typeToken = inferType(type, loader);
            Builder builder = BasicConfigKey.builder(typeToken)
                    .name(name)
                    .description(description)
                    .defaultValue(defaultValue)
                    .constraint(constraint)
                    .runtimeInheritance(runtimeInheritance)
                    .typeInheritance(typeInheritance);

            if (PortRange.class.equals(typeToken.getRawType())) {
                sensorType = new PortAttributeSensorAndConfigKey(builder);
                configType = ((HasConfigKey)sensorType).getConfigKey();
            } else {
                configType = builder.build();
            }

            return new SpecParameterForInheritance(label, pinned, configType, sensorType,
                    hasType, hasDefaultValue, hasConstraints, hasRuntimeInheritance, hasTypeInheritance);
        }

        @SuppressWarnings({ "rawtypes" })
        private static TypeToken inferType(String typeRaw, BrooklynClassLoadingContext loader) {
            if (typeRaw == null) return TypeToken.of(String.class);
            String type = typeRaw.trim();
            if (BUILT_IN_TYPES.containsKey(type.toLowerCase())) {
                return TypeToken.of(BUILT_IN_TYPES.get(type.toLowerCase()));
            } else {
                // Assume it's a Java type
                Maybe<Class<?>> inputType = loader.tryLoadClass(type);
                if (inputType.isPresent()) {
                    return TypeToken.of(inputType.get());
                } else {
                    throw new IllegalArgumentException("The type '" + type + "' for a catalog input not recognised as a built-in (" + BUILT_IN_TYPES.keySet() + ") or a java type");
                }
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static Predicate parseConstraints(Object obj, BrooklynClassLoadingContext loader) {
            List constraintsRaw;
            if (obj == null) {
                constraintsRaw = ImmutableList.of();
            } else if (obj instanceof String) {
                constraintsRaw = ImmutableList.of(obj);
            } else if (obj instanceof List) {
                constraintsRaw = (List) obj;
            } else {
                throw new IllegalArgumentException ("The constraint '" + obj + "' for a catalog input is invalid format - string or list supported");
            }
            List<Predicate> constraints = new ArrayList(constraintsRaw.size());
            for (Object untypedConstraint : constraintsRaw) {
                String constraint = (String)untypedConstraint;
                if (BUILT_IN_CONSTRAINTS.containsKey(constraint)) {
                    constraints.add(BUILT_IN_CONSTRAINTS.get(constraint));
                } else {
                    throw new IllegalArgumentException("The constraint '" + constraint + "' for a catalog input is not recognized as a built-in (" + BUILT_IN_CONSTRAINTS.keySet() + ")");
                }
            }
            if (!constraints.isEmpty()) {
                if (constraints.size() == 1) {
                    return constraints.get(0);
                } else {
                    return Predicates.and((List<Predicate<Object>>)(List) constraints);
                }
            } else {
                return Predicates.alwaysTrue();
            }
        }

        private static ConfigInheritance parseInheritance(Object obj, BrooklynClassLoadingContext loader) {
            if (obj == null || obj instanceof String) {
                return BasicConfigInheritance.fromString((String)obj);
            } else {
                throw new IllegalArgumentException ("The config-inheritance '" + obj + "' for a catalog input is invalid format - string supported");
            }
        }
    }

    private static final class ParseClassParameters {
        private static final class WeightedParameter {
            private Double weight;
            private SpecParameter<?> input;
            public WeightedParameter(Double weight, SpecParameter<?> input) {
                this.weight = weight;
                this.input = input;
            }
            public Double getWeight() {return weight; }
            public SpecParameter<?> getInput() { return input; }
        }
        private static final class WeightedParameterComparator implements Comparator<WeightedParameter> {
            @Override
            public int compare(WeightedParameter o1, WeightedParameter o2) {
                if (o1.getWeight() == null && o2.getWeight() == null) {
                    return o1.getInput().getLabel().compareTo(o2.getInput().getLabel());
                } else if (o1.getWeight() == null) {
                    return 1;
                } else if (o2.getWeight() == null) {
                    return -1;
                } else {
                    return -Double.compare(o1.getWeight(),  o2.getWeight());
                }
            }
        }
        private static final class SpecParameterTransformer implements Function<WeightedParameter, SpecParameter<?>> {
            @Override
            public SpecParameter<?> apply(WeightedParameter input) {
                return input.getInput();
            }
        }

        public static List<SpecParameter<?>> parseParameters(Class<?> c) {
            MutableList<WeightedParameter> parameters = MutableList.<WeightedParameter>of();
            if (BrooklynObject.class.isAssignableFrom(c)) {
                @SuppressWarnings("unchecked")
                Class<? extends BrooklynObject> brooklynClass = (Class<? extends BrooklynObject>) c;
                BrooklynDynamicType<?, ?> dynamicType = BrooklynTypes.getDefinedBrooklynType(brooklynClass);
                BrooklynType type = dynamicType.getSnapshot();
                for (ConfigKey<?> x: type.getConfigKeys()) {
                    WeightedParameter fieldConfig = getFieldConfig(x, dynamicType.getConfigKeyField(x.getName()));
                    parameters.appendIfNotNull(fieldConfig);
                }
                Collections.sort(parameters, new WeightedParameterComparator());
                return FluentIterable.from(parameters)
                        .transform(new SpecParameterTransformer()).toList();
            } else {
                return ImmutableList.of();
            }
        }

        public static WeightedParameter getFieldConfig(ConfigKey<?> config, Field configKeyField) {
            if (configKeyField == null) return null;
            CatalogConfig catalogConfig = configKeyField.getAnnotation(CatalogConfig.class);
            String label = config.getName();
            Double priority = null;
            Boolean pinned = Boolean.FALSE;
            if (catalogConfig != null) {
                label = Maybe.fromNullable(catalogConfig.label()).or(config.getName());
                priority = catalogConfig.priority();
                pinned = catalogConfig.pinned();
            }
            @SuppressWarnings({ "unchecked", "rawtypes" })
            SpecParameter<?> param = new BasicSpecParameter(label, pinned, config);
            return new WeightedParameter(priority, param);
        }

        private static <T extends Entity> Class<? extends T> getEntityImplementedBy(ManagementContext mgmt, Class<T> type) {
            return mgmt.getEntityManager().getEntityTypeRegistry().getImplementedBy(type);
        }
    }

    /**
     * Adds the given list of {@link SpecParameter parameters} to the provided
     * {@link AbstractBrooklynObjectSpec spec} or generates a list from the
     * spec if the provided list is empty.
     *
     * @see EntitySpec#parameters(List)
     */
    public static void addParameters(AbstractBrooklynObjectSpec<?, ?> spec, List<? extends SpecParameter<?>> explicitParams, BrooklynClassLoadingContext loader) {
        if (spec.getParameters().isEmpty()) {
            spec.parametersAdd(BasicSpecParameter.fromSpec(loader.getManagementContext(), spec));
        }
        if (explicitParams.size() > 0) {
            spec.parametersReplace(resolveParameters(explicitParams, spec.getParameters(), spec));
        }
    }

    /** merge parameters against other parameters and known and type-inherited config keys */
    static Collection<SpecParameter<?>> resolveParameters(Collection<? extends SpecParameter<?>> newParams, Collection<? extends SpecParameter<?>> existingReferenceParamsToKeep, AbstractBrooklynObjectSpec<?,?> spec) {
        Map<String,SpecParameter<?>> existingToKeep = MutableMap.of();
        if (existingReferenceParamsToKeep!=null) {
            for (SpecParameter<?> p: existingReferenceParamsToKeep) { 
                existingToKeep.put(p.getConfigKey().getName(), p);
            }
        }

        List<SpecParameter<?>> result = MutableList.<SpecParameter<?>>of();
        for (SpecParameter<?> p: newParams) {
            if (p instanceof SpecParameterForInheritance) {
                SpecParameter<?> existingP = existingToKeep.remove(p.getConfigKey().getName());
                if (existingP!=null) {
                    p = ((SpecParameterForInheritance<?>)p).resolveWithAncestor(existingP);
                } else {
                    // TODO find config keys on the type (not available as parameters)
                    /* we don't currently do this due to low priority; all it means if there is a config key in java,
                     * and a user wishes to expose it as a parameter, they have to redeclare everything;
                     * nothing from the config key in java will be inherited */
                    p = ((SpecParameterForInheritance<?>)p).resolveWithAncestor((ConfigKey<?>)null);
                }
                result.add(p);
            } else {
                existingToKeep.remove(p.getConfigKey().getName());
                result.add(p);
            }
        }
        result.addAll(existingToKeep.values());
        return result;
    }

    /** instance of {@link SpecParameter} which includes information about what fields are explicitly set,
     * for use with a subsequent merge */
    @SuppressWarnings("serial")
    @Beta
    static class SpecParameterForInheritance<T> extends BasicSpecParameter<T> {

        private final boolean hasType, hasLabelSet, hasPinnedSet, hasDefaultValue, hasConstraints, hasRuntimeInheritance, hasTypeInheritance;

        private <SensorType> SpecParameterForInheritance(String label, Boolean pinned, ConfigKey<T> config, AttributeSensor<SensorType> sensor,
                boolean hasType, boolean hasDefaultValue, boolean hasConstraints, boolean hasRuntimeInheritance, boolean hasTypeInheritance) {
            super(Preconditions.checkNotNull(label!=null ? label : config.getName(), "label or config name must be set"), 
                    pinned==null ? true : pinned, config, sensor);
            this.hasType = hasType;
            this.hasLabelSet = label!=null;
            this.hasPinnedSet = pinned!=null;
            this.hasDefaultValue = hasDefaultValue;
            this.hasConstraints = hasConstraints;
            this.hasRuntimeInheritance = hasRuntimeInheritance;
            this.hasTypeInheritance = hasTypeInheritance;
        }

        /** used if yaml specifies a parameter, but possibly incomplete, and a spec supertype has a parameter */
        SpecParameter<?> resolveWithAncestor(SpecParameter<?> ancestor) {
            if (ancestor==null) return new BasicSpecParameter<>(getLabel(), isPinned(), getConfigKey(), getSensor());

            return new BasicSpecParameter<>(
                    hasLabelSet ? getLabel() : ancestor.getLabel(), 
                            hasPinnedSet ? isPinned() : ancestor.isPinned(), 
                                    resolveWithAncestorConfigKey(ancestor.getConfigKey()), 
                                    hasType ? getSensor() : ancestor.getSensor());
        }

        /** as {@link #resolveWithAncestor(SpecParameter)} but where the param redefines/extends a config key coming from a java supertype, rather than a parameter */
        // TODO not used yet; see calls to the other resolveWithAncestor method,
        // and see BrooklynComponentTemplateResolver.findAllConfigKeys;
        // also note whilst it is easiest to do this here, logically it is messy,
        // and arguably it should be done when converting the spec to an instance
        SpecParameter<?> resolveWithAncestor(ConfigKey<?> ancestor) {
            if (ancestor==null) return new BasicSpecParameter<>(getLabel(), isPinned(), getConfigKey(), getSensor());

            // TODO probably want to do this (but it could get expensive!)
            //          Set<Class<?>> types = MutableSet.<Class<?>>builder()
            //                  .add(spec.getImplementation())
            //                  .add(spec.getType())
            //                  .addAll(spec.getAdditionalInterfaces())
            //                  .remove(null)
            //                  .build();
            //          // order above is important, respected below to take the first one defined 
            //          MutableMap<String, ConfigKey<?>> result = MutableMap.copyOf(FlagUtils.findAllConfigKeys(null, types));

            return new BasicSpecParameter<>(
                    getLabel(), 
                    isPinned(), 
                    resolveWithAncestorConfigKey(ancestor),
                    // TODO port sensor will be lost (see messy code above which sets the port sensor)
                    getSensor());
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private ConfigKey<?> resolveWithAncestorConfigKey(ConfigKey<?> ancestor) {
            ConfigKey<?> dominant = getConfigKey();
            return ConfigKeys.<Object>builder((TypeToken)(hasType ? dominant : ancestor).getTypeToken())
                    .name(dominant.getName())
                    .description((dominant.getDescription()!=null ? dominant : ancestor).getDescription())
                    .defaultValue((hasDefaultValue ? dominant : ancestor).getDefaultValue())
                    .constraint((Predicate<Object>) (hasConstraints ? dominant : ancestor).getConstraint())
                    .runtimeInheritance( (hasRuntimeInheritance ? dominant : ancestor).getInheritanceByContext(InheritanceContext.RUNTIME_MANAGEMENT))
                    .typeInheritance( (hasTypeInheritance ? dominant : ancestor).getInheritanceByContext(InheritanceContext.TYPE_DEFINITION))
                    // not yet configurable from yaml
                    //.reconfigurable()
                    .build();
        }

        public boolean isHasDefaultValue() {
            return hasDefaultValue;
        }
        public boolean isHasConstraints() {
            return hasConstraints;
        }
        public boolean isHasRuntimeInheritance() {
            return hasRuntimeInheritance;
        }
        public boolean isHasTypeInheritance() {
            return hasTypeInheritance;
        }
    }

}
