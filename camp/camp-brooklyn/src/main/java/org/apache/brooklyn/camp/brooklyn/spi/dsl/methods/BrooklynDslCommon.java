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
package org.apache.brooklyn.camp.brooklyn.spi.dsl.methods;

import com.fasterxml.jackson.annotation.JsonIgnore;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import java.util.*;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslUtils;
import static org.apache.brooklyn.camp.brooklyn.spi.dsl.DslUtils.resolved;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampReservedKeys;
import org.apache.brooklyn.camp.brooklyn.spi.creation.BrooklynYamlTypeInstantiator;
import org.apache.brooklyn.camp.brooklyn.spi.creation.EntitySpecConfiguration;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslAccessible;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent.Scope;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.external.ExternalConfigSupplier;
import org.apache.brooklyn.core.entity.EntityDynamicType;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.classloading.OsgiBrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.internal.ExternalConfigSupplierRegistry;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.persist.DeserializingClassRenamesProvider;
import org.apache.brooklyn.core.objs.AbstractConfigurationSupportInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.xstream.ObjectWithDefaultStringImplConverter;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercer;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.thoughtworks.xstream.annotations.XStreamConverter;

/**
 * static import functions which can be used in `$brooklyn:xxx` contexts
 * WARNING: Don't overload methods - the DSL evaluator will pick any one that matches, not the best match.
 */
public class BrooklynDslCommon {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynDslCommon.class);

    public static final String PREFIX = "$brooklyn:";

    public static void registerSerializationHooks() {
        BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings.BROOKLYN_PARSE_DSL_FUNCTION = DslUtils::parseBrooklynDsl;
    }
    static {
        registerSerializationHooks();
    }
    
    // Access specific entities

    @DslAccessible
    public static DslComponent self() {
        return new DslComponent(Scope.THIS);
    }
    @DslAccessible
    public static DslComponent entity(Object id) {
        return DslComponent.newInstance(Scope.GLOBAL, id);
    }
    @DslAccessible
    public static DslComponent parent() {
        return new DslComponent(Scope.PARENT);
    }
    @DslAccessible
    public static DslComponent child(Object id) {
        return DslComponent.newInstance(Scope.CHILD, id);
    }
    @DslAccessible
    public static DslComponent sibling(Object id) {
        return DslComponent.newInstance(Scope.SIBLING, id);
    }
    @DslAccessible
    public static DslComponent descendant(Object id) {
        return DslComponent.newInstance(Scope.DESCENDANT, id);
    }
    @DslAccessible
    public static DslComponent ancestor(Object id) {
        return DslComponent.newInstance(Scope.ANCESTOR, id);
    }
    @DslAccessible
    public static DslComponent root() {
        return new DslComponent(Scope.ROOT);
    }
    @DslAccessible
    public static DslComponent scopeRoot() {
        return new DslComponent(Scope.SCOPE_ROOT);
    }
    // prefer the syntax above to the below now, but not deprecating the below
    @DslAccessible
    public static DslComponent component(Object id) {
        return component("global", id);
    }
    @DslAccessible
    public static DslComponent component(String scope, Object id) {
        if (!DslComponent.Scope.isValid(scope)) {
            throw new IllegalArgumentException(scope + " is not a valid scope");
        }
        return DslComponent.newInstance(DslComponent.Scope.fromString(scope), id);
    }

    // Access things on entities

    @DslAccessible
    public static BrooklynDslDeferredSupplier<?> config(Object keyName) {
        return new DslComponent(Scope.THIS, "").config(keyName);
    }

    @DslAccessible
    public static BrooklynDslDeferredSupplier<?> config(BrooklynObjectInternal obj, Object keyName) {
        return new DslBrooklynObjectConfigSupplier(obj, keyName);
    }

    public static class DslBrooklynObjectConfigSupplier extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = -2378555915585603381L;

        // Keep in mind this object gets serialized so is the following reference
        private BrooklynObjectInternal obj;
        @XStreamConverter(ObjectWithDefaultStringImplConverter.class)
        private Object keyName;

        public DslBrooklynObjectConfigSupplier(BrooklynObjectInternal obj, Object keyName) {
            checkNotNull(obj, "obj");
            checkNotNull(keyName, "keyName");

            this.obj = obj;
            this.keyName = keyName;
        }

        protected String resolveKeyName(boolean immediately) {
            if (keyName instanceof String) {
                return (String)keyName;
            }
            
            return Tasks.resolving(keyName)
                .as(String.class)
                .context(DslComponent.findExecutionContext(this))
                .immediately(immediately)
                .description("Resolving key name from " + keyName)
                .get();
        }

        @Override @JsonIgnore
        public Maybe<Object> getImmediately() {
            if (obj instanceof Entity) {
                // Shouldn't worry too much about it since DSL can fetch objects from same app only.
                // Just in case check whether it's same app for entities.
                checkState(entity().getApplicationId().equals(((Entity)obj).getApplicationId()));
            }
            String keyNameS = resolveKeyName(true);
            ConfigKey<Object> key = ConfigKeys.newConfigKey(Object.class, keyNameS);
            Maybe<? extends Object> result = ((AbstractConfigurationSupportInternal)obj.config()).getNonBlocking(key);
            return Maybe.<Object>cast(result);
        }

        @Override
        public Task<Object> newTask() {
            return Tasks.builder()
                    .displayName("retrieving config for "+keyName+" on "+obj)
                    .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                    .dynamic(false)
                    .body(new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            String keyNameS = resolveKeyName(true);
                            ConfigKey<Object> key = ConfigKeys.newConfigKey(Object.class, keyNameS);
                            return obj.getConfig(key);
                        }})
                    .build();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(obj, keyName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslBrooklynObjectConfigSupplier that = DslBrooklynObjectConfigSupplier.class.cast(obj);
            return Objects.equal(this.obj, that.obj) &&
                    Objects.equal(this.keyName, that.keyName);
        }

        @Override
        public String toString() {
            return DslToStringHelpers.concat(DslToStringHelpers.internal(obj), ".", DslToStringHelpers.fn("config", keyName));
        }
    }

    @DslAccessible
    public static BrooklynDslDeferredSupplier<?> attributeWhenReady(Object sensorName) {
        return new DslComponent(Scope.THIS, "").attributeWhenReady(sensorName);
    }

    @DslAccessible
    public static BrooklynDslDeferredSupplier<?> entityId() {
        return new DslComponent(Scope.THIS, "").entityId();
    }

    /** Returns a {@link Sensor}, looking up the sensor on the context if available and using that,
     * or else defining an untyped (Object) sensor */
    @DslAccessible
    public static BrooklynDslDeferredSupplier<Sensor<?>> sensor(Object sensorName) {
        return new DslComponent(Scope.THIS, "").sensor(sensorName);
    }
    
    /** Returns a {@link Sensor} declared on the type (e.g. entity class) declared in the first argument. */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @DslAccessible
    public static Sensor<?> sensor(String clazzName, String sensorName) {
        try {
            // TODO Should use catalog's classloader, rather than ClassLoaderUtils; how to get that? Should we return a future?!
            //      Should have the catalog's loader at this point in a thread local
            String mappedClazzName = DeserializingClassRenamesProvider.INSTANCE.findMappedName(clazzName);
            Class<?> clazz = new ClassLoaderUtils(BrooklynDslCommon.class).loadClass(mappedClazzName);
            
            Sensor<?> sensor;
            if (Entity.class.isAssignableFrom(clazz)) {
                sensor = new EntityDynamicType((Class<? extends Entity>) clazz).getSensor(sensorName);
            } else {
                // Some non-entity classes (e.g. ServiceRestarter policy) declare sensors that other
                // entities/policies/enrichers may wish to reference.
                Map<String,Sensor<?>> sensors = EntityDynamicType.findSensors((Class)clazz, null);
                sensor = sensors.get(sensorName);
            }
            if (sensor == null) {
                // TODO could extend API to return a sensor of the given type; useful but makes API ambiguous in theory (unlikely in practise, but still...)
                throw new IllegalArgumentException("Sensor " + sensorName + " not found on class " + clazzName);
            }
            return sensor;
        } catch (ClassNotFoundException e) {
            throw Exceptions.propagate(e);
        }
    }

    @DslAccessible
    public static BrooklynDslDeferredSupplier<?> location() {
        return new DslComponent(Scope.THIS, "").location();
    }

    @DslAccessible
    public static BrooklynDslDeferredSupplier<?> location(Object index) {
        return new DslComponent(Scope.THIS, "").location(index);
    }

    // Build complex things

    // TODO allow entitySpec to take string, parsed as YAML, and if just a string that's taken as the type
    
    @DslAccessible
    public static EntitySpecConfiguration entitySpec(Map<String, Object> arguments) {
        return new EntitySpecConfiguration(arguments);
    }

    /**
     * Return an instance of the specified class with its fields set according
     * to the {@link Map}. Or a {@link BrooklynDslDeferredSupplier} if either the arguments are 
     * not yet fully resolved, or the class cannot be loaded yet (e.g. needs the catalog's OSGi 
     * bundles).
     */
    @SuppressWarnings("unchecked")
    @DslAccessible
    public static Object object(Map<String, Object> arguments) {
        ConfigBag config = ConfigBag.newInstance(arguments);
        String typeName = BrooklynYamlTypeInstantiator.InstantiatorFromKey.extractTypeName("object", config).orNull();
        List<Object> constructorArgs = (List<Object>) config.getStringKeyMaybe("constructor.args").or(ImmutableList.of());
        String factoryMethodName = (String) config.getStringKeyMaybe("factoryMethod.name").orNull();
        List<Object> factoryMethodArgs = (List<Object>) config.getStringKeyMaybe("factoryMethod.args").or(ImmutableList.of());
        Map<String,Object> objectFields = (Map<String, Object>) config.getStringKeyMaybe("object.fields").or(MutableMap.of());
        Map<String,Object> brooklynConfig = (Map<String, Object>) config.getStringKeyMaybe(BrooklynCampReservedKeys.BROOKLYN_CONFIG).or(MutableMap.of());
        boolean deferred = TypeCoercions.coerce(config.getStringKeyMaybe("deferred").or(Boolean.FALSE), Boolean.class);

        String mappedTypeName = DeserializingClassRenamesProvider.INSTANCE.findMappedName(typeName);
        Class<?> type;
        try {
            // local classes get loaded here
            type = new ClassLoaderUtils(BrooklynDslCommon.class).loadClass(mappedTypeName);
        } catch (ClassNotFoundException e) {
            // this is normal, we just do a deferred load, likely a registered type
            if (LOG.isTraceEnabled()) {
                LOG.trace("No local class " + typeName + " for DSL 'object'; assuming it is a registered type; will defer its loading");
            }
            return new DslObject(mappedTypeName, constructorArgs, objectFields, brooklynConfig);
        }

        if (!deferred && resolved(constructorArgs) && resolved(factoryMethodArgs) && resolved(objectFields.values()) && resolved(brooklynConfig.values())) {
            if (factoryMethodName == null) {
                return DslObject.create(type, constructorArgs, objectFields, brooklynConfig);
            } else {
                return DslObject.create(type, factoryMethodName, factoryMethodArgs, objectFields, brooklynConfig);
            }
        } else {
            if (factoryMethodName == null) {
                return new DslObject(type, constructorArgs, objectFields, brooklynConfig);
            } else {
                return new DslObject(type, factoryMethodName, factoryMethodArgs, objectFields, brooklynConfig);
            }
        }
    }

    @DslAccessible
    public static Object object(String argumentsMapAsYamlStringOrShorthand) {
        Object arg = Yamls.parseAll(argumentsMapAsYamlStringOrShorthand).iterator().next();
        if (arg instanceof Map) return object( (Map<String,Object>)arg );
        if (arg instanceof Collection) {
            List argL = MutableList.copyOf((Collection)arg);
            if (argL.size()>=1) {
                return object(MutableMap.of("type", argL.remove(0), "constructor.args", argL));
            }
        }
        if (arg instanceof String) {
            return object(MutableMap.of("type", arg, "constructor.args", Collections.emptyList()));
        }

        throw new IllegalArgumentException("Argument to object should be a map, or shorthand type name as string, or type name and constructor arguments as list");
    }

    // String manipulation

    /** Return a supplier for the given expression as a literal string without any further parsing. */
    @DslAccessible
    public static Object literal(Object expression) {
        // since 2020-10 always defer, in case something else might try to parse it
        return new DslLiteral(expression);
    }

    protected final static class DslLiteral extends BrooklynDslDeferredSupplier<Object> {
        final String literalString;
        final String literalObjectJson;

        private DslLiteral() { this(null); }

        public DslLiteral(Object input) {
            this.literalString = input instanceof String ? (String)input : null;
            this.literalObjectJson = input instanceof String ? null : Jsonya.render(input);
        }

        @Override
        public Task<Object> newTask() {
            return Tasks.builder().displayName("DSL literal value")
                    .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                    .dynamic(false)
                    .body(() -> getImmediately().get())
                    .build();
        }

        @Override @JsonIgnore
        public Maybe<Object> getImmediately() {
            return Maybe.ofAllowingNull( literalObjectJson!=null ? Yamls.parseAll(literalObjectJson).iterator().next() : literalString );
        }

        @Override
        public String toString() {
            return "$brooklyn:literal(" +
                    (literalString!=null ? JavaStringEscapes.wrapJavaString(literalString) : literalObjectJson)
                    +")";
        }
    }

    /**
     * Returns the arg with characters escaped so it is a valid part of a URL, or a 
     * {@link BrooklynDslDeferredSupplier} that returns this if the arguments are not yet fully 
     * resolved.
     * 
     * See {@link Urls#encode(String)} for further details (it currently uses the encoding rules for
     * "x-www-form-urlencoded")
     * 
     * Do not call with a whole URL unless you want everything escaped (e.g. "http://myhost" will be
     * encoded as "http%3A%2F%2Fmyhost").
     */
    @DslAccessible
    public static Object urlEncode(Object arg) {
        if (resolved(arg)) {
            // if all args are resolved, apply the transform now
            return (arg == null) ? null : Urls.encode(arg.toString());
        } else {
            return new DslUrlEncode(arg);
        }
    }

    /**
     * Returns a formatted string or a {@link BrooklynDslDeferredSupplier} if the arguments
     * are not yet fully resolved.
     */
    @DslAccessible
    public static Object formatString(final Object pattern, final Object...args) {
        if (resolved(Lists.asList(pattern, args))) {
            // if all args are resolved, apply the format string now
            return String.format(String.valueOf(pattern), args);
        } else {
            return new DslFormatString(pattern, args);
        }
    }

    @DslAccessible
    public static Object regexReplacement(final Object source, final Object pattern, final Object replacement) {
        if (resolved(Arrays.asList(source, pattern, replacement))) {
            return (new Functions.RegexReplacer(String.valueOf(pattern), String.valueOf(replacement))).apply(String.valueOf(source));
        } else {
            return new DslRegexReplacement(source, pattern, replacement);
        }
    }
    
    /**
     * Deferred execution of escaping a URL.
     *
     * @see DependentConfiguration#urlEncode(Object)
     */
    protected static class DslUrlEncode extends BrooklynDslDeferredSupplier<String> {

        private static final long serialVersionUID = -4849297712650560863L;

        private final Object arg;

        public DslUrlEncode(Object arg) {
            this.arg = arg;
        }

        @Override @JsonIgnore
        public final Maybe<String> getImmediately() {
            return DependentConfiguration.urlEncodeImmediately(arg);
        }

        @Override
        public Task<String> newTask() {
            return DependentConfiguration.urlEncode(arg);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(arg);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslUrlEncode that = DslUrlEncode.class.cast(obj);
            return Objects.equal(this.arg, that.arg);
        }

        @Override
        public String toString() {
            return "$brooklyn:urlEncode("+arg+")";
        }
    }

    /**
     * Deferred execution of String formatting.
     *
     * @see DependentConfiguration#formatString(Object, Object...)
     */
    protected static class DslFormatString extends BrooklynDslDeferredSupplier<String> {

        private static final long serialVersionUID = -4849297712650560863L;

        private final Object pattern;
        private final Object[] args;

        public DslFormatString(Object pattern, Object ...args) {
            this.pattern = pattern;
            this.args = args;
        }

        @Override @JsonIgnore
        public final Maybe<String> getImmediately() {
            return DependentConfiguration.formatStringImmediately(pattern, args);
        }

        @Override
        public Task<String> newTask() {
            return DependentConfiguration.formatString(pattern, args);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(pattern, args);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslFormatString that = DslFormatString.class.cast(obj);
            return Objects.equal(this.pattern, that.pattern) &&
                    Arrays.deepEquals(this.args, that.args);
        }

        @Override
        public String toString() {
            return DslToStringHelpers.fn("formatString", MutableList.<Object>builder().add(pattern).addAll(args).build());
        }
    }

    protected static class DslRegexReplacement extends BrooklynDslDeferredSupplier<String> {

        private static final long serialVersionUID = 737189899361183341L;

        private Object source;
        private Object pattern;
        private Object replacement;

        public DslRegexReplacement(Object source, Object pattern, Object replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
            this.source = source;
        }

        @Override @JsonIgnore
        public Maybe<String> getImmediately() {
            return DependentConfiguration.regexReplacementImmediately(source, pattern, replacement);
        }
        
        @Override
        public Task<String> newTask() {
            return DependentConfiguration.regexReplacement(source, pattern, replacement);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(source, pattern, replacement);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslRegexReplacement that = DslRegexReplacement.class.cast(obj);
            return Objects.equal(this.pattern, that.pattern) &&
                    Objects.equal(this.replacement, that.replacement) &&
                    Objects.equal(this.source, that.source);
        }

        @Override
        public String toString() {
            return DslToStringHelpers.fn("regexReplace", source, pattern, replacement);
        }
    }

    /** @deprecated since 0.7.0; kept for persisted state backwards compatibility */
    @SuppressWarnings({ "serial", "unused" })
    @Deprecated
    private static class FormatString extends DslFormatString {
        public FormatString(Object pattern, Object[] args) {
            super(pattern, args);
        }
    }

    /** Deferred execution of Object creation. */
    protected static class DslObject extends BrooklynDslDeferredSupplier<Object> {

        private static final long serialVersionUID = 8878388748085419L;

        private final String typeName;
        private final String factoryMethodName;
        private final Class<?> type;
        private final List<Object> constructorArgs, factoryMethodArgs;
        private final Map<String, Object> fields, config;

        public DslObject(
                String typeName,
                List<Object> constructorArgs,
                Map<String, Object> fields,
                Map<String, Object> config) {
            this.typeName = checkNotNull(typeName, "typeName");
            this.type = null;
            this.constructorArgs = checkNotNull(constructorArgs, "constructorArgs");
            this.factoryMethodName = null;
            this.factoryMethodArgs = ImmutableList.of();
            this.fields = MutableMap.copyOf(fields);
            this.config = MutableMap.copyOf(config);
        }

        public DslObject(
                Class<?> type,
                List<Object> constructorArgs,
                Map<String, Object> fields,
                Map<String, Object> config) {
            this.typeName = null;
            this.type = checkNotNull(type, "type");
            this.constructorArgs = checkNotNull(constructorArgs, "constructorArgs");
            this.factoryMethodName = null;
            this.factoryMethodArgs = ImmutableList.of();
            this.fields = MutableMap.copyOf(fields);
            this.config = MutableMap.copyOf(config);
        }

        public DslObject(
                String typeName,
                String factoryMethodName,
                List<Object> factoryMethodArgs,
                Map<String, Object> fields,
                Map<String, Object> config) {
            this.typeName = checkNotNull(typeName, "typeName");
            this.type = null;
            this.constructorArgs = ImmutableList.of();
            this.factoryMethodName = factoryMethodName;
            this.factoryMethodArgs = checkNotNull(factoryMethodArgs, "factoryMethodArgs");
            this.fields = MutableMap.copyOf(fields);
            this.config = MutableMap.copyOf(config);
        }

        public DslObject(
                Class<?> type,
                String factoryMethodName,
                List<Object> factoryMethodArgs,
                Map<String, Object> fields,
                Map<String, Object> config) {
            this.typeName = null;
            this.type = checkNotNull(type, "type");
            this.constructorArgs = ImmutableList.of();
            this.factoryMethodName = factoryMethodName;
            this.factoryMethodArgs = checkNotNull(factoryMethodArgs, "factoryMethodArgs");
            this.fields = MutableMap.copyOf(fields);
            this.config = MutableMap.copyOf(config);
        }


        @Override @JsonIgnore
        public Maybe<Object> getImmediately() {
            // TODO reconcile with "bean-with-type" usage and constructors;
            // for now, if it's a registered type we use that to get the java instance but we ignore fields on it

            final Class<?> clazz = getOrLoadType();
            final ExecutionContext executionContext = entity().getExecutionContext();

            final Function<Object, Object> resolver = new Function<Object, Object>() {
                @Override public Object apply(Object value) {
                    Maybe<Object> result = Tasks.resolving(value, Object.class).context(executionContext).deep().immediately(true).getMaybe();
                    if (result.isAbsent()) {
                        throw new ImmediateValueNotAvailableException();
                    } else {
                        return result.get();
                    }
                }
            };
            
            try {
                Map<String, Object> resolvedFields = MutableMap.copyOf(Maps.transformValues(fields, resolver));
                Map<String, Object> resolvedConfig = MutableMap.copyOf(Maps.transformValues(config, resolver));
                List<Object> resolvedConstructorArgs = MutableList.copyOf(Lists.transform(constructorArgs, resolver));
                List<Object> resolvedFactoryMethodArgs = MutableList.copyOf(Lists.transform(factoryMethodArgs, resolver));
    
                Object result;
                if (factoryMethodName == null) {
                    result = create(clazz, resolvedConstructorArgs, resolvedFields, resolvedConfig);
                } else {
                    result = create(clazz, factoryMethodName, resolvedFactoryMethodArgs, resolvedFields, resolvedConfig);
                }
                return Maybe.of(result);
            } catch (ImmediateValueNotAvailableException e) {
                return ImmediateValueNotAvailableException.newAbsentWithExceptionSupplier();
            }
        }
        
        @Override
        public Task<Object> newTask() {
            final Class<?> clazz = getOrLoadType();
            final ExecutionContext executionContext = entity().getExecutionContext();
            
            final Function<Object, Object> resolver = new Function<Object, Object>() {
                @Override public Object apply(Object value) {
                    try {
                        return Tasks.resolveDeepValueWithoutCoercion(value, executionContext);
                    } catch (ExecutionException | InterruptedException e) {
                        throw Exceptions.propagate(e);
                    }
                }
            };
            
            return Tasks.builder().displayName("building instance of '"+clazz+"'")
                    .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                    .dynamic(false)
                    .body(new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            // TODO de-dupe with getImmediately

                            Map<String, Object> resolvedFields = MutableMap.copyOf(Maps.transformValues(fields, resolver));
                            Map<String, Object> resolvedConfig = MutableMap.copyOf(Maps.transformValues(config, resolver));
                            List<Object> resolvedConstructorArgs = MutableList.copyOf(Lists.transform(constructorArgs, resolver));
                            List<Object> resolvedFactoryMethodArgs = MutableList.copyOf(Lists.transform(factoryMethodArgs, resolver));
                            
                            if (factoryMethodName == null) {
                                return create(clazz, resolvedConstructorArgs, resolvedFields, resolvedConfig);
                            } else {
                                return create(clazz, factoryMethodName, resolvedFactoryMethodArgs, resolvedFields, resolvedConfig);
                            }
                        }})
                    .build();
        }

        @JsonIgnore
        protected Class<?> getOrLoadType() {
            Class<?> type = this.type;

            if (type == null) {
                EntityInternal entity = entity();
                try {
                    if (entity==null) {
                        throw new IllegalStateException("Cannot invoke without a Task running the context of an entity");
                    }
                    RegisteredType rt = managementContext().getTypeRegistry().get(typeName, RegisteredTypeLoadingContexts.loader(new OsgiBrooklynClassLoadingContext(entity)));
                    if (rt!=null) {
                        Object inst = managementContext().getTypeRegistry().create(rt, null, null);
                        if (inst!=null) {
                            // we ignore the actually instance for now; see comments at start of this class
                            return inst.getClass();
                        }
                    }

                    // fall back to trying to load as a class
                    type = new ClassLoaderUtils(BrooklynDslCommon.class, entity).loadClass(typeName);
                } catch (ClassNotFoundException e) {
                    throw Exceptions.propagate(e);
                }
            }
            return type;
        }
        
        public static <T> T create(Class<T> type, List<?> constructorArgs, Map<String,?> fields, Map<String,?> config) {
            try {
                T bean = Reflections.invokeConstructorFromArgs(type, constructorArgs.toArray()).get();
                BeanUtils.populate(bean, fields);

                if (config.size() > 0) {
                    if (bean instanceof Configurable) {
                        ConfigBag configBag = ConfigBag.newInstance(config);
                        FlagUtils.setFieldsFromFlags(bean, configBag);
                        FlagUtils.setAllConfigKeys((Configurable) bean, configBag, true);
                    } else {
                        LOG.warn("While building object, type "+type+" is not 'Configurable'; cannot apply supplied config (continuing)");
                    }
                }
                return bean;
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }

        public static Object create(Class<?> type, String factoryMethodName, List<?> factoryMethodArgs, Map<String,?> fields, Map<String,?> config) {
            try {
                Optional<TypeCoercer> coercer = Optional.of(TypeCoercions.asTypeCoercer());
                Object bean = Reflections.invokeMethodFromArgs(type, factoryMethodName, factoryMethodArgs, false, coercer).get();
                BeanUtils.populate(bean, fields);

                if (config.size() > 0) {
                    if (bean instanceof Configurable) {
                        ConfigBag configBag = ConfigBag.newInstance(config);
                        FlagUtils.setFieldsFromFlags(bean, configBag);
                        FlagUtils.setAllConfigKeys((Configurable) bean, configBag, true);
                    } else {
                        LOG.warn("While building object via factory method '"+factoryMethodName+"', type "
                                + (bean == null ? "<null>" : bean.getClass())+" is not 'Configurable'; cannot apply "
                                + "supplied config (continuing)");
                    }
                }
                return bean;
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(type, fields, config);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslObject that = DslObject.class.cast(obj);
            return Objects.equal(this.type, that.type) &&
                    Objects.equal(this.fields, that.fields) &&
                    Objects.equal(this.config, that.config);
        }

        @Override
        public String toString() {
            // prefer the dsl set on us, if set
            if (dsl instanceof String && Strings.isNonBlank((String)dsl)) return (String)dsl;

            Object arg = type != null ? type.getName() : typeName;
            if (!constructorArgs.isEmpty()) {
                arg = MutableList.of(arg).appendAll(constructorArgs).toString();
            }
            return DslToStringHelpers.fn("object", arg);
        }
    }

    /**
     * Defers to management context's {@link ExternalConfigSupplierRegistry} to resolve values at runtime.
     * The name of the appropriate {@link ExternalConfigSupplier} is captured, along with the key of
     * the desired config value.
     */
    @DslAccessible
    public static DslExternal external(final String providerName, final String key) {
        return new DslExternal(providerName, key);
    }
    protected final static class DslExternal extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = -3860334240490397057L;
        private final String providerName;
        private final String key;

        public DslExternal(String providerName, String key) {
            this.providerName = providerName;
            this.key = key;
        }

        @Override @JsonIgnore
        public final Maybe<Object> getImmediately() {
            // Note this call to getConfig() is different from entity.getConfig.
            // We expect it to not block waiting for other entities.
            ManagementContextInternal managementContext = DslExternal.managementContext();
            return Maybe.<Object>of(managementContext.getExternalConfigProviderRegistry().getConfig(providerName, key));
        }

        @Override
        public Task<Object> newTask() {
            return Tasks.<Object>builder()
                .displayName("resolving external configuration: '" + key + "' from provider '" + providerName + "'")
                .dynamic(false)
                .body(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        return getImmediately().get();
                    }
                })
                .build();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(providerName, key);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslExternal that = DslExternal.class.cast(obj);
            return Objects.equal(this.providerName, that.providerName) &&
                    Objects.equal(this.key, that.key);
        }

        @Override
        public String toString() {
            return DslToStringHelpers.fn("external", providerName, key);
        }
    }

    public static Object template(Object template) {
        return new DslComponent(Scope.THIS, "").template(template);
    }

    public static Object template(Object template, Map<? ,?> substitutions) {
        return new DslComponent(Scope.THIS, "").template(template, substitutions);
    }

    public static class Functions {
        @DslAccessible
        public static Object regexReplacement(final Object pattern, final Object replacement) {
            if (resolved(pattern, replacement)) {
                return new org.apache.brooklyn.util.text.StringFunctions.RegexReplacer(String.valueOf(pattern), String.valueOf(replacement));
            } else {
                return new DslRegexReplacer(pattern, replacement);
            }
        }

        /** @deprecated since 0.11.0; use {@link org.apache.brooklyn.util.text.StringFunctions.RegexReplacer} instead */
        @Deprecated
        public static class RegexReplacer extends org.apache.brooklyn.util.text.StringFunctions.RegexReplacer {
            public RegexReplacer(String pattern, String replacement) {
                super(pattern, replacement);
            }
        }

        protected static class DslRegexReplacer extends BrooklynDslDeferredSupplier<Function<String, String>> {

            private static final long serialVersionUID = -2900037495440842269L;

            private Object pattern;
            private Object replacement;

            public DslRegexReplacer(Object pattern, Object replacement) {
                this.pattern = pattern;
                this.replacement = replacement;
            }

            @Override @JsonIgnore
            public Maybe<Function<String, String>> getImmediately() {
                return DependentConfiguration.regexReplacementImmediately(pattern, replacement);
            }
            
            @Override
            public Task<Function<String, String>> newTask() {
                return DependentConfiguration.regexReplacement(pattern, replacement);
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(pattern, replacement);
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj) return true;
                if (obj == null || getClass() != obj.getClass()) return false;
                DslRegexReplacer that = DslRegexReplacer.class.cast(obj);
                return Objects.equal(this.pattern, that.pattern) &&
                        Objects.equal(this.replacement, that.replacement);
            }

            @Override
            public String toString() {
                return DslToStringHelpers.fn("function.regexReplace", pattern, replacement);
            }
        }
    }

    // The results of the following methods are not supposed to get serialized. They are
    // only intermediate values for the DSL evaluator to apply function calls on. There
    // will always be a next method that gets executed on the return value.
    public static class DslFacades {
        private static class EntitySupplier implements DeferredSupplier<Entity>, ImmediateSupplier<Entity> {
            private String entityId;

            public EntitySupplier(String entityId) {
                this.entityId = entityId;
            }

            @Override @JsonIgnore
            public Maybe<Entity> getImmediately() {
                EntityInternal entity = entity();
                if (entity == null) {
                    return Maybe.absent("No entity available");
                }
                Entity targetEntity = entity.getManagementContext().getEntityManager().getEntity(entityId);
                return Maybe.of(targetEntity);
            }

            @Override
            public Entity get() {
                return getImmediately().orNull();
            }

            private EntityInternal entity() {
                return (EntityInternal) BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
            }
        }

        @DslAccessible
        public static Object wrap(Entity entity) {
            return DslComponent.newInstance(Scope.GLOBAL, new EntitySupplier(entity.getId()));
        }
    }

}
