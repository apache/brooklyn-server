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
package org.apache.brooklyn.util.core.text;

import com.google.common.annotations.Beta;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import freemarker.cache.StringTemplateLoader;
import freemarker.core.Environment;
import freemarker.core.Expression;
import freemarker.template.*;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.drivers.EntityDriver;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.ThreadLocalStack;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/** A variety of methods to assist in Freemarker template processing,
 * including passing in maps with keys flattened (dot-separated namespace),
 * and accessing {@link ManagementContext} brooklyn.properties 
 * and {@link Entity}, {@link EntityDriver}, and {@link Location} methods and config.
 * <p>
 * See {@link #processTemplateContents(String, ManagementContext, Map)} for
 * a description of how management access is done.
 */
public class TemplateProcessor {

    private static final Logger log = LoggerFactory.getLogger(TemplateProcessor.class);

    static {
        if (System.getProperty(freemarker.log.Logger.SYSTEM_PROPERTY_NAME_LOGGER_LIBRARY)==null) {
            System.setProperty(freemarker.log.Logger.SYSTEM_PROPERTY_NAME_LOGGER_LIBRARY, freemarker.log.Logger.LIBRARY_NAME_SLF4J);
        }
    }

    static BrooklynFreemarkerUnwrappableObjectWrapper BROOKLYN_WRAPPER = new BrooklynFreemarkerUnwrappableObjectWrapper();

    static ThreadLocalStack<Map<TemplateModel,Object>> TEMPLATE_MODEL_UNWRAP_CACHE = new ThreadLocalStack<>(true);
    static ThreadLocalStack<String> TEMPLATE_FILE_WANTING_LEGACY_SYNTAX = new ThreadLocalStack<>(true);

    public interface UnwrappableTemplateModel {
        Maybe<Object> unwrap();
    }

    static class BrooklynFreemarkerUnwrappableObjectWrapper extends BrooklynFreemarkerObjectWrapper {

        public Maybe<Object> unwrapMaybe(TemplateModel model) {
            Maybe<Object> result;
            if (model instanceof UnwrappableTemplateModel) {
                result = ((UnwrappableTemplateModel) model).unwrap();
                if (result.isPresent()) return result;
            }
            Maybe<Map<TemplateModel, Object>> unwrappingMapM = TEMPLATE_MODEL_UNWRAP_CACHE.peek();
            if (unwrappingMapM.isAbsent()) {
                return Maybe.absent("This thread does not support unwrapping");
            }
            if (!unwrappingMapM.get().containsKey(model)) {
                // happens if we return a constant within a model
                if (model instanceof TemplateScalarModel) {
                    try {
                        return Maybe.ofAllowingNull( ((TemplateScalarModel)model).getAsString() );
                    } catch (TemplateModelException e) {
                        throw Exceptions.propagate(e);
                    }
                }
                return Maybe.absent("Type and source of model is unknown: " + model);
            }
            return Maybe.ofAllowingNull(unwrappingMapM.get().get(model));
        }

        public TemplateModel rememberWrapperIfSupported(Object o, TemplateModel m) {
            Map<TemplateModel, Object> unwrappingMap = TEMPLATE_MODEL_UNWRAP_CACHE.peek().orNull();
            if (unwrappingMap!=null) unwrappingMap.put(m, o);
            return m;
        }

        @Override
        public TemplateModel wrap(Object o) throws TemplateModelException {
            return rememberWrapperIfSupported(o, super.wrap(o));
        }

        @Override
        public TemplateModel wrapAsBean(Object o) throws TemplateModelException {
            return rememberWrapperIfSupported(o, super.wrapAsBean(o));
        }
    }

    /** instead of this:
     * new DefaultObjectWrapperBuilder(Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS).build();
     * this class ensures our extensions are applied recursively, and we get our special model plus ths bean model for common types */
    static class BrooklynFreemarkerObjectWrapper extends DefaultObjectWrapper {

        public BrooklynFreemarkerObjectWrapper() {
            this(Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS);
        }
        public BrooklynFreemarkerObjectWrapper(Version incompatibleImprovements) {
            super(incompatibleImprovements);
        }

        @Override
        public TemplateModel wrap(Object o) throws TemplateModelException {
            if (o instanceof TemplateModel) {
                return (TemplateModel) o;
            }

            if (o instanceof Map) {
                // use our map recursively, so a map with `a.b` as a single key can be referenced as` ${a.b}` in the freemarker template
                return new DotSplittingTemplateModel((Map<?,?>)o);
            }

            if (o instanceof Instant) {
                // Freemarker doesn't support Instant, so we add
                return super.wrap(Date.from( (Instant)o ));
            }

            return super.wrap(o);
        }

        @Override
        protected TemplateModel handleUnknownType(final Object o) throws TemplateModelException {
            if (o instanceof EntityInternal) return EntityAndMapTemplateModel.forEntity((EntityInternal)o, null);
            if (o instanceof Location) return LocationAndMapTemplateModel.forLocation((LocationInternal)o, null);

            return super.handleUnknownType(o);
        }

        public TemplateModel wrapAsBean(Object o) throws TemplateModelException {
            if (o instanceof BrooklynObject) {
                // deproxy to reduce freemarker introspection interrupted errors
                o = Entities.deproxy((BrooklynObject) o);
            }
            // can get "Class inrospection data lookup aborded" from freemarker ClassIntrospector:250
            // if thread is interrupted because class lookup uses wait on a shared cache;
            // if the "interruption" is because of us, retry in this instance
            while (true) {
                try {
                    return super.handleUnknownType(o);
                } catch (Exception e) {
                    if (WorkflowExpressionResolution.isInterruptSetToPreventWaiting()) {
                        if (Exceptions.isRootCauseIsInterruption(e) || e.toString().contains(InterruptedException.class.getSimpleName())) {
                            Thread.yield();
                            continue;
                        }
                    }
                    throw e;
                }
            }
        }

    }

    public static TemplateModel wrapAsTemplateModel(Object o) throws TemplateModelException {
        return BROOKLYN_WRAPPER.wrap(o);
    }

    /** As per {@link #processTemplateContents(String, Map)}, but taking a file. */
    public static String processTemplateFile(String templateFileName, Map<String, ? extends Object> substitutions) {
        String templateContents;
        try {
            templateContents = Files.toString(new File(templateFileName), Charsets.UTF_8);
        } catch (IOException e) {
            log.warn("Error loading file " + templateFileName, e);
            throw Exceptions.propagate(e);
        }
        return processTemplateContents(templateFileName, templateContents, substitutions);
    }

    /** Processes template contents according to {@link EntityAndMapTemplateModel}. */
    public static String processTemplateFile(String templateFileName, EntityDriver driver, Map<String, ? extends Object> extraSubstitutions) {
        String templateContents;
        try {
            templateContents = Files.toString(new File(templateFileName), Charsets.UTF_8);
        } catch (IOException e) {
            log.warn("Error loading file " + templateFileName, e);
            throw Exceptions.propagate(e);
        }
        return processTemplateContents(templateFileName, templateContents, driver, extraSubstitutions);
    }

    /** Processes template contents according to {@link EntityAndMapTemplateModel}. */
    public static String processTemplateContents(String templateContents, EntityDriver driver, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents("unknown", templateContents, EntityAndMapTemplateModel.forDriver(driver, extraSubstitutions));
    }
    /** Processes template contents according to {@link EntityAndMapTemplateModel}. */
    public static String processTemplateContents(String templateContents, ManagementContext managementContext, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents("unknown", templateContents, EntityAndMapTemplateModel.forManagementContext(managementContext, extraSubstitutions));
    }
    /** Processes template contents according to {@link EntityAndMapTemplateModel}. */
    public static String processTemplateContents(String templateContents, Location location, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents("unknown", templateContents, LocationAndMapTemplateModel.forLocation((LocationInternal)location, extraSubstitutions));
    }


    /** Processes template contents according to {@link EntityAndMapTemplateModel}. */
    public static String processTemplateContents(String context, String templateContents, EntityDriver driver, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents(context, templateContents, EntityAndMapTemplateModel.forDriver(driver, extraSubstitutions));
    }
    /** Processes template contents according to {@link EntityAndMapTemplateModel}. */
    public static String processTemplateContents(String context, String templateContents, ManagementContext managementContext, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents(context, templateContents, EntityAndMapTemplateModel.forManagementContext(managementContext, extraSubstitutions));
    }
    /** Processes template contents according to {@link EntityAndMapTemplateModel}. */
    public static String processTemplateContents(String context, String templateContents, Location location, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents(context, templateContents, LocationAndMapTemplateModel.forLocation((LocationInternal)location, extraSubstitutions));
    }

    public static final class FirstAvailableTemplateModel implements TemplateHashModel, UnwrappableTemplateModel {
        MutableList<TemplateHashModel> models = MutableList.of();
        public FirstAvailableTemplateModel(Iterable<TemplateHashModel> modelsO) {
            for (TemplateHashModel m : modelsO) {
                if (m!=null) this.models.add(m);
            }
        }
        public FirstAvailableTemplateModel(TemplateHashModel ...modelsO) {
            this(Arrays.asList(modelsO));
        }

        @Override
        public Maybe<Object> unwrap() {
            return Maybe.of(models.stream().filter(m -> m instanceof UnwrappableTemplateModel).findAny()).mapMaybe(m -> ((UnwrappableTemplateModel)m).unwrap());
        }

        @Override
        public TemplateModel get(String s) throws TemplateModelException {
            for (TemplateHashModel m: models) {
                TemplateModel result = m.get(s);
                if (result!=null) return result;
            }
            return null;
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            for (TemplateHashModel m: models) {
                if (!m.isEmpty()) return false;
            }
            return true;
        }
    }

    /**
     * A Freemarker {@link TemplateHashModel} which will correctly handle entries of the form "a.b" in this map,
     * matching against template requests for "${a.b}".
     * <p>
     * Freemarker requests "a" in a map when given such a request, and expects that to point to a map
     * with a key "b". This model provides such maps even for "a.b" in a map.
     * <p>
     * However if "a" <b>and</b> "a.b" are in the map, this will <b>not</b> currently do the deep mapping.
     * (It does not have enough contextual information from Freemarker to handle this case.) */
    public static final class DotSplittingTemplateModel implements TemplateHashModelEx2, UnwrappableTemplateModel {
        protected final Map<?,?> map;

        protected DotSplittingTemplateModel(Map<?,?> map) {
            this.map = map;
        }

        @Override
        public Maybe<Object> unwrap() {
            return Maybe.of(map);
        }

        @Override
        public boolean isEmpty() { return map!=null && map.isEmpty(); }

        public boolean contains(String key) {
            if (map==null) return false;
            if (map.containsKey(key)) return true;
            for (Map.Entry<?,?> entry: map.entrySet()) {
                String k = Strings.toString(entry.getKey());
                if (k.startsWith(key+".")) {
                    // contains this prefix
                    return true;
                }
            }
            return false;
        }

        @Override
        public TemplateModel get(String key) {
            if (map==null) return null;
            try {
                if (map.containsKey(key))
                    return wrapAsTemplateModel( map.get(key) );

                Map<String,Object> result = MutableMap.of();
                for (Map.Entry<?,?> entry: map.entrySet()) {
                    String k = Strings.toString(entry.getKey());
                    if (k.startsWith(key+".")) {
                        String k2 = Strings.removeFromStart(k, key+".");
                        result.put(k2, entry.getValue());
                    }
                }
                if (!result.isEmpty())
                    return wrapAsTemplateModel( result );

            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw new IllegalStateException("Error accessing config '"+key+"'"+": "+e, e);
            }

            return null;
        }

        @Override
        public String toString() {
            return getClass().getName()+"["+map+"]";
        }

        public int size() {
            return map.size();
        }

        public TemplateCollectionModel keys() {
            return new SimpleCollection(map.keySet(), BROOKLYN_WRAPPER);
        }

        public TemplateCollectionModel values() {
            return new SimpleCollection(map.values(), BROOKLYN_WRAPPER);
        }

        public KeyValuePairIterator keyValuePairIterator() {
            return new MapKeyValuePairIterator(map, BROOKLYN_WRAPPER);
        }
    }

    /** FreeMarker {@link TemplateHashModel} which resolves keys inside the given entity or management context.
     * Callers are required to include dots for dot-separated keys.
     * Freemarker will only do this when in inside bracket notation in an outer map, as in <code>${outer['a.b.']}</code>;
     * as a result this is intended only for use by {@link EntityAndMapTemplateModel} where
     * a caller has used bracked notation, as in <code>${mgmt['key.subkey']}</code>. */
    protected static final class EntityConfigTemplateModel implements TemplateHashModel, UnwrappableTemplateModel {
        protected final EntityInternal entity;
        protected final ManagementContext mgmt;

        protected EntityConfigTemplateModel(EntityInternal entity) {
            this.entity = checkNotNull(entity, "entity");
            this.mgmt = entity.getManagementContext();
        }

        @Override
        public Maybe<Object> unwrap() {
            return Maybe.of(entity);
        }

        @Override
        public boolean isEmpty() { return false; }

        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            try {
                Object result = entity.getConfig(ConfigKeys.builder(Object.class).name(key).build());

                if (result==null)
                    result = mgmt.getConfig().getConfig(ConfigKeys.builder(Object.class).name(key).build());

                if (result!=null)
                    return wrapAsTemplateModel( result );

            } catch (Exception e) {
                throw handleModelError("Error accessing config '"+key+"' on "+entity, e);
            }

            return null;
        }

        @Override
        public String toString() {
            return getClass().getName()+"["+entity+"]";
        }
    }

    /** FreeMarker {@link TemplateHashModel} which resolves keys inside the given management context.
     * Callers are required to include dots for dot-separated keys.
     * Freemarker will only do this when in inside bracket notation in an outer map, as in <code>${outer['a.b.']}</code>;
     * as a result this is intended only for use by {@link EntityAndMapTemplateModel} where
     * a caller has used bracked notation, as in <code>${mgmt['key.subkey']}</code>. */
    protected static final class MgmtConfigTemplateModel implements TemplateHashModel {
        protected final ManagementContext mgmt;

        protected MgmtConfigTemplateModel(ManagementContext mgmt) {
            this.mgmt = checkNotNull(mgmt, "mgmt");
        }

        @Override
        public boolean isEmpty() { return false; }

        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            try {
                Object result = mgmt.getConfig().getConfig(ConfigKeys.builder(Object.class).name(key).build());

                if (result!=null)
                    return wrapAsTemplateModel( result );

            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw Exceptions.propagateAnnotated("Error accessing config '"+key+"': "+e, e);
            }

            return null;
        }

        @Override
        public String toString() {
            return getClass().getName()+"["+mgmt+"]";
        }
    }

    /** FreeMarker {@link TemplateHashModel} which resolves keys inside the given location.
     * Callers are required to include dots for dot-separated keys.
     * Freemarker will only do this when in inside bracket notation in an outer map, as in <code>${outer['a.b.']}</code>;
     * as a result this is intended only for use by {@link LocationAndMapTemplateModel} where
     * a caller has used bracked notation, as in <code>${mgmt['key.subkey']}</code>. */
    protected static final class LocationConfigTemplateModel implements TemplateHashModel, UnwrappableTemplateModel {
        protected final LocationInternal location;
        protected final ManagementContext mgmt;

        protected LocationConfigTemplateModel(LocationInternal location) {
            this.location = checkNotNull(location, "location");
            this.mgmt = location.getManagementContext();
        }

        @Override
        public Maybe<Object> unwrap() {
            return Maybe.of(location);
        }

        @Override
        public boolean isEmpty() { return false; }

        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            try {
                Object result = null;

                result = location.getConfig(ConfigKeys.builder(Object.class).name(key).build());

                if (result==null && mgmt!=null)
                    result = mgmt.getConfig().getConfig(ConfigKeys.builder(Object.class).name(key).build());

                if (result!=null)
                    return wrapAsTemplateModel( result );

            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw Exceptions.propagateAnnotated("Error accessing config '"+key+"'"
                        + (location!=null ? " on "+location : "")+": "+e, e);
            }

            return null;
        }

        @Override
        public String toString() {
            return getClass().getName()+"["+location+"]";
        }
    }

    public static class TemplateModelDataUnavailableException extends TemplateModelException {
        public TemplateModelDataUnavailableException(String s, Throwable cause) {
            super(s, cause);
        }
    }

    @Beta
    public static Error handleModelError(String msg, Throwable cause) throws TemplateModelException {
        // up to caller to determine if it was an interruption

        if (Exceptions.isCausedByInterruptInAnyThread(cause)) {
            // we can only catch exceptions in expressions if we throw InvalidReferenceException, but that is checked and not allowed (also doesn't allow cause)
//                    throw new InvalidReferenceException("Sensor '" + key + "' unavailable", null);
            // we could return null but that might mask errors. instead let caller handle (eg WorkflowExpressionResolution)
            // throw a custom exception of our own

            // maybe do not include the InterruptException cause because that causes any thread who propagates this to also be interrupted?
            // or change propagateIfFatal not to set interrupt flag again?
            throw new TemplateModelDataUnavailableException(msg+": "+Exceptions.collapseText(cause), cause);
        } else {
            // interruptions not treated as fatal here
            Exceptions.propagateIfFatal(cause);
        }

        // throwing this doesn't buy us much but a bit of efficiency in the catching and consistency with other freemarker code
        throw new TemplateModelException(msg+": "+Exceptions.collapseText(cause), cause);
    }

    protected final static class EntityAttributeTemplateModel implements TemplateHashModel, UnwrappableTemplateModel {
        protected final EntityInternal entity;
        private final SensorResolutionMode mode;

        @Override
        public Maybe<Object> unwrap() {
            return Maybe.of(entity);
        }

        enum SensorResolutionMode { SENSOR_DEFINITION, ATTRIBUTE_VALUE, ATTRIBUTE_WHEN_READY }

        protected EntityAttributeTemplateModel(EntityInternal entity, SensorResolutionMode mode) {
            this.entity = entity;
            if (TEMPLATE_FILE_WANTING_LEGACY_SYNTAX.peek().isPresentAndNonNull()) {
                // in templates, we have only ever supported attribute when ready. preserve that for now, but warn of deprecation.
                if (mode != SensorResolutionMode.ATTRIBUTE_WHEN_READY) {
                    log.warn("Using deprecated legacy attributeWhenReady behaviour of ${entity.attribute...} or ${entity.sensor...}. Template should be updated to use ${entity.attributeWhenReady...} if that is required: "
                        + TEMPLATE_FILE_WANTING_LEGACY_SYNTAX.peek());
                    mode = SensorResolutionMode.ATTRIBUTE_WHEN_READY;
                }
            }
            this.mode = mode;
        }

        @Override
        public boolean isEmpty() throws TemplateModelException {
            return false;
        }

        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            Object result;
            try {
                result =
                        mode == SensorResolutionMode.ATTRIBUTE_WHEN_READY ?
                                ((EntityInternal)entity).getExecutionContext().get( DependentConfiguration.attributeWhenReady(entity,
                                    Sensors.builder(Object.class, key).persistence(AttributeSensor.SensorPersistenceMode.NONE).build()))
                        : mode == SensorResolutionMode.ATTRIBUTE_VALUE ?
                                entity.sensors().get( Sensors.newSensor(Object.class, key) )
                        : mode == SensorResolutionMode.SENSOR_DEFINITION ?
                                entity.getEntityType().getSensor(key)
                        : Exceptions.propagate(new IllegalStateException("Invalid mode "+mode));
            } catch (Exception e) {
                throw handleModelError("Error resolving attribute '"+key+"' on "+entity, e);
            }

            if (result == null) {
                return null;
            } else {
                return wrapAsTemplateModel(result);
            }
        }

        @Override
        public String toString() {
            return getClass().getName()+"["+entity+"]";
        }
    }

    private static TemplateHashModel dotOrNull(Map<String,?> extraSubstitutions) {
        if (extraSubstitutions==null) return null;
        return new DotSplittingTemplateModel(extraSubstitutions);
    }

    private static TemplateHashModel wrappedBeanToHashOrNull(Object o) {
        if (o==null) return null;
        TemplateModel wrapped = null;
        try {
            wrapped = BROOKLYN_WRAPPER.wrapAsBean(o);
        } catch (TemplateModelException e) {
            throw Exceptions.propagate(e);
        }
        if (wrapped instanceof TemplateHashModel) return (TemplateHashModel) wrapped;
        return null;
    }

    /**
     * Provides access to config on an entity or management context, using
     * <code>${config['entity.config.key']}</code> or <code>${mgmt['brooklyn.properties.key']}</code> notation,
     * and also allowing access to <code>getX()</code> methods on entity (interface) or driver
     * using <code>${entity.x}</code> or <code><${driver.x}</code>.
     * Optional extra properties can be supplied, treated as per {@link DotSplittingTemplateModel}.
     */
    public static final class EntityAndMapTemplateModel implements TemplateHashModel, UnwrappableTemplateModel {
        protected final EntityInternal entity;
        protected final EntityDriver driver;
        protected final ManagementContext mgmt;
        protected final DotSplittingTemplateModel extraSubstitutionsModel;

        // TODO the extra substitutions here (and in LocationAndMapTemplateModel) could be replaced with
        // FirstAvailableTemplateModel(entityModel, mapHashModel)

        protected EntityAndMapTemplateModel(ManagementContext mgmt, EntityInternal entity, EntityDriver driver) {
            this.driver = driver;
            this.entity = entity !=null ? entity : driver!=null ? (EntityInternal) driver.getEntity() : null;
            this.mgmt = mgmt != null ? mgmt : this.entity!=null ? this.entity.getManagementContext() : null;
            extraSubstitutionsModel = new DotSplittingTemplateModel(null);
        }

        @Override
        public Maybe<Object> unwrap() {
            return Maybe.ofDisallowingNull(entity!=null ? entity : mgmt!=null ? mgmt : extraSubstitutionsModel.unwrap().orNull());
        }

        public static TemplateHashModel forDriver(EntityDriver driver, Map<String,? extends Object> extraSubstitutions) {
            return new FirstAvailableTemplateModel(new EntityAndMapTemplateModel(null, null, driver), wrappedBeanToHashOrNull(driver), wrappedBeanToHashOrNull(driver.getEntity()), dotOrNull(extraSubstitutions));
        }

        public static TemplateHashModel forEntity(Entity entity, Map<String,? extends Object> extraSubstitutions) {
            return new FirstAvailableTemplateModel(new EntityAndMapTemplateModel(null, (EntityInternal) entity, null), wrappedBeanToHashOrNull(entity), dotOrNull(extraSubstitutions));
        }

        public static TemplateHashModel forManagementContext(ManagementContext mgmt, Map<String,? extends Object> extraSubstitutions) {
            return new FirstAvailableTemplateModel(new EntityAndMapTemplateModel(mgmt, null, null), dotOrNull(extraSubstitutions));
        }

        @Deprecated /** @deprecated since 1.1 use {@link #forEntity(Entity, Map)} and related instead; substitions added separately using {@link FirstAvailableTemplateModel }*/
        protected EntityAndMapTemplateModel(ManagementContext mgmt, Map<String,? extends Object> extraSubstitutions) {
            this.entity = null;
            this.driver = null;
            this.mgmt = mgmt;
            this.extraSubstitutionsModel = new DotSplittingTemplateModel(extraSubstitutions);
        }

        @Deprecated /** @deprecated since 1.1 use {@link #forEntity(Entity, Map)} and related instead; substitions added separately using {@link FirstAvailableTemplateModel }*/
        protected EntityAndMapTemplateModel(EntityDriver driver, Map<String,? extends Object> extraSubstitutions) {
            this.driver = driver;
            this.entity = (EntityInternal) driver.getEntity();
            this.mgmt = entity.getManagementContext();
            this.extraSubstitutionsModel = new DotSplittingTemplateModel(extraSubstitutions);
        }

        @Deprecated /** @deprecated since 1.1 use {@link #forEntity(Entity, Map)} and related instead; substitions added separately using {@link FirstAvailableTemplateModel }*/
        protected EntityAndMapTemplateModel(EntityInternal entity, Map<String,? extends Object> extraSubstitutions) {
            this.entity = entity;
            this.driver = null;
            this.mgmt = entity.getManagementContext();
            this.extraSubstitutionsModel = new DotSplittingTemplateModel(extraSubstitutions);
        }

        @Override
        public boolean isEmpty() { return false; }

        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            if (extraSubstitutionsModel.contains(key))
                return wrapAsTemplateModel( extraSubstitutionsModel.get(key) );

            if ("entity".equals(key) && entity!=null)
                return wrapAsTemplateModel( entity );
            if ("config".equals(key)) {
                if (entity!=null)
                    return new EntityConfigTemplateModel(entity);
                else
                    return new MgmtConfigTemplateModel(mgmt);
            }
            if ("mgmt".equals(key)) {
                return new MgmtConfigTemplateModel(mgmt);
            }

            if ("driver".equals(key) && driver!=null)
                return wrapAsTemplateModel( driver );
            if ("location".equals(key)) {
                if (driver!=null && driver.getLocation()!=null)
                    return wrapAsTemplateModel( driver.getLocation() );
                if (entity!=null)
                    return wrapAsTemplateModel( Iterables.getOnlyElement( entity.getLocations() ) );
            }

            if ("sensor".equals(key)) return new EntityAttributeTemplateModel(entity, EntityAttributeTemplateModel.SensorResolutionMode.ATTRIBUTE_VALUE);
            if ("attribute".equals(key)) return new EntityAttributeTemplateModel(entity, EntityAttributeTemplateModel.SensorResolutionMode.ATTRIBUTE_VALUE);
            if ("attributeWhenReady".equals(key)) return new EntityAttributeTemplateModel(entity, EntityAttributeTemplateModel.SensorResolutionMode.ATTRIBUTE_WHEN_READY);
            // new option
            if ("sensor_definition".equals(key)) return new EntityAttributeTemplateModel(entity, EntityAttributeTemplateModel.SensorResolutionMode.SENSOR_DEFINITION);

//            // getters work for these
//            if ("id".equals(key)) return wrapAsTemplateModel(entity.getId());
//            if ("displayName".equals(key)) return wrapAsTemplateModel(entity.getDisplayName());
//            if ("parent".equals(key)) return wrapAsTemplateModel(entity.getParent());
//            if ("application".equals(key)) return wrapAsTemplateModel(entity.getApplication());

            if ("name".equals(key)) return wrapAsTemplateModel(entity.getDisplayName());
            if ("tags".equals(key)) return wrapAsTemplateModel(entity.tags().getTags());

            // bit of hack, but sometimes we use ${javaSysProps.JVM_SYSTEM_PROPERTY}
            if ("javaSysProps".equals(key))
                return wrapAsTemplateModel( System.getProperties() );


            return null;
        }

        @Override
        public String toString() {
            return getClass().getName()+"["+(entity!=null ? entity : mgmt)+"]";
        }
    }

    /**
     * Provides access to config on an entity or management context, using
     * <code>${config['entity.config.key']}</code> or <code>${mgmt['brooklyn.properties.key']}</code> notation,
     * and also allowing access to <code>getX()</code> methods on entity (interface) or driver
     * using <code>${entity.x}</code> or <code><${driver.x}</code>.
     * Optional extra properties can be supplied, treated as per {@link DotSplittingTemplateModel}.
     */
    protected static final class LocationAndMapTemplateModel implements TemplateHashModel, UnwrappableTemplateModel {
        protected final LocationInternal location;
        protected final ManagementContext mgmt;
        protected final DotSplittingTemplateModel extraSubstitutionsModel;

        @Deprecated /** @deprecated since 1.1 use {@link #forLocation(LocationInternal, Map)} instead; substitions added separately using {@link FirstAvailableTemplateModel }*/
        protected LocationAndMapTemplateModel(LocationInternal location, Map<String,? extends Object> extraSubstitutions) {
            this.location = checkNotNull(location, "location");
            this.mgmt = location.getManagementContext();
            this.extraSubstitutionsModel = new DotSplittingTemplateModel(extraSubstitutions);
        }

        @Override
        public Maybe<Object> unwrap() {
            return Maybe.of(location);
        }

        static TemplateHashModel forLocation(LocationInternal location, Map<String,? extends Object> extraSubstitutions) {
            return new FirstAvailableTemplateModel(new LocationAndMapTemplateModel(location, null), wrappedBeanToHashOrNull(location), dotOrNull(extraSubstitutions));
        }

        @Override
        public boolean isEmpty() { return false; }

        @Override
        public TemplateModel get(String key) throws TemplateModelException {
            if (extraSubstitutionsModel.contains(key))
                return wrapAsTemplateModel( extraSubstitutionsModel.get(key) );

            if ("location".equals(key))
                return wrapAsTemplateModel( location );
            if ("config".equals(key)) {
                return new LocationConfigTemplateModel(location);
            }
            if ("mgmt".equals(key)) {
                return new MgmtConfigTemplateModel(mgmt);
            }

            if (mgmt!=null) {
                // TODO deprecated in 0.7.0, remove after next version
                // ie not supported to access global props without qualification
                Object result = mgmt.getConfig().getConfig(ConfigKeys.builder(Object.class).name(key).build());
                if (result!=null) {
                    log.warn("Deprecated access of global brooklyn.properties value for "+key+"; should be qualified with 'mgmt.'");
                    return wrapAsTemplateModel( result );
                }
            }

            if ("name".equals(key))
                return wrapAsTemplateModel( location.getDisplayName() );
            if ("javaSysProps".equals(key))
                return wrapAsTemplateModel( System.getProperties() );

            return null;
        }

        @Override
        public String toString() {
            return getClass().getName()+"["+location+"]";
        }
    }

    /** Processes template contents with the given items in scope as per {@link EntityAndMapTemplateModel}. */
    public static String processTemplateContents(String templateContents, final EntityInternal entity, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents("unknown", templateContents, entity, extraSubstitutions);
    }
    public static String processTemplateContents(String context, String templateContents, final EntityInternal entity, Map<String,? extends Object> extraSubstitutions) {
        return processTemplateContents(context, templateContents, EntityAndMapTemplateModel.forEntity(entity, extraSubstitutions));
    }

    /** Processes template contents using the given map, passed to freemarker,
     * with dot handling as per {@link DotSplittingTemplateModel}. */
    public static String processTemplateContents(String templateContents, final Map<String, ? extends Object> substitutions) {
        return processTemplateContents("unknown", templateContents, substitutions);
    }
    public static String processTemplateContents(String context, String templateContents, final Map<String, ? extends Object> substitutions) {
        TemplateHashModel root;
        try {
            root = substitutions != null
                    ? (TemplateHashModel)wrapAsTemplateModel(substitutions)
                    : null;
        } catch (TemplateModelException e) {
            throw new IllegalStateException("Unable to set up TemplateHashModel to parse template, given "+substitutions+": "+e, e);
        }

        return processTemplateContents(context, templateContents, root);
    }

    /** Processes template contents against the given {@link TemplateHashModel}. */
    public static String processTemplateContents(String templateContents, final TemplateHashModel substitutions) {
        return (String) processTemplateContents("unknown", templateContents, substitutions);
    }
    private static String processTemplateContents(String context, String templateContents, final TemplateHashModel substitutions) {
        return (String) processTemplateContentsLegacy(context, templateContents, substitutions, false, true);
    }

    @Deprecated /** since 1.1, used to warn about deprecated use of ${entity.sensor.value} to have attribute-when-ready behaviour */
    private static Object processTemplateContentsLegacy(String context, String templateContents, final TemplateHashModel substitutions, boolean allowSingleVariableObject, boolean logErrors) {
        try {
            TEMPLATE_FILE_WANTING_LEGACY_SYNTAX.push(context);
            return processTemplateContents(context, templateContents, substitutions, allowSingleVariableObject, logErrors);
        } finally {
            TEMPLATE_FILE_WANTING_LEGACY_SYNTAX.pop();
        }
    }

    public static Object processTemplateContents(String context, String templateContents, final TemplateHashModel substitutions, boolean allowSingleVariableObject, boolean logErrors) {
        return processTemplateContents(context, templateContents, substitutions, allowSingleVariableObject, logErrors, InterpolationErrorMode.FAIL);
    }
    public static Object processTemplateContents(String context, String templateContents, final TemplateHashModel substitutions, boolean allowSingleVariableObject, boolean logErrors, InterpolationErrorMode errorMode) {
        try {
            Configuration cfg = new Configuration(Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS);
            cfg.setLogTemplateExceptions(logErrors);
            cfg.setNumberFormat("computer");

            StringTemplateLoader templateLoader = new StringTemplateLoader();
            templateLoader.putTemplate(context, templateContents);
            cfg.setTemplateLoader(templateLoader);
            Template template = cfg.getTemplate(context);

            if (allowSingleVariableObject && template.getRootTreeNode().getClass().getName().equals("freemarker.core.DollarVariable")) {
                Object dollarVariable = template.getRootTreeNode();
                // calculateInterpolatedStringOrMarkup calls escapedExpression.eval(env); unv very little accessible, so we use reflection
                Maybe<Object> escapedExpression = Reflections.getFieldValueMaybe(dollarVariable, "escapedExpression");
                Environment env = template.createProcessingEnvironment(substitutions, null);
                Maybe<Method> evalMethod = Reflections.findMethodMaybe(Expression.class, "eval", Environment.class);
                try {
                    TEMPLATE_MODEL_UNWRAP_CACHE.push(MutableMap.of());
                    Maybe<Object> model = evalMethod.isAbsent() ? Maybe.Absent.castAbsent(evalMethod) : escapedExpression.map(expr -> {
                        try {
                            return Reflections.invokeMethodFromArgs(expr,
                                    evalMethod.get(), MutableList.of(env), true);
                        } catch (Exception e) {
                            throw Exceptions.propagate(e);
                        }
                    });
                    if (model.isPresent()) {
                        if (model.get() instanceof TemplateModel) {
                            return BROOKLYN_WRAPPER.unwrapMaybe((TemplateModel) model.get()).get();
                        } else if (model.get()==null) {
                            // key not found, fall through to below for proper error handling
                        } else {
                            log.warn("Unable to find model in local cache for unwrapping: " + model.get());
                        }
                    } else {
                        log.warn("Unable to access FreeMarker internals to resolve " + templateContents + "; will cast argument as string");
                    }
                } finally {
                    TEMPLATE_MODEL_UNWRAP_CACHE.pop();
                }
            }

            // TODO could expose CAMP '$brooklyn:' style dsl, based on template.createProcessingEnvironment
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Writer out = new OutputStreamWriter(baos);
            template.setTemplateExceptionHandler(new ForgivingFreemarkerTemplateExceptionHandler(errorMode));
            template.process(substitutions, out);
            out.flush();

            return new String(baos.toByteArray());
        } catch (Exception e) {
            if (logErrors) {
                if (e instanceof RuntimeInterruptedException) {
                    log.warn("Template not currently resolvable: " + Exceptions.collapseText(e));
                } else {
                    log.warn("Error processing template (propagating): " + Exceptions.collapseText(e), e);
                }
                log.debug("Template which could not be parsed (causing " + e + ") is:"
                        + (Strings.isMultiLine(templateContents) ? "\n" + templateContents : templateContents));
            }
            throw Exceptions.propagate(e);
        }
    }

    public enum InterpolationErrorMode {
        FAIL,
        BLANK,
        IGNORE,
    }

    InterpolationErrorMode interpolationErrorMode;

    public void setInterpolationErrorMode(InterpolationErrorMode interpolationErrorMode) {
        this.interpolationErrorMode = interpolationErrorMode;
    }

    public static class ForgivingFreemarkerTemplateExceptionHandler implements TemplateExceptionHandler {
        private final InterpolationErrorMode errorMode;
        public ForgivingFreemarkerTemplateExceptionHandler(InterpolationErrorMode errorMode) {
            this.errorMode = errorMode;
        }
        public void handleTemplateException(TemplateException te, Environment env, Writer out) throws TemplateException {
            if (errorMode==null || errorMode==InterpolationErrorMode.FAIL) throw te;

            if (errorMode==InterpolationErrorMode.BLANK) return;
            if (errorMode==InterpolationErrorMode.IGNORE) {
                try {
                    // below won't work for complex expressions but those are discouraged anyways
                    out.write("${" + te.getBlamedExpressionString() + "}");

                    // this would work better, if we want to access private fields
                    //te.getFTLInstructionStack();
//                TemplateElement els[] = env.instructionStack;
//                env.instructionStackSize - 1
                } catch (IOException e) {
                    throw Exceptions.propagate(e);
                }
                return;
            }
        }
    }

}
