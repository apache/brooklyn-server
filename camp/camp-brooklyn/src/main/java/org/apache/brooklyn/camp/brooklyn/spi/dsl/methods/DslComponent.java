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

import static org.apache.brooklyn.camp.brooklyn.spi.dsl.DslUtils.resolved;

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslCallable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.EntityManagerInternal;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.groovy.GroovyJavaMethods;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Callables;

public class DslComponent extends BrooklynDslDeferredSupplier<Entity> implements DslCallable {

    private static final long serialVersionUID = -7715984495268724954L;
    
    private final String componentId;
    private final DeferredSupplier<?> componentIdSupplier;
    private final DslComponent scopeComponent;
    private final Scope scope;

    /**
     * Checks the type of {@code componentId} to create the right kind of {@link DslComponent}
     * (based on whether the componentId is already resolved. Accepts either a {@link String} or a 
     * {@link DeferredSupplier}.
     */
    public static DslComponent newInstance(DslComponent scopeComponent, Scope scope, Object componentId) {
        if (resolved(componentId)) {
            // if all args are resolved, apply the componentId now
            return new DslComponent(scopeComponent, scope, (String) componentId);
        } else {
            return new DslComponent(scopeComponent, scope, (DeferredSupplier<?>)componentId);
        }
    }

    /**
     * Checks the type of {@code componentId} to create the right kind of {@link DslComponent}
     * (based on whether the componentId is already resolved. Accepts either a {@link String} or a 
     * {@link DeferredSupplier}.
     */
    public static DslComponent newInstance(Scope scope, Object componentId) {
        if (resolved(componentId)) {
            // if all args are resolved, apply the componentId now
            return new DslComponent(scope, (String) componentId);
        } else {
            return new DslComponent(scope, (DeferredSupplier<?>)componentId);
        }
    }

    /**
     * Resolve componentId in the {@link Scope#GLOBAL} scope.
     * 
     * @deprecated since 0.10.0; pass the {@link Scope} explicitly.
     */
    @Deprecated
    public DslComponent(String componentId) {
        this(Scope.GLOBAL, componentId);
    }

    /**
     * Resolve in scope relative to the current 
     * {@link BrooklynTaskTags#getTargetOrContextEntity) target or context} entity
     * (where the scope defines an unambiguous relationship that will resolve to a single 
     * component - e.g. "parent").
     */
    public DslComponent(Scope scope) {
        this(null, scope);
    }

    /**
     * Resolve in scope relative to {@code scopeComponent} entity
     * (where the scope defines an unambiguous relationship that will resolve to a single 
     * component - e.g. "parent").
     */
    public DslComponent(DslComponent scopeComponent, Scope scope) {
        this(scopeComponent, scope, (String)null);
    }
    
    /**
     * Resolve componentId in scope relative to the current
     * {@link BrooklynTaskTags#getTargetOrContextEntity) target or context} entity.
     */
    public DslComponent(Scope scope, String componentId) {
        this(null, scope, componentId);
    }

    public DslComponent(Scope scope, DeferredSupplier<?> componentIdSupplier) {
        this(null, scope, componentIdSupplier);
    }

    /**
     * Resolve componentId in scope relative to scopeComponent.
     */
    public DslComponent(DslComponent scopeComponent, Scope scope, String componentId) {
        Preconditions.checkNotNull(scope, "scope");
        this.scopeComponent = scopeComponent;
        this.componentId = componentId;
        this.componentIdSupplier = null;
        this.scope = scope;
    }

    /**
     * Resolve componentId in scope relative to scopeComponent.
     */
    public DslComponent(DslComponent scopeComponent, Scope scope, DeferredSupplier<?> componentIdSupplier) {
        Preconditions.checkNotNull(scope, "scope");
        this.scopeComponent = scopeComponent;
        this.componentId = null;
        this.componentIdSupplier = componentIdSupplier;
        this.scope = scope;
    }

    // ---------------------------

    @Override
    public final Maybe<Entity> getImmediately() {
        return new EntityInScopeFinder(scopeComponent, scope, componentId, componentIdSupplier).getImmediately();
    }

    @Override
    public Task<Entity> newTask() {
        return TaskBuilder.<Entity>builder()
                .displayName(toString())
                .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                .body(new EntityInScopeFinder(scopeComponent, scope, componentId, componentIdSupplier))
                .build();
    }
    
    protected static class EntityInScopeFinder implements Callable<Entity>, ImmediateSupplier<Entity> {
        protected final DslComponent scopeComponent;
        protected final Scope scope;
        protected final String componentId;
        protected final DeferredSupplier<?> componentIdSupplier;
        
        public EntityInScopeFinder(DslComponent scopeComponent, Scope scope, String componentId, DeferredSupplier<?> componentIdSupplier) {
            this.scopeComponent = scopeComponent;
            this.scope = scope;
            this.componentId = componentId;
            this.componentIdSupplier = componentIdSupplier;
        }

        @Override 
        public Maybe<Entity> getImmediately() {
            try {
                return callImpl(true);
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }

        @Override
        public Entity call() throws Exception {
            return callImpl(false).get();
        }

        protected Maybe<Entity> getEntity(boolean immediate) {
            if (scopeComponent != null) {
                if (immediate) {
                    return scopeComponent.getImmediately();
                } else {
                    return Maybe.of(scopeComponent.get());
                }
            } else {
                return Maybe.<Entity>of(entity());
            }
        }
        
        protected Maybe<Entity> callImpl(boolean immediate) throws Exception {
            Maybe<Entity> entityMaybe = getEntity(immediate);
            if (immediate && entityMaybe.isAbsent()) {
                return entityMaybe;
            }
            EntityInternal entity = (EntityInternal) entityMaybe.get();
            
            Iterable<Entity> entitiesToSearch = null;
            Predicate<Entity> notSelfPredicate = Predicates.not(Predicates.<Entity>equalTo(entity));

            switch (scope) {
                case THIS:
                    return Maybe.<Entity>of(entity);
                case PARENT:
                    return Maybe.<Entity>of(entity.getParent());
                case GLOBAL:
                    entitiesToSearch = ((EntityManagerInternal)entity.getManagementContext().getEntityManager())
                        .getAllEntitiesInApplication( entity().getApplication() );
                    break;
                case ROOT:
                    return Maybe.<Entity>of(entity.getApplication());
                case SCOPE_ROOT:
                    return Maybe.<Entity>of(Entities.catalogItemScopeRoot(entity));
                case DESCENDANT:
                    entitiesToSearch = Entities.descendantsWithoutSelf(entity);
                    break;
                case ANCESTOR:
                    entitiesToSearch = Entities.ancestorsWithoutSelf(entity);
                    break;
                case SIBLING:
                    entitiesToSearch = entity.getParent().getChildren();
                    entitiesToSearch = Iterables.filter(entitiesToSearch, notSelfPredicate);
                    break;
                case CHILD:
                    entitiesToSearch = entity.getChildren();
                    break;
                default:
                    throw new IllegalStateException("Unexpected scope "+scope);
            }
            
            String desiredComponentId;
            if (componentId == null) {
                if (componentIdSupplier == null) {
                    throw new IllegalArgumentException("No component-id or component-id supplier, when resolving entity in scope '" + scope + "' wrt " + entity);
                }
                
                Maybe<Object> maybeComponentId = Tasks.resolving(componentIdSupplier)
                        .as(Object.class)
                        .context(getExecutionContext())
                        .immediately(immediate)
                        .description("Resolving component-id from " + componentIdSupplier)
                        .getMaybe();
                
                if (immediate) {
                    if (maybeComponentId.isAbsent()) {
                        return Maybe.absent(Maybe.getException(maybeComponentId));
                    }
                }
                
                // Support being passed an explicit entity via the DSL
                if (maybeComponentId.get() instanceof Entity) {
                    if (Iterables.contains(entitiesToSearch, maybeComponentId.get())) {
                        return Maybe.of((Entity)maybeComponentId.get());
                    } else {
                        throw new IllegalStateException("Resolved component " + maybeComponentId.get() + " is not in scope '" + scope + "' wrt " + entity);
                    }
                }
                
                desiredComponentId = TypeCoercions.coerce(maybeComponentId.get(), String.class);

                if (Strings.isBlank(desiredComponentId)) {
                    throw new IllegalStateException("component-id blank, from " + componentIdSupplier);
                }
                
            } else {
                desiredComponentId = componentId;
            }
            
            Optional<Entity> result = Iterables.tryFind(entitiesToSearch, EntityPredicates.configEqualTo(BrooklynCampConstants.PLAN_ID, desiredComponentId));
            
            if (result.isPresent()) {
                return Maybe.of(result.get());
            }
            
            // TODO may want to block and repeat on new entities joining?
            throw new NoSuchElementException("No entity matching id " + desiredComponentId+
                (scope==Scope.GLOBAL ? "" : ", in scope "+scope+" wrt "+entity+
                (scopeComponent!=null ? " ("+scopeComponent+" from "+entity()+")" : "")));
        }
        
        private ExecutionContext getExecutionContext() {
            EntityInternal contextEntity = (EntityInternal) BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
            ExecutionContext execContext =
                    (contextEntity != null) ? contextEntity.getExecutionContext()
                                            : BasicExecutionContext.getCurrentExecutionContext();
            if (execContext == null) {
                throw new IllegalStateException("No execution context available to resolve " + toString());
            }
            return execContext;
        }
    }
    
    // -------------------------------

    // DSL words which move to a new component
    
    public DslComponent entity(Object id) {
        return DslComponent.newInstance(this, Scope.GLOBAL, id);
    }
    public DslComponent child(Object id) {
        return DslComponent.newInstance(this, Scope.CHILD, id);
    }
    public DslComponent sibling(Object id) {
        return DslComponent.newInstance(this, Scope.SIBLING, id);
    }
    public DslComponent descendant(Object id) {
        return DslComponent.newInstance(this, Scope.DESCENDANT, id);
    }
    public DslComponent ancestor(Object id) {
        return DslComponent.newInstance(this, Scope.ANCESTOR, id);
    }
    public DslComponent root() {
        return new DslComponent(this, Scope.ROOT);
    }
    public DslComponent scopeRoot() {
        return new DslComponent(this, Scope.SCOPE_ROOT);
    }
    
    @Deprecated /** @deprecated since 0.7.0 */
    public DslComponent component(Object id) {
        return DslComponent.newInstance(this, Scope.GLOBAL, id);
    }
    
    public DslComponent self() {
        return new DslComponent(this, Scope.THIS);
    }
    
    public DslComponent parent() {
        return new DslComponent(this, Scope.PARENT);
    }
    
    public DslComponent component(String scope, Object id) {
        if (!DslComponent.Scope.isValid(scope)) {
            throw new IllegalArgumentException(scope + " is not a valid scope");
        }
        return DslComponent.newInstance(this, DslComponent.Scope.fromString(scope), id);
    }

    // DSL words which return things

    public BrooklynDslDeferredSupplier<?> entityId() {
        return new EntityId(this);
    }
    protected static class EntityId extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = -419427634694971033L;
        private final DslComponent component;

        public EntityId(DslComponent component) {
            this.component = Preconditions.checkNotNull(component);
        }

        @Override
        public Maybe<Object> getImmediately() {
            Maybe<Entity> targetEntityMaybe = component.getImmediately();
            if (targetEntityMaybe.isAbsent()) return Maybe.absent("Target entity not available");
            Entity targetEntity = targetEntityMaybe.get();

            return Maybe.<Object>of(targetEntity.getId());
        }
        
        @Override
        public Task<Object> newTask() {
            Entity targetEntity = component.get();
            return Tasks.create("identity", Callables.<Object>returning(targetEntity.getId()));
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component);
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            EntityId that = EntityId.class.cast(obj);
            return Objects.equal(this.component, that.component);
        }
        @Override
        public String toString() {
            return (component.scope==Scope.THIS ? "" : component.toString()+".") + "entityId()";
        }
    }

    public BrooklynDslDeferredSupplier<?> attributeWhenReady(final String sensorName) {
        return new AttributeWhenReady(this, sensorName);
    }
    protected static class AttributeWhenReady extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = 1740899524088902383L;
        private final DslComponent component;
        private final String sensorName;

        public AttributeWhenReady(DslComponent component, String sensorName) {
            this.component = Preconditions.checkNotNull(component);
            this.sensorName = sensorName;
        }

        @Override
        public final Maybe<Object> getImmediately() {
            Maybe<Entity> targetEntityMaybe = component.getImmediately();
            if (targetEntityMaybe.isAbsent()) return Maybe.absent("Target entity not available");
            Entity targetEntity = targetEntityMaybe.get();

            AttributeSensor<?> targetSensor = (AttributeSensor<?>) targetEntity.getEntityType().getSensor(sensorName);
            if (targetSensor == null) {
                targetSensor = Sensors.newSensor(Object.class, sensorName);
            }
            Object result = targetEntity.sensors().get(targetSensor);
            return GroovyJavaMethods.truth(result) ? Maybe.of(result) : Maybe.absent();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Task<Object> newTask() {
            Entity targetEntity = component.get();
            Sensor<?> targetSensor = targetEntity.getEntityType().getSensor(sensorName);
            if (!(targetSensor instanceof AttributeSensor<?>)) {
                targetSensor = Sensors.newSensor(Object.class, sensorName);
            }
            return (Task<Object>) DependentConfiguration.attributeWhenReady(targetEntity, (AttributeSensor<?>)targetSensor);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component, sensorName);
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            AttributeWhenReady that = AttributeWhenReady.class.cast(obj);
            return Objects.equal(this.component, that.component) &&
                    Objects.equal(this.sensorName, that.sensorName);
        }
        @Override
        public String toString() {
            return (component.scope==Scope.THIS ? "" : component.toString()+".") +
                "attributeWhenReady("+JavaStringEscapes.wrapJavaString(sensorName)+")";
        }
    }

    public BrooklynDslDeferredSupplier<?> config(final String keyName) {
        return new DslConfigSupplier(this, keyName);
    }
    protected final static class DslConfigSupplier extends BrooklynDslDeferredSupplier<Object> {
        private final DslComponent component;
        private final String keyName;
        private static final long serialVersionUID = -4735177561947722511L;

        public DslConfigSupplier(DslComponent component, String keyName) {
            this.component = Preconditions.checkNotNull(component);
            this.keyName = keyName;
        }

        @Override
        public final Maybe<Object> getImmediately() {
            Maybe<Entity> targetEntityMaybe = component.getImmediately();
            if (targetEntityMaybe.isAbsent()) return Maybe.absent("Target entity not available");
            EntityInternal targetEntity = (EntityInternal) targetEntityMaybe.get();

            ConfigKey<?> key = targetEntity.getEntityType().getConfigKey(keyName);
            Maybe<? extends Object> result = targetEntity.config().getNonBlocking(key != null ? key : ConfigKeys.newConfigKey(Object.class, keyName));
            return Maybe.<Object>cast(result);
        }

        @Override
        public Task<Object> newTask() {
            return Tasks.builder()
                    .displayName("retrieving config for "+keyName)
                    .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                    .dynamic(false)
                    .body(new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            Entity targetEntity = component.get();
                            ConfigKey<?> key = targetEntity.getEntityType().getConfigKey(keyName);
                            return targetEntity.getConfig(key != null ? key : ConfigKeys.newConfigKey(Object.class, keyName));
                        }})
                    .build();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component, keyName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslConfigSupplier that = DslConfigSupplier.class.cast(obj);
            return Objects.equal(this.component, that.component) &&
                    Objects.equal(this.keyName, that.keyName);
        }

        @Override
        public String toString() {
            return (component.scope==Scope.THIS ? "" : component.toString()+".") + 
                "config("+JavaStringEscapes.wrapJavaString(keyName)+")";
        }
    }

    // TODO
    // public BrooklynDslDeferredSupplier<?> relation(BrooklynObjectInternal obj, final String relationName) {...}

    public BrooklynDslDeferredSupplier<Sensor<?>> sensor(final Object sensorIndicator) {
        return new DslSensorSupplier(this, sensorIndicator);
    }
    protected final static class DslSensorSupplier extends BrooklynDslDeferredSupplier<Sensor<?>> {
        private final DslComponent component;
        private final Object sensorName;
        private static final long serialVersionUID = -4735177561947722511L;

        public DslSensorSupplier(DslComponent component, Object sensorIndicator) {
            this.component = Preconditions.checkNotNull(component);
            this.sensorName = sensorIndicator;
        }

        @Override
        public Maybe<Sensor<?>> getImmediately() {
            return getImmediately(sensorName, false);
        }
        
        protected Maybe<Sensor<?>> getImmediately(Object si, boolean resolved) {
            if (si instanceof Sensor) {
                return Maybe.<Sensor<?>>of((Sensor<?>)si);
            } else if (si instanceof String) {
                Maybe<Entity> targetEntityMaybe = component.getImmediately();
                if (targetEntityMaybe.isAbsent()) return Maybe.absent("Target entity not available");
                Entity targetEntity = targetEntityMaybe.get();

                Sensor<?> result = null;
                if (targetEntity!=null) {
                    result = targetEntity.getEntityType().getSensor((String)si);
                }
                if (result!=null) return Maybe.<Sensor<?>>of(result);
                return Maybe.<Sensor<?>>of(Sensors.newSensor(Object.class, (String)si));
            }
            if (!resolved) {
                // attempt to resolve, and recurse
                final ExecutionContext executionContext = ((EntityInternal)entity()).getExecutionContext();
                Maybe<Object> resolvedSi = Tasks.resolving(si, Object.class).deep(true).immediately(true).context(executionContext).getMaybe();
                if (resolvedSi.isAbsent()) return Maybe.absent();
                return getImmediately(resolvedSi.get(), true);
            }
            throw new IllegalStateException("Cannot resolve '"+sensorName+"' as a sensor (got type "+(si == null ? "null" : si.getClass().getName()+")"));
        }
        
        @Override
        public Task<Sensor<?>> newTask() {
            return Tasks.<Sensor<?>>builder()
                    .displayName("looking up sensor for "+sensorName)
                    .dynamic(false)
                    .body(new Callable<Sensor<?>>() {
                        @Override
                        public Sensor<?> call() throws Exception {
                            return resolve(sensorName, false);
                        }
                        
                        public Sensor<?> resolve(Object si, boolean resolved) throws ExecutionException, InterruptedException {
                            if (si instanceof Sensor) return (Sensor<?>)si;
                            if (si instanceof String) {
                                Entity targetEntity = component.get();
                                Sensor<?> result = null;
                                if (targetEntity!=null) {
                                    result = targetEntity.getEntityType().getSensor((String)si);
                                }
                                if (result!=null) return result;
                                return Sensors.newSensor(Object.class, (String)si);
                            }
                            if (!resolved) {
                                // attempt to resolve, and recurse
                                final ExecutionContext executionContext = ((EntityInternal)entity()).getExecutionContext();
                                return resolve(Tasks.resolveDeepValue(si, Object.class, executionContext), true);
                            }
                            throw new IllegalStateException("Cannot resolve '"+sensorName+"' as a sensor (got type "+(si == null ? "null" : si.getClass().getName()+")"));
                        }})
                    .build();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component, sensorName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslSensorSupplier that = DslSensorSupplier.class.cast(obj);
            return Objects.equal(this.component, that.component) &&
                    Objects.equal(this.sensorName, that.sensorName);
        }

        @Override
        public String toString() {
            return (component.scope==Scope.THIS ? "" : component.toString()+".") + 
                "sensor("+
                    (sensorName instanceof String ? JavaStringEscapes.wrapJavaString((String)sensorName) :
                        sensorName instanceof Sensor ? JavaStringEscapes.wrapJavaString(((Sensor<?>)sensorName).getName()) :
                        sensorName)+")";
        }
    }

    public static enum Scope {
        GLOBAL,
        CHILD,
        PARENT,
        SIBLING,
        DESCENDANT,
        ANCESTOR,
        ROOT,
        SCOPE_ROOT,
        THIS;

        private static Converter<String, String> converter = CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.UPPER_UNDERSCORE);

        public static Scope fromString(String name) {
            Maybe<Scope> parsed = tryFromString(name);
            return parsed.get();
        }

        public static Maybe<Scope> tryFromString(String name) {
            try {
                Scope scope = valueOf(converter.convert(name));
                return Maybe.of(scope);
            } catch (Exception cause) {
                return Maybe.absent(cause);
            }
        }

        public static boolean isValid(String name) {
            Maybe<Scope> check = tryFromString(name);
            return check.isPresentAndNonNull();
        }

        @Override
        public String toString() {
            return converter.reverse().convert(name());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(componentId, scopeComponent, scope);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DslComponent that = DslComponent.class.cast(obj);
        return Objects.equal(this.componentId, that.componentId) &&
                Objects.equal(this.scopeComponent, that.scopeComponent) &&
                Objects.equal(this.scope, that.scope);
    }

    @Override
    public String toString() {
        return "$brooklyn:entity("+
            (scopeComponent==null ? "" : JavaStringEscapes.wrapJavaString(scopeComponent.toString())+", ")+
            (scope==Scope.GLOBAL ? "" : JavaStringEscapes.wrapJavaString(scope.toString())+", ")+
            (componentId != null ? JavaStringEscapes.wrapJavaString(componentId) : componentIdSupplier)+
            ")";
    }

}