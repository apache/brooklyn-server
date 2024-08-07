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
import com.google.common.base.Objects;
import com.google.common.base.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Callables;
import com.thoughtworks.xstream.annotations.XStreamConverter;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslAccessible;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslFunctionSource;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.core.xstream.ObjectWithDefaultStringImplConverter;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.brooklyn.camp.brooklyn.spi.dsl.DslUtils.resolved;

public class DslComponent extends BrooklynDslDeferredSupplier<Entity> implements DslFunctionSource {

    private static final Logger log = LoggerFactory.getLogger(DslComponent.class);

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

    public static DslComponent newInstanceChangingScope(Scope scope, DslComponent old, Function<String,String> dslUpdateFn) {
        DslComponent result;
        if (old.componentIdSupplier!=null) result = new DslComponent(scope, old.componentIdSupplier);
        else if (old.componentId!=null) result = new DslComponent(scope, old.componentId);
        else result = new DslComponent(scope);

        if (dslUpdateFn!=null && old.dsl instanceof String) {
            result.dsl = dslUpdateFn.apply((String) old.dsl);
        } else {
            result.dsl = old.dsl;
        }
        return result;
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

    // for JSON deserialization only
    private DslComponent() {
        this.scopeComponent = null;
        this.componentId = null;
        this.componentIdSupplier = null;
        this.scope = null;
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

    public Scope getScope() {
        return scope;
    }

    public DeferredSupplier<?> getComponentIdSupplier() {
        return componentIdSupplier;
    }

    public String getComponentId() {
        return componentId;
    }

    public DslComponent getScopeComponent() {
        return scopeComponent;
    }

    @Override @JsonIgnore
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

        @Override @JsonIgnore
        public Maybe<Entity> getImmediately() {
            try {
                return callImpl(true);
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }

        @Override
        public Entity get() {
            try {
                return call();
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
                return Maybe.<Entity>ofDisallowingNull(entity()).or(Maybe.<Entity>absent("Context entity not available when trying to evaluate Brooklyn DSL"));
            }
        }

        static abstract class DslEntityResolver {
            Scope scope;
            boolean needsBaseEntity;
            boolean needsComponentId;
            Entity entity;
            String componentId;

            public void withScope(Scope scope) {
                this.scope = scope;
            }

            void withBaseEntity(Entity entity) {
                this.entity = entity;
            }
            void withComponentId(String componentId) {
                this.componentId = componentId;
            }

            abstract boolean test(Entity entity);
            abstract Maybe<Entity> resolve();
        }

        static class ApplicationEntityResolver extends DslEntityResolver {
            public ApplicationEntityResolver() {
                this.needsBaseEntity = false;
                this.needsComponentId = true;
            }
            boolean test(Entity entity) {
                return entity.getParent()==null;
            }

            @Override
            Maybe<Entity> resolve() {
                Entity result = managementContext().getEntityManager().getEntity(componentId);

                Entity nonAppMatch = null;
                if (result!=null) {
                    if (test(result)) Maybe.of(result);
                    nonAppMatch = result;
                }

                List<Entity> allMatches = managementContext().getEntityManager().getEntities().stream()
                        .filter(EntityPredicates.configEqualTo(BrooklynCampConstants.PLAN_ID, componentId))
                        .collect(Collectors.toList());

                List<Entity> appMatches = allMatches.stream().filter(this::test).collect(Collectors.toList());

                if (!appMatches.isEmpty()) {
                    if (appMatches.size() > 1) {
                        log.warn("Multiple matches for '"+componentId+"', returning the first: "+appMatches);
                    }
                    return Maybe.of(appMatches.iterator().next());
                }

                if (nonAppMatch!=null) allMatches = MutableList.of(nonAppMatch).appendAll(allMatches);
                return Maybe.absent("No application entity matching ID '"+componentId+"'" +
                        (allMatches.isEmpty() ? "" : "; non-application entity matches: "+allMatches));
            }
        }

        static class SingleRelativeEntityResolver extends DslEntityResolver {
            private final Function<Entity, Maybe<Entity>> relativeFinder;

            public SingleRelativeEntityResolver(Function<Entity,Entity> relativeFinder) {
                this.needsBaseEntity = true;
                this.relativeFinder = input -> {
                    Entity result = relativeFinder.apply(input);
                    if (result==null) return Maybe.absent("No entity found for scope "+scope+" realtive to "+entity);
                    else return Maybe.of(result);
                };
            }

            @Override
            boolean test(Entity entity) {
                return false;
            }

            @Override
            Maybe<Entity> resolve() {
                return relativeFinder.apply(entity);
            }
        }

        static class AcceptableEntityResolver extends DslEntityResolver {
            Function<Entity,java.util.function.Predicate<Entity>> acceptableEntityProducer;
            java.util.function.Predicate<Entity> acceptableEntity;

            public AcceptableEntityResolver(Function<Entity,java.util.function.Predicate<Entity>> acceptableEntityProducer) {
                this.needsBaseEntity = true;
                this.needsComponentId = true;
                this.acceptableEntityProducer = acceptableEntityProducer;
            }

            @Override
            boolean test(Entity entity) {
                return acceptableEntity.test(entity);
            }

            @Override
            void withBaseEntity(Entity entity) {
                super.withBaseEntity(entity);
                this.acceptableEntity = acceptableEntityProducer.apply(entity);
            }

            @Override
            Maybe<Entity> resolve() {
                // TODO inefficient when looking at descendants or ancestors, as it also traverses in the other direction,
                // and compares against a pre-determined set. ideally the traversal is more scope- or direction- aware.

                List<Entity> firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(entity, true,
                        Predicates.and(EntityPredicates.configEqualTo(BrooklynCampConstants.PLAN_ID, componentId), acceptableEntity::test)::apply);
                if (firstGroupOfMatches.isEmpty()) {
                    firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(entity, true,
                            Predicates.and(EntityPredicates.idEqualTo(componentId), acceptableEntity::test)::apply);
                }
                if (!firstGroupOfMatches.isEmpty()) {
                    return Maybe.of(firstGroupOfMatches.get(0));
                }

                // could be nice if DSL has an extra .block() method to allow it to wait for a matching entity.
                // previously we threw if nothing existed; now we return an absent with a detailed error
                return Maybe.absent(new NoSuchElementException("No entity matching id '" + componentId+"'"+
                        (scope==Scope.GLOBAL ? " near entity " : " in scope "+scope+" of ")+entity));
            }
        }

        static DslEntityResolver getResolverForScope(Scope scope) {
            switch (scope) {
                // component-less items
                case THIS:
                    return new SingleRelativeEntityResolver(entity -> entity);
                case PARENT:
                    return new SingleRelativeEntityResolver(Entity::getParent);
                case ROOT:
                    return new SingleRelativeEntityResolver(Entity::getApplication);
                case SCOPE_ROOT:
                    return new SingleRelativeEntityResolver(Entities::catalogItemScopeRoot);

                case APPLICATIONS:
                    return new ApplicationEntityResolver();

                case GLOBAL:
                    return new AcceptableEntityResolver(entity -> {
                        if (Entities.isManaged(entity)) {
                            // use management context if entity is managed (usual case, more efficient)
                            String appId = entity.getApplicationId();
                            return ee -> appId != null && appId.equals(ee.getApplicationId());

                        } else {
                            // otherwise traverse the application (probably we could drop this)
                            if (entity != null && entity.getApplication() != null) {
                                Set<Entity> toVisit = MutableSet.of(entity.getApplication()), visited = MutableSet.of(entity.getApplication());
                                while (!toVisit.isEmpty()) {
                                    Set<Entity> visiting = MutableSet.copyOf(toVisit);
                                    toVisit.clear();
                                    visiting.forEach(e -> {
                                        e.getChildren().forEach(ec -> {
                                            if (visited.add(ec)) toVisit.add(ec);
                                        });
                                    });
                                }
                                return visited::contains;
                            } else {
                                // accept any (again probably we could drop this)
                                return x -> true;
                            }
                        }
                    });

                case DESCENDANT:
                    return new AcceptableEntityResolver(entity -> MutableSet.copyOf(Entities.descendantsWithoutSelf(entity))::contains);
                case MEMBERS:
                    return new AcceptableEntityResolver(entity -> MutableSet.copyOf(Entities.descendantsAndMembersWithoutSelf(entity))::contains);
                case MEMBERS_ONLY:
                    return new AcceptableEntityResolver(entity -> {
                        Set<Entity> acceptable = MutableSet.of();
                        if (entity instanceof Group) acceptable.addAll( ((Group)entity).getMembers() );
                        return acceptable::contains;
                    });
                case ANCESTOR:
                    return new AcceptableEntityResolver(entity -> MutableSet.copyOf(Entities.ancestorsWithoutSelf(entity))::contains);
                case SIBLING:
                    return new AcceptableEntityResolver(entity -> {
                        Predicate<Entity> notSelfPredicate = Predicates.not(Predicates.<Entity>equalTo(entity));
                        return MutableSet.copyOf(Iterables.filter(entity.getParent().getChildren(), notSelfPredicate))::contains;
                    });
                case CHILD:
                    return new AcceptableEntityResolver(entity -> MutableSet.copyOf(entity.getChildren())::contains);
                default:
                    throw new IllegalStateException("Unexpected scope "+scope);
            }
        }

        protected Maybe<Entity> callImpl(boolean immediate) throws Exception {
            DslEntityResolver resolver = getResolverForScope(scope);
            resolver.withScope(scope);

            if (resolver.needsBaseEntity) {
                Maybe<Entity> entityMaybe = getEntity(immediate);
                if (immediate && entityMaybe.isAbsent()) {
                    return entityMaybe;
                }
                resolver.withBaseEntity( entityMaybe.get() );
            }

            if (resolver.needsComponentId) {
                if (componentId == null) {
                    if (componentIdSupplier == null) {
                        throw new IllegalArgumentException("No component-id or component-id supplier, when resolving entity in scope '" + scope + "' wrt " + resolver.entity);
                    }

                    Maybe<Object> maybeComponentId = Tasks.resolving(componentIdSupplier)
                            .as(Object.class)
                            .context(getExecutionContext())
                            .immediately(immediate)
                            .description("Resolving component-id from " + componentIdSupplier)
                            .getMaybe();

                    if (immediate) {
                        if (maybeComponentId.isAbsent()) {
                            return ImmediateValueNotAvailableException.newAbsentWrapping("Cannot find component ID", maybeComponentId);
                        }
                    }

                    // Support being passed an explicit entity via the DSL
                    Object candidate = maybeComponentId.get();
                    if (candidate instanceof BrooklynObject) {
                        resolver.withComponentId( ((BrooklynObject)candidate).getId() );

                        if (resolver.test((Entity) candidate)) {
                            return Maybe.of((Entity) candidate);
                        } else {
                            throw new IllegalStateException("Resolved component " + maybeComponentId.get() + " is not in scope '" + scope + "' wrt " + resolver.entity);
                        }
                    }

                    resolver.withComponentId( TypeCoercions.coerce(maybeComponentId.get(), String.class) );

                    if (Strings.isBlank(resolver.componentId)) {
                        throw new IllegalStateException("component-id blank, from " + componentIdSupplier);
                    }

                } else {
                    resolver.withComponentId( componentId );
                }
            }

            return resolver.resolve();
        }
        
        private ExecutionContext getExecutionContext() {
            return findExecutionContext(this);
        }
    }

    static ExecutionContext findExecutionContext(Object callerContext) {
        ExecutionContext execContext = BrooklynTaskTags.getCurrentExecutionContext();
        if (execContext == null) {
            throw new IllegalStateException("No execution context available to resolve " + callerContext);
        }
        return execContext;
    }

    // -------------------------------

    // DSL words which move to a new component
    
    @DslAccessible
    public DslComponent entity(Object id) {
        return DslComponent.newInstance(this, Scope.GLOBAL, id);
    }
    @DslAccessible
    public DslComponent child(Object id) {
        return DslComponent.newInstance(this, Scope.CHILD, id);
    }
    @DslAccessible
    public DslComponent sibling(Object id) {
        return DslComponent.newInstance(this, Scope.SIBLING, id);
    }
    @DslAccessible
    public DslComponent descendant(Object id) {
        return DslComponent.newInstance(this, Scope.DESCENDANT, id);
    }
    @DslAccessible
    public DslComponent ancestor(Object id) {
        return DslComponent.newInstance(this, Scope.ANCESTOR, id);
    }
    @DslAccessible
    public DslComponent root() {
        return new DslComponent(this, Scope.ROOT);
    }
    @DslAccessible
    public DslComponent scopeRoot() {
        return new DslComponent(this, Scope.SCOPE_ROOT);
    }
    
    @Deprecated /** @deprecated since 0.7.0 */
    @DslAccessible
    public DslComponent component(Object id) {
        return DslComponent.newInstance(this, Scope.GLOBAL, id);
    }
    
    @DslAccessible
    public DslComponent self() {
        return new DslComponent(this, Scope.THIS);
    }
    
    @DslAccessible
    public DslComponent parent() {
        return new DslComponent(this, Scope.PARENT);
    }
    
    @DslAccessible
    public DslComponent component(String scope, Object id) {
        if (!DslComponent.Scope.isValid(scope)) {
            throw new IllegalArgumentException(scope + " is not a valid scope");
        }
        return DslComponent.newInstance(this, DslComponent.Scope.fromString(scope), id);
    }

    @DslAccessible
    public DslComponent application(Object id) {
        return DslComponent.newInstance(this, Scope.APPLICATIONS, id);
    }

    @DslAccessible
    public DslComponent member(Object id) {
        return DslComponent.newInstance(this, Scope.MEMBERS, id);
    }

    // DSL words which return things

    @DslAccessible
    public BrooklynDslDeferredSupplier<?> entityId() {
        return new EntityId(this);
    }
    protected static class EntityId extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = -419427634694971033L;
        private final DslComponent component;

        public EntityId(DslComponent component) {
            this.component = Preconditions.checkNotNull(component);
        }

        @Override @JsonIgnore
        public Maybe<Object> getImmediately() {
            Maybe<Entity> targetEntityMaybe = component.getImmediately();
            if (targetEntityMaybe.isAbsent()) return ImmediateValueNotAvailableException.newAbsentWrapping("Target entity is not available: "+component, targetEntityMaybe);
            Entity targetEntity = targetEntityMaybe.get();

            return Maybe.of(targetEntity.getId());
        }
        
        @Override
        public Task<Object> newTask() {
            Entity targetEntity = component.get();
            return Tasks.create("identity", Callables.returning(targetEntity.getId()));
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
        public String toDslString(boolean yamlAllowed) {
            return DslToStringHelpers.chainFunctionOnComponent(yamlAllowed, component, "entityId");
        }
    }

    @DslAccessible
    public BrooklynDslDeferredSupplier<?> attributeWhenReady(final Object sensorNameOrSupplier) {
        return new AttributeWhenReady(this, sensorNameOrSupplier);
    }
    @DslAccessible
    public BrooklynDslDeferredSupplier<?> attributeWhenReadyAllowingOnFire(final Object sensorNameOrSupplier) {
        return new AttributeWhenReady(this, sensorNameOrSupplier, DependentConfiguration.AttributeWhenReadyOptions.allowingOnFireMap());
    }
    @DslAccessible
    public BrooklynDslDeferredSupplier<?> attributeWhenReady(final Object sensorNameOrSupplier, Map options) {
        return new AttributeWhenReady(this, sensorNameOrSupplier, options);
    }
    public static class AttributeWhenReady extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = 1740899524088902383L;
        private final DslComponent component;
        @XStreamConverter(ObjectWithDefaultStringImplConverter.class)
        private final Object sensorName;
        private final Map options;

        // JSON deserialization only
        private AttributeWhenReady() {
            this.component = null;
            this.sensorName = null;
            this.options = null;
        }
        public AttributeWhenReady(DslComponent component, Object sensorName) {
            this(component, sensorName, null);
        }
        public AttributeWhenReady(DslComponent component, Object sensorName, Map opts) {
            this.component = Preconditions.checkNotNull(component);
            this.sensorName = sensorName;
            this.options = opts;
        }

        public Object getSensorName() {
            return sensorName;
        }

        public DslComponent getComponent() {
            return component;
        }

        protected String resolveSensorName(boolean immediately) {
            if (sensorName instanceof String) {
                return (String)sensorName;
            }
            
            return Tasks.resolving(sensorName)
                .as(String.class)
                .context(findExecutionContext(this))
                .immediately(immediately)
                .description("Resolving sensorName from " + sensorName)
                .get();
        }
        
        @Override @JsonIgnore
        public final Maybe<Object> getImmediately() {
            Maybe<Entity> targetEntityMaybe = component.getImmediately();
            if (targetEntityMaybe.isAbsent()) return ImmediateValueNotAvailableException.newAbsentWrapping("Target entity not available: "+component, targetEntityMaybe);
            Entity targetEntity = targetEntityMaybe.get();

            String sensorNameS = resolveSensorName(true);
            AttributeSensor<?> targetSensor = (AttributeSensor<?>) targetEntity.getEntityType().getSensor(sensorNameS);
            if (targetSensor == null) {
                targetSensor = Sensors.newSensor(Object.class, sensorNameS);
            }
            Object result = targetEntity.sensors().get(targetSensor);
            AttributeSensor<?> ts2 = targetSensor;
            return JavaGroovyEquivalents.groovyTruth(result) ? Maybe.of(result) : ImmediateValueNotAvailableException.newAbsentWithExceptionSupplier(() -> "Sensor '"+ts2+"' on "+targetEntity+" not immediately available");
        }

        @SuppressWarnings("unchecked")
        @Override
        public Task<Object> newTask() {
            Entity targetEntity = component.get();
            
            String sensorNameS = resolveSensorName(false);
            Sensor<?> targetSensor = targetEntity.getEntityType().getSensor(sensorNameS);
            if (!(targetSensor instanceof AttributeSensor<?>)) {
                targetSensor = Sensors.newSensor(Object.class, sensorNameS);
            }
            return (Task<Object>) DependentConfiguration.attributeWhenReady(targetEntity, (AttributeSensor<?>)targetSensor, options!=null ?
                    TypeCoercions.coerce(options, DependentConfiguration.AttributeWhenReadyOptions.class) : DependentConfiguration.AttributeWhenReadyOptions.defaultOptions());
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
        public String toDslString(boolean yamlAllowed) {
            if (options!=null && DependentConfiguration.AttributeWhenReadyOptions.allowingOnFireMap().equals(options)) {
                return DslToStringHelpers.chainFunctionOnComponent1(yamlAllowed, component,
                        "attributeWhenReadyAllowingOnFire", sensorName);
            }
            return DslToStringHelpers.chainFunctionOnComponent(yamlAllowed, component,
                    "attributeWhenReady", MutableList.of(sensorName).appendIfNotNull(options));
        }
    }

    @DslAccessible
    public BrooklynDslDeferredSupplier<?> config(final Object keyNameOrSupplier) {
        return new DslConfigSupplier(this, keyNameOrSupplier);
    }
    public final static class DslConfigSupplier extends BrooklynDslDeferredSupplier<Object> {
        private final DslComponent component;
        @XStreamConverter(ObjectWithDefaultStringImplConverter.class)
        private final Object keyName;
        private static final long serialVersionUID = -4735177561947722511L;

        // JSON-only constructor
        private DslConfigSupplier() {
            component = null;
            keyName = null;
        }
        public DslConfigSupplier(DslComponent component, Object keyName) {
            this.component = Preconditions.checkNotNull(component);
            this.keyName = keyName;
        }

        public Object getKeyName() {
            return keyName;
        }

        public DslComponent getComponent() {
            return component;
        }

        protected String resolveKeyName(boolean immediately) {
            if (keyName instanceof String) {
                return (String)keyName;
            }
            
            return Tasks.resolving(keyName)
                .as(String.class)
                .context(findExecutionContext(this))
                .immediately(immediately)
                .description("Resolving key name from " + keyName)
                .get();
        }
        
        @Override @JsonIgnore
        public final Maybe<Object> getImmediately() {
            Maybe<Object> maybeWrappedMaybe = findExecutionContext(this).getImmediately(newCallableReturningImmediateMaybeOrNonImmediateValue(true));
            // the answer will be wrapped twice due to the callable semantics;
            // the inner present/absent is important; it will only get an outer absent if interrupted
            if (maybeWrappedMaybe.isAbsent()) return maybeWrappedMaybe;
            return Maybe.<Object>cast( (Maybe<?>) maybeWrappedMaybe.get() );
        }

        @Override
        public Task<Object> newTask() {
            return Tasks.builder()
                    .displayName("retrieving config for "+keyName)
                    .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                    .dynamic(false)
                    .body(newCallableReturningImmediateMaybeOrNonImmediateValue(false)).build();
        }

        private Callable<Object> newCallableReturningImmediateMaybeOrNonImmediateValue(final boolean immediate) {
            return new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Entity targetEntity;
                    if (immediate) { 
                        Maybe<Entity> targetEntityMaybe = component.getImmediately();
                        if (targetEntityMaybe.isAbsent()) return Maybe.<Object>cast(targetEntityMaybe);
                        targetEntity = (EntityInternal) targetEntityMaybe.get();
                    } else {
                        targetEntity = component.get();
                    }
                    
                    // this is always run in a new dedicated task (possibly a fake task if immediate), so no need to clear
                    String tag = "DSL:entity('"+targetEntity.getId()+"').config('"+keyName+"')";
                    checkAndTagForRecursiveReference(targetEntity, tag);

                    String keyNameS = resolveKeyName(true);
                    ConfigKey<?> key = targetEntity.getEntityType().getConfigKey(keyNameS);
                    if (key==null) key = ConfigKeys.newConfigKey(Object.class, keyNameS);
                    if (immediate) {
                        return ((EntityInternal)targetEntity).config().getNonBlocking(key, true);
                    } else {
                        return targetEntity.getConfig(key);
                    }
                }
            };
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
        public String toDslString(boolean yamlAllowed) {
            return DslToStringHelpers.chainFunctionOnComponent1(yamlAllowed, component, "config", keyName);
        }
    }

    // TODO
    // public BrooklynDslDeferredSupplier<?> relation(BrooklynObjectInternal obj, final String relationName) {...}

    @DslAccessible
    public BrooklynDslDeferredSupplier<Sensor<?>> sensor(final Object sensorIndicator) {
        return new DslSensorSupplier(this, sensorIndicator);
    }
    public final static class DslSensorSupplier extends BrooklynDslDeferredSupplier<Sensor<?>> {
        private final DslComponent component;
        @XStreamConverter(ObjectWithDefaultStringImplConverter.class)
        private final Object sensorName;
        private static final long serialVersionUID = -4735177561947722511L;

        public DslSensorSupplier(DslComponent component, Object sensorIndicator) {
            this.component = Preconditions.checkNotNull(component);
            this.sensorName = sensorIndicator;
        }

        public Object getSensorName() {
            return sensorName;
        }

        public DslComponent getComponent() {
            return component;
        }

        @Override @JsonIgnore
        public Maybe<Sensor<?>> getImmediately() {
            return getImmediately(sensorName, false);
        }
        
        protected Maybe<Sensor<?>> getImmediately(Object si, boolean resolved) {
            if (si instanceof Sensor) {
                return Maybe.<Sensor<?>>of((Sensor<?>)si);
            } else if (si instanceof String) {
                Maybe<Entity> targetEntityMaybe = component.getImmediately();
                if (targetEntityMaybe.isAbsent()) return ImmediateValueNotAvailableException.newAbsentWrapping("Target entity is not available: "+component, targetEntityMaybe);
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
                final ExecutionContext executionContext = entity().getExecutionContext();
                Maybe<Object> resolvedSi = Tasks.resolving(si, Object.class).deep().immediately(true).context(executionContext).getMaybe();
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
                                final ExecutionContext executionContext = entity().getExecutionContext();
                                return resolve(Tasks.resolveDeepValueWithoutCoercion(si, executionContext), true);
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
        public String toDslString(boolean yamlAllowed) {
            return DslToStringHelpers.chainFunctionOnComponent1(yamlAllowed, component, "sensorName",
                sensorName instanceof Sensor ? ((Sensor<?>)sensorName).getName() : sensorName);
        }
    }

    @DslAccessible
    public BrooklynDslDeferredSupplier<Object> location() {
        return new DslLocationSupplier(this, 0);
    }
    
    @DslAccessible
    public BrooklynDslDeferredSupplier<Object> location(Object index) {
        return new DslLocationSupplier(this, index);
    }

    public final static class DslLocationSupplier extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = 5597335296158584040L;
        private final DslComponent component;
        private final Object index;
        
        public DslLocationSupplier(DslComponent component, Object index) {
            this.component = Preconditions.checkNotNull(component);
            this.index = index;
        }

        public Object getIndex() {
            return index;
        }

        public DslComponent getComponent() {
            return component;
        }

        @Override @JsonIgnore
        public final Maybe<Object> getImmediately() {
            Callable<Object> job = new Callable<Object>() {
                @Override public Object call() {
                    Maybe<Entity> targetEntityMaybe = component.getImmediately();
                    if (targetEntityMaybe.isAbsent()) return ImmediateValueNotAvailableException.newAbsentWrapping("Target entity not available: "+component, targetEntityMaybe);
                    Entity targetEntity = targetEntityMaybe.get();
        
                    int indexI = resolveIndex(true);
                    
                    Collection<Location> locations = getLocations(targetEntity);
                    if (locations.isEmpty()) {
                        throw new ImmediateValueNotAvailableException("Target entity has no locations: "+component);
                    } else if (locations.size() < (indexI + 1)) {
                        throw new IndexOutOfBoundsException("Target entity ("+component+") has "+locations.size()+" location(s), but requested index "+index);
                    }
                    Location result = Iterables.get(locations, indexI);
                    if (result == null) {
                        throw new NullPointerException("Target entity ("+component+") has null location at index "+index);
                    }
                    return result;
                }
            };
            
            return findExecutionContext(this).getImmediately(job);
        }

        // Pattern copied from DslConfigSupplier; see that for explanation
        @Override
        public Task<Object> newTask() {
            boolean immediate = false;
            
            Callable<Object> job = new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Entity targetEntity = component.get();
                    
                    int indexI = resolveIndex(immediate);
                    
                    // this is always run in a new dedicated task (possibly a fake task if immediate), so no need to clear
                    String tag = "DSL:entity('"+targetEntity.getId()+"').location('"+indexI+"')";
                    checkAndTagForRecursiveReference(targetEntity, tag);

                    // TODO Try repeatedly if no location(s)?
                    Collection<Location> locations = getLocations(targetEntity);
                    if (locations.size() < (indexI + 1)) {
                        throw new IndexOutOfBoundsException("Target entity ("+component+") has "+locations.size()+" location(s), but requested index "+index);
                    }
                    Location result = Iterables.get(locations, indexI);
                    if (result == null) {
                        throw new NullPointerException("Target entity ("+component+") has null location at index "+index);
                    }
                    return result;
                }
            };
            
            return Tasks.builder()
                    .displayName("retrieving locations["+index+"] for "+component)
                    .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                    .dynamic(false)
                    .body(job).build();
        }

        private int resolveIndex(boolean immediately) {
            if (index instanceof String || index instanceof Number) {
                return TypeCoercions.coerce(index, Integer.class);
            }
            
            Integer result = Tasks.resolving(index)
                .as(Integer.class)
                .context(findExecutionContext(this))
                .immediately(immediately)
                .description("Resolving index from " + index)
                .get();
            return result;
        }
        
        private Collection<Location> getLocations(Entity entity) {
            // TODO Arguably this should not look at ancestors. For example, in a `SoftwareProcess`
            // then after start() its location with be a `MachineLocation`. But before start has 
            // completed, we'll retrieve the `MachineProvisioningLocation` from its parent.
            
            Collection<? extends Location> locations = entity.getLocations();
            locations = Locations.getLocationsCheckingAncestors(locations, entity);
            return ImmutableList.copyOf(locations);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component);
        }
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslLocationSupplier that = DslLocationSupplier.class.cast(obj);
            return Objects.equal(this.component, that.component) &&
                    Objects.equal(this.index, that.index);
        }
        @Override
        public String toDslString(boolean yamlAllowed) {
            return DslToStringHelpers.chainFunctionOnComponent1(yamlAllowed, component, "location", index);
        }
    }

    public Object template(Object template) {
        return new DslTemplate(this, template);
    }

    public Object template(Object template, Map<?, ?> substitutions) {
        return new DslTemplate(this, template, substitutions);
    }

    public final static class DslTemplate extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = -585564936781673667L;
        private DslComponent component;
        private Object template;
        private Map<?, ?> substitutions;

        public DslTemplate(DslComponent component, Object template) {
            this(component, template, ImmutableMap.of());
        }

        public DslTemplate(DslComponent component, Object template, Map<?, ?> substitutions) {
            this.component = component;
            this.template = template;
            this.substitutions = substitutions;
        }

        public DslComponent getComponent() {
            return component;
        }

        public Object getTemplate() {
            return template;
        }

        public Map<?, ?> getSubstitutions() {
            return substitutions;
        }

        private String resolveTemplate(boolean immediately) {
            if (template instanceof String) {
                return (String)template;
            }
            
            return Tasks.resolving(template)
                .as(String.class)
                .context(findExecutionContext(this))
                .immediately(immediately)
                .description("Resolving template from " + template)
                .get();
        }
        
        @SuppressWarnings("unchecked")
        private Map<String, ?> resolveSubstitutions(boolean immediately) {
            return (Map<String, ?>) Tasks.resolving(substitutions)
                    .as(Object.class)
                    .context(findExecutionContext(this))
                    .immediately(immediately)
                    .deep()
                    .description("Resolving substitutions " + substitutions + " for template " + template)
                    .get();
        }

        @Override @JsonIgnore
        public Maybe<Object> getImmediately() {
            String resolvedTemplate = resolveTemplate(true);
            Map<String, ?> resolvedSubstitutions = resolveSubstitutions(true);

            Maybe<Entity> targetEntityMaybe = component.getImmediately();
            if (targetEntityMaybe.isAbsent()) return ImmediateValueNotAvailableException.newAbsentWrapping("Target entity is not available: "+component, targetEntityMaybe);
            Entity targetEntity = targetEntityMaybe.get();

            String evaluatedTemplate = TemplateProcessor.processTemplateContents(
                    "$brooklyn:template", resolvedTemplate, (EntityInternal)targetEntity, resolvedSubstitutions);
            return Maybe.of(evaluatedTemplate);
        }

        @Override
        public Task<Object> newTask() {
            return Tasks.<Object>builder().displayName("evaluating template "+template ).dynamic(false).body(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Entity targetEntity = component.get();
                    Map<String, ?> resolvedSubstitutions = resolveSubstitutions(false);
                    return TemplateProcessor.processTemplateContents("$brooklyn:template",
                            resolveTemplate(false), (EntityInternal)targetEntity, resolvedSubstitutions);
                }
            }).build();
        }

        @Override
        public String toDslString(boolean yamlAllowed) {
            return DslToStringHelpers.chainFunctionOnComponent(yamlAllowed, component, "template", MutableList.of(template).appendIfNotNull(substitutions));
        }
    }

    public static enum Scope {
        APPLICATIONS,
        GLOBAL,
        CHILD,
        PARENT,
        SIBLING,
        DESCENDANT,
        MEMBERS,
        MEMBERS_ONLY,
        ANCESTOR,
        ROOT,
        /** root node of blueprint where the the DSL is used; usually the depth in ancestor,
         *  though specially treated in CampResolver to handle usage within blueprints */
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
    public String toDslString(boolean yamlAllowed) {
        Object component = componentId != null ? componentId : componentIdSupplier;
        
        if (scope==Scope.GLOBAL) {
            return DslToStringHelpers.fn1(yamlAllowed, "entity", component);
        }
        
        if (scope==Scope.THIS) {
            if (scopeComponent!=null) {
                return scopeComponent.toString();
            }
            return DslToStringHelpers.fn(yamlAllowed, "entity", "this", "");
        }
        
        String remainder;
        if (component==null || "".equals(component)) {
            return DslToStringHelpers.chainFunctionOnComponent(yamlAllowed, scopeComponent, scope.toString());
        } else {
            return DslToStringHelpers.chainFunctionOnComponent1(yamlAllowed, scopeComponent, scope.toString(), component);
        }

    }
    
}
