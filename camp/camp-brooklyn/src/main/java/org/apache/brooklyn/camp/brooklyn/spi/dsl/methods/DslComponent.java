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

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Callables;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.EntityManagerInternal;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;

public class DslComponent extends BrooklynDslDeferredSupplier<Entity> {

    private static final long serialVersionUID = -7715984495268724954L;
    
    private final String componentId;
    private final DslComponent scopeComponent;
    private final Scope scope;

    /**
     * Resolve componentId in the {@link Scope#GLOBAL} scope.
     */
    public DslComponent(String componentId) {
        this(Scope.GLOBAL, componentId);
    }

    /**
     * Resolve componentId in scope relative to the current
     * {@link BrooklynTaskTags#getTargetOrContextEntity) target or context} entity.
     */
    public DslComponent(Scope scope, String componentId) {
        this(null, scope, componentId);
    }

    /**
     * Resolve componentId in scope relative to scopeComponent.
     */
    public DslComponent(DslComponent scopeComponent, Scope scope, String componentId) {
        Preconditions.checkNotNull(scope, "scope");
        this.scopeComponent = scopeComponent;
        this.componentId = componentId;
        this.scope = scope;
    }

    // ---------------------------
    
    @Override
    public Task<Entity> newTask() {
        return TaskBuilder.<Entity>builder().displayName(toString()).tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
            .body(new EntityInScopeFinder(scopeComponent, scope, componentId)).build();
    }
    
    protected static class EntityInScopeFinder implements Callable<Entity> {
        protected final DslComponent scopeComponent;
        protected final Scope scope;
        protected final String componentId;

        public EntityInScopeFinder(DslComponent scopeComponent, Scope scope, String componentId) {
            this.scopeComponent = scopeComponent;
            this.scope = scope;
            this.componentId = componentId;
        }

        protected EntityInternal getEntity() {
            if (scopeComponent!=null) {
                return (EntityInternal)scopeComponent.get();
            } else {
                return entity();
            }
        }
        
        @Override
        public Entity call() throws Exception {
            Iterable<Entity> entitiesToSearch = null;
            EntityInternal entity = getEntity();
            Predicate<Entity> notSelfPredicate = Predicates.not(Predicates.<Entity>equalTo(entity));

            switch (scope) {
                case THIS:
                    return entity;
                case PARENT:
                    return entity.getParent();
                case GLOBAL:
                    entitiesToSearch = ((EntityManagerInternal)entity.getManagementContext().getEntityManager())
                        .getAllEntitiesInApplication( entity().getApplication() );
                    break;
                case ROOT:
                    return entity.getApplication();
                case SCOPE_ROOT:
                    return Entities.catalogItemScopeRoot(entity);
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
            
            Optional<Entity> result = Iterables.tryFind(entitiesToSearch, EntityPredicates.configEqualTo(BrooklynCampConstants.PLAN_ID, componentId));
            
            if (result.isPresent())
                return result.get();
            
            // TODO may want to block and repeat on new entities joining?
            throw new NoSuchElementException("No entity matching id " + componentId+
                (scope==Scope.GLOBAL ? "" : ", in scope "+scope+" wrt "+entity+
                (scopeComponent!=null ? " ("+scopeComponent+" from "+entity()+")" : "")));
        }        
    }
    
    // -------------------------------

    // DSL words which move to a new component
    
    public DslComponent entity(String scopeOrId) {
        return new DslComponent(this, Scope.GLOBAL, scopeOrId);
    }
    public DslComponent child(String scopeOrId) {
        return new DslComponent(this, Scope.CHILD, scopeOrId);
    }
    public DslComponent sibling(String scopeOrId) {
        return new DslComponent(this, Scope.SIBLING, scopeOrId);
    }
    public DslComponent descendant(String scopeOrId) {
        return new DslComponent(this, Scope.DESCENDANT, scopeOrId);
    }
    public DslComponent ancestor(String scopeOrId) {
        return new DslComponent(this, Scope.ANCESTOR, scopeOrId);
    }
    public DslComponent root() {
        return new DslComponent(this, Scope.ROOT, "");
    }
    public DslComponent scopeRoot() {
        return new DslComponent(this, Scope.SCOPE_ROOT, "");
    }
    
    @Deprecated /** @deprecated since 0.7.0 */
    public DslComponent component(String scopeOrId) {
        return new DslComponent(this, Scope.GLOBAL, scopeOrId);
    }
    
    public DslComponent self() {
        return new DslComponent(this, Scope.THIS, null);
    }
    
    public DslComponent parent() {
        return new DslComponent(this, Scope.PARENT, "");
    }
    
    public DslComponent component(String scope, String id) {
        if (!DslComponent.Scope.isValid(scope)) {
            throw new IllegalArgumentException(scope + " is not a vlaid scope");
        }
        return new DslComponent(this, DslComponent.Scope.fromString(scope), id);
    }

    // DSL words which return things

    public BrooklynDslDeferredSupplier<?> entityId() {
        return new EntityId(this);
    }
    protected static class EntityId extends BrooklynDslDeferredSupplier<Object> {
        private final DslComponent component;

        public EntityId(DslComponent component) {
            this.component = Preconditions.checkNotNull(component);
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

    public BrooklynDslDeferredSupplier<?> attribute(final String sensorName) {
        return new AttributeWhenReady(this, sensorName, Predicates.notNull());
    }
    public BrooklynDslDeferredSupplier<?> attributeWhenReady(final String sensorName) {
        return new AttributeWhenReady(this, sensorName);
    }
    protected static class AttributeWhenReady extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = 1740899524088903494L;
        private final DslComponent component;
        private final String sensorName;
        private final Predicate readyCondition;

        public AttributeWhenReady(DslComponent component, String sensorName) {
            this(component, sensorName, null);
        }
        public AttributeWhenReady(DslComponent component, String sensorName, Predicate<?> readyCondition) {
            this.component = Preconditions.checkNotNull(component);
            this.sensorName = sensorName;
            this.readyCondition = readyCondition;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Task<Object> newTask() {
            Entity targetEntity = component.get();
            Sensor<?> targetSensor = targetEntity.getEntityType().getSensor(sensorName);
            if (!(targetSensor instanceof AttributeSensor<?>)) {
                targetSensor = Sensors.newSensor(Object.class, sensorName);
            }
            if (readyCondition != null) {
                return (Task<Object>) DependentConfiguration.attributeWhenReady(targetEntity, (AttributeSensor<?>) targetSensor, readyCondition);
            } else {
                return (Task<Object>) DependentConfiguration.attributeWhenReady(targetEntity, (AttributeSensor<?>) targetSensor);
            }
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
        public Task<Object> newTask() {
            return Tasks.builder().displayName("retrieving config for "+keyName).tag(BrooklynTaskTags.TRANSIENT_TASK_TAG).dynamic(false).body(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Entity targetEntity = component.get();
                    return targetEntity.getConfig(ConfigKeys.newConfigKey(Object.class, keyName));
                }
            }).build();
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
    
    public BrooklynDslDeferredSupplier<Sensor<?>> sensor(final String sensorName) {
        return new DslSensorSupplier(this, sensorName);
    }
    protected final static class DslSensorSupplier extends BrooklynDslDeferredSupplier<Sensor<?>> {
        private final DslComponent component;
        private final String sensorName;
        private static final long serialVersionUID = -4735177561947722511L;

        public DslSensorSupplier(DslComponent component, String sensorName) {
            this.component = Preconditions.checkNotNull(component);
            this.sensorName = sensorName;
        }

        @Override
        public Task<Sensor<?>> newTask() {
            return Tasks.<Sensor<?>>builder().displayName("looking up sensor for "+sensorName).dynamic(false).body(new Callable<Sensor<?>>() {
                @Override
                public Sensor<?> call() throws Exception {
                    Entity targetEntity = component.get();
                    Sensor<?> result = null;
                    if (targetEntity!=null) {
                        result = targetEntity.getEntityType().getSensor(sensorName);
                    }
                    if (result!=null) return result;
                    return Sensors.newSensor(Object.class, sensorName);
                }
            }).build();
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
                "sensor("+JavaStringEscapes.wrapJavaString(sensorName)+")";
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
            JavaStringEscapes.wrapJavaString(componentId)+
            ")";
    }

}