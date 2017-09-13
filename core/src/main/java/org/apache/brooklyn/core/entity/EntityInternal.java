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

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.SubscriptionContext;
import org.apache.brooklyn.api.mgmt.rebind.RebindSupport;
import org.apache.brooklyn.api.mgmt.rebind.Rebindable;
import org.apache.brooklyn.api.mgmt.rebind.mementos.EntityMemento;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.core.mgmt.internal.EntityManagementSupport;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;

import com.google.common.annotations.Beta;

/** 
 * Extended Entity interface with additional functionality that is purely-internal (i.e. intended 
 * for the brooklyn framework only).
 */
@Beta
public interface EntityInternal extends BrooklynObjectInternal, EntityLocal, Rebindable {
    
    void addLocations(@Nullable Collection<? extends Location> locations);

    void removeLocations(Collection<? extends Location> locations);

    /**
     * Adds the given locations to this entity, but without emitting {@link AbstractEntity#LOCATION_ADDED} 
     * events, and without auto-managing the locations. This is for internal purposes only; it is primarily 
     * intended for use during rebind.
     */
    @Beta
    void addLocationsWithoutPublishing(@Nullable Collection<? extends Location> locations);

    void clearLocations();

    /**
     * @deprecated since 0.8.0; use {@link SensorSupportInternal#setWithoutPublishing(AttributeSensor, Object)} via code like {@code sensors().setWithoutPublishing(attribute, val)}.
     */
    @Deprecated
    <T> T setAttributeWithoutPublishing(AttributeSensor<T> sensor, T val);

    /**
     * @deprecated since 0.8.0; use {@link SensorSupportInternal#getAll()} via code like {@code sensors().getAll()}.
     */
    @Deprecated
    @Beta
    Map<AttributeSensor, Object> getAllAttributes();

    /**
     * @deprecated since 0.8.0; use {@link SensorSupportInternal#remove(AttributeSensor)} via code like {@code sensors().remove(attribute)}.
     */
    @Deprecated
    @Beta
    void removeAttribute(AttributeSensor<?> attribute);

    /**
     * Must be called before the entity is started.
     * 
     * @return this entity (i.e. itself)
     */
    @Beta // for internal use only
    EntityInternal configure(Map flags);

    /** 
     * @return Routings for accessing and inspecting the management context of the entity
     */
    EntityManagementSupport getManagementSupport();

    /**
     * Should be invoked at end-of-life to clean up the item.
     */
    @Beta
    void destroy();
    
    /** 
     * Returns the management context for the entity. If the entity is not yet managed, some 
     * operations on the management context will fail. 
     * 
     * Do not cache this object; instead call getManagementContext() each time you need to use it.
     */
    ManagementContext getManagementContext();

    /** 
     * Returns the task execution context for the entity. If the entity is not yet managed, some 
     * operations on the management context will fail.
     * 
     * Do not cache this object; instead call getExecutionContext() each time you need to use it.
     */    
    ExecutionContext getExecutionContext();

    /** returns the dynamic type corresponding to the type of this entity instance */
    @Beta
    EntityDynamicType getMutableEntityType();

    /** returns the effector registered against a given name */
    @Beta
    Effector<?> getEffector(String effectorName);
    
    FeedSupport feeds();
    
    /**
     * @since 0.7.0-M2
     * @deprecated since 0.7.0-M2; use {@link #feeds()}
     */
    @Deprecated
    FeedSupport getFeedSupport();

    Map<String, String> toMetadataRecord();
    
    /**
     * Users are strongly discouraged from calling or overriding this method.
     * It is for internal calls only, relating to persisting/rebinding entities.
     * This method may change (or be removed) in a future release without notice.
     */
    @Override
    @Beta
    RebindSupport<EntityMemento> getRebindSupport();

    @Override
    RelationSupportInternal<Entity> relations();
    
    /**
     * Can be called to request that the entity be persisted.
     * This persistence may happen asynchronously, or may not happen at all if persistence is disabled.
     */
    void requestPersist();
    
    @Override
    EntitySubscriptionSupportInternal subscriptions();

    @Override
    SensorSupportInternal sensors();

    @Override
    PolicySupportInternal policies();

    @Override
    EnricherSupportInternal enrichers();

    @Override
    GroupSupportInternal groups();

    @Beta
    public interface EntitySubscriptionSupportInternal extends BrooklynObjectInternal.SubscriptionSupportInternal {
        public SubscriptionContext getSubscriptionContext();
    }
    
    @Beta
    public interface SensorSupportInternal extends Entity.SensorSupport {
        /**
         * 
         * Like {@link EntityLocal#setAttribute(AttributeSensor, Object)}, except does not publish an attribute-change event.
         */
        <T> T setWithoutPublishing(AttributeSensor<T> sensor, T val);
        
        @Beta
        Map<AttributeSensor<?>, Object> getAll();

        @Beta
        void remove(AttributeSensor<?> attribute);
    }

    public interface FeedSupport extends Iterable<Feed> {

        Collection<Feed> getFeeds();

        /**
         * Adds the given feed to this entity. The feed will automatically be re-added on brooklyn restart.
         */
        <T extends Feed> T add(T feed);

        /** @deprecated since 0.10.0; use {@link #add()} */
        @Deprecated
        <T extends Feed> T addFeed(T feed);

        /**
         * Removes the given feed from this entity. 
         * @return True if the feed existed at this entity; false otherwise
         */
        boolean remove(Feed feed);

        /** @deprecated since 0.10.0; use {@link #remove()} */
        @Deprecated
        boolean removeFeed(Feed feed);

        /**
         * Removes all feeds from this entity.
         * Use with caution as some entities automatically register feeds; this will remove those feeds as well.
         * @return True if any feeds existed at this entity; false otherwise
         */
        boolean removeAll();

        /** @deprecated since 0.10.0; use {@link #removeAll()} */
        @Deprecated
        boolean removeAllFeeds();
    }
    
    @Beta
    public interface PolicySupportInternal extends Entity.PolicySupport {
        /**
         * Removes all policy from this entity. 
         * @return True if any policies existed at this entity; false otherwise
         */
        boolean removeAllPolicies();
    }
    
    @Beta
    public interface EnricherSupportInternal extends Entity.EnricherSupport {
        /**
         * Removes all enricher from this entity.
         * Use with caution as some entities automatically register enrichers; this will remove those enrichers as well.
         * @return True if any enrichers existed at this entity; false otherwise
         */
        boolean removeAll();
    }
    
    @Beta
    public interface GroupSupportInternal extends Entity.GroupSupport {
        /**
         * Add this entity as a member of the given {@link Group}. Called by framework.
         * <p>
         * Users should call {@link Group#addMember(Entity)} instead; this method will then 
         * automatically be called. However, the reverse is not true (calling this method will 
         * not tell the group; this behaviour may change in a future release!)
         */
        void add(Group group);

        /**
         * Removes this entity as a member of the given {@link Group}. Called by framework.
         * <p>
         * Users should call {@link Group#removeMember(Entity)} instead; this method will then 
         * automatically be called. However, the reverse is not true (calling this method will 
         * not tell the group; this behaviour may change in a future release!)
         */
        void remove(Group group);
    }
}
