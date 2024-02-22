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

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ComputeServiceState;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceNotUpLogic;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.objs.AbstractBrooklynObject;
import org.apache.brooklyn.core.objs.AbstractEntityAdjunct;
import org.apache.brooklyn.core.objs.proxy.EntityAdjunctProxyImpl;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Convenience methods for working with entity adjunts.
 */
public class EntityAdjuncts {

    public static <T extends EntityAdjunct> Maybe<T> tryFindWithUniqueTag(Iterable<T> adjuncts, Object tag) {
        Preconditions.checkNotNull(tag, "tag");
        for (T adjunct: adjuncts)
            if (tag.equals(adjunct.getUniqueTag())) 
                return Maybe.of(adjunct);
        return Maybe.absent("Not found with tag "+tag);
    }

    public static boolean matchesTagOrIdOrAdjunctItself(EntityAdjunct adjunct, Object tagOrIdOrAdjunctItself) {
        if (adjunct==null || tagOrIdOrAdjunctItself==null) return false;
        if (tagOrIdOrAdjunctItself.equals(adjunct)) return true;
        if (tagOrIdOrAdjunctItself instanceof EntityAdjunct) return false;
        if (tagOrIdOrAdjunctItself.equals(adjunct.getUniqueTag())) return true;
        if (tagOrIdOrAdjunctItself.equals(adjunct.config().get(BrooklynConfigKeys.PLAN_ID))) return true;
        return false;
    }

    public static <T extends EntityAdjunct> Maybe<T> tryFind(Iterable<T> adjuncts, Object tagOrIdOrAdjunctItself) {
        if (tagOrIdOrAdjunctItself==null) return Maybe.absent("Asked to find null");
        for (T adjunct: adjuncts)
            if (matchesTagOrIdOrAdjunctItself(adjunct, tagOrIdOrAdjunctItself)) return Maybe.of(adjunct);
        return Maybe.absent("Not found");
    }

    public static Maybe<EntityAdjunct> tryFindOnEntity(Entity entity, Object tagOrIdOrAdjunctItself) {
        if (tagOrIdOrAdjunctItself==null) return Maybe.absent("Asked to find null");
        Maybe<? extends EntityAdjunct> result;
        result = tryFind(entity.policies(), tagOrIdOrAdjunctItself);
        if (result.isPresent()) return (Maybe<EntityAdjunct>) result;
        result = tryFind(entity.enrichers(), tagOrIdOrAdjunctItself);
        if (result.isPresent()) return (Maybe<EntityAdjunct>) result;
        result = tryFind(((EntityInternal)entity).feeds(), tagOrIdOrAdjunctItself);
        if (result.isPresent()) return (Maybe<EntityAdjunct>) result;

        return Maybe.absent("Adjunct/policy not found: "+tagOrIdOrAdjunctItself);
    }

    public static final List<String> SYSTEM_ENRICHER_UNIQUE_TAGS = ImmutableList.of(
        ServiceNotUpLogic.DEFAULT_ENRICHER_UNIQUE_TAG,
        ComputeServiceState.DEFAULT_ENRICHER_UNIQUE_TAG,
        ComputeServiceIndicatorsFromChildrenAndMembers.DEFAULT_UNIQUE_TAG,
        ComputeServiceIndicatorsFromChildrenAndMembers.DEFAULT_UNIQUE_TAG_UP);
    
    public static List<Enricher> getNonSystemEnrichers(Entity entity) {
        List<Enricher> result = MutableList.copyOf(entity.enrichers());
        Iterator<Enricher> ri = result.iterator();
        while (ri.hasNext()) {
            if (isSystemEnricher(ri.next())) ri.remove();
        }
        return result;
    }

    public static boolean isSystemEnricher(Enricher enr) {
        if (enr.getUniqueTag()==null) return false;
        if (SYSTEM_ENRICHER_UNIQUE_TAGS.contains(enr.getUniqueTag())) return true;
        return false;
    }

    @Beta
    public static Lifecycle inferAdjunctStatus(EntityAdjunct a) {
        if (a.isRunning()) return Lifecycle.RUNNING;
        if (a.isDestroyed()) return Lifecycle.DESTROYED;
        
        // adjuncts don't currently support an "error" state; though that would be useful!
        
        if (a instanceof Policy) {
            if (((Policy)a).isSuspended()) return Lifecycle.STOPPED;
            return Lifecycle.CREATED;
        }
        if (a instanceof Feed) {
            if (((Feed)a).isSuspended()) return Lifecycle.STOPPED;
            if (((Feed)a).isActivated()) return Lifecycle.STARTING;
            return Lifecycle.CREATED;
        }

        // Enricher doesn't support suspend so if not running or destroyed then
        // it is just created
        return Lifecycle.CREATED;
    }

    public static <T extends EntityAdjunct> T createProxyForInstance(Class<T> adjunctType, @Nonnull T delegate) {
        return (T) java.lang.reflect.Proxy.newProxyInstance(
                /** must have sight of all interfaces */ EntityAdjuncts.class.getClassLoader(),
                new Class[] { adjunctType, EntityAdjuncts.EntityAdjunctProxyable.class},

                new EntityAdjunctProxyImpl(Preconditions.checkNotNull(delegate)));
    }

    public static <T extends EntityAdjunct> T createProxyForId(Class<T> adjunctType, String id) {
        return (T) java.lang.reflect.Proxy.newProxyInstance(
                /** must have sight of all interfaces */ EntityAdjuncts.class.getClassLoader(),
                new Class[] { adjunctType, EntityAdjuncts.EntityAdjunctProxyable.class},

                new EntityAdjunctProxyImpl(id));
    }

    /** all real EntityAdjunct items support getEntity, but via two different paths, depending whether it is proxied or not; Entity returns itself; other BrooklynObjects do not usually have an entity */
    public static <T extends BrooklynObject> Maybe<Entity> getEntity(T value, boolean treatDefinitelyMissingAsAbsent) {
        if (value instanceof Entity) return Maybe.of((Entity)value);

        if (value instanceof EntityAdjunct) {
            final Function<Maybe<Entity>, Maybe<Entity>> handleNull = mv -> {
                if (!treatDefinitelyMissingAsAbsent || mv.isAbsent() || mv.isPresentAndNonNull()) return mv;
                return Maybe.absentNull("entity definitely not set on adjunct");
            };
            if (value instanceof EntityAdjuncts.EntityAdjunctProxyable) {
                // works for proxies and almost all real instances
                return handleNull.apply(Maybe.ofAllowingNull(((EntityAdjuncts.EntityAdjunctProxyable) value).getEntity()));
            }
            if (value instanceof AbstractEntityAdjunct) {
                // needed for items that don't extend the standard Abstracts, eg something that implements Enricher but don't extend AbstractEnricher
                // (we might be able to get rid of those; and maybe move getEntity to the EntityAdjunct interface, but that is for another day)
                return handleNull.apply(Maybe.ofAllowingNull(((AbstractEntityAdjunct) value).getEntity()));
            }
            return Maybe.absent("adjunct does not supply way to detect if entity set");
        }

        return Maybe.absent("brooklyn object "+value+" not supported for entity retrieval");
    }

    public static boolean removeAdjunct(Entity entity, EntityAdjunct policy) {
        if (policy instanceof Policy) return entity.policies().remove((Policy) policy);
        if (policy instanceof Enricher) return entity.enrichers().remove((Enricher) policy);
        if (policy instanceof Feed) return ((EntityInternal)entity).feeds().remove((Feed) policy);
        if (policy==null) return false;
        throw new IllegalArgumentException("Unexpected adjunct type "+policy.getClass()+" "+policy);
    }

    public static BrooklynObject addAdjunct(Entity entity, AbstractBrooklynObjectSpec spec) {
        if (spec instanceof PolicySpec) return entity.policies().add((PolicySpec) spec);
        if (spec instanceof EnricherSpec) return entity.enrichers().add((EnricherSpec) spec);
        // feed specs not supported
        if (spec==null) return null;
        throw new IllegalArgumentException("Unexpected adjunct type "+spec.getClass()+" "+spec);
    }

    @Beta //experimental; typically we want to use specs, but might not always be so easy
    public static BrooklynObject addAdjunct(Entity entity, EntityAdjunct spec) {
        if (spec instanceof Policy) { entity.policies().add(init(((EntityInternal)entity).getManagementContext(), (Policy) spec)); return (Policy) spec; }
        if (spec instanceof Enricher) { entity.enrichers().add(init(((EntityInternal)entity).getManagementContext(), (Enricher) spec)); return (Enricher) spec; }
        if (spec instanceof Feed) { ((EntityInternal)entity).feeds().add(init(((EntityInternal)entity).getManagementContext(), (Feed) spec)); return (Feed) spec; }
        if (spec==null) return null;
        throw new IllegalArgumentException("Unexpected adjunct type "+spec.getClass()+" "+spec);
    }

    private static <T extends BrooklynObject> T init(ManagementContext mgmt, T adjunct) {
        ((AbstractBrooklynObject)adjunct).setManagementContext((ManagementContextInternal) mgmt);
        ((AbstractBrooklynObject)adjunct).init();
        return adjunct;
    }

    /** supported by nearly all EntityAdjuncts, but a few in the wild might that don't extend the standard AbstractEntityAdjunct might not implement this; see {@link #getEntity()} */
    public interface EntityAdjunctProxyable extends EntityAdjunct {
        Entity getEntity();
        ManagementContext getManagementContext();
    }

}
