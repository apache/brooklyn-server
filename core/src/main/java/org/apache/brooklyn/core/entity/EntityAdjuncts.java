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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ComputeServiceIndicatorsFromChildrenAndMembers;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ComputeServiceState;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceNotUpLogic;
import org.apache.brooklyn.core.objs.proxy.EntityAdjunctProxyImpl;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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

    public static <T extends EntityAdjunct> T createProxy(Class<T> adjunctType, @Nullable T delegate) {
        return (T) java.lang.reflect.Proxy.newProxyInstance(
                /** must have sight of all interfaces */ EntityAdjuncts.class.getClassLoader(),
                new Class[] { adjunctType, EntityAdjuncts.EntityAdjunctProxyable.class},

                new EntityAdjunctProxyImpl(delegate));
    }

    public interface EntityAdjunctProxyable {
        Entity getEntity();
    }

}
