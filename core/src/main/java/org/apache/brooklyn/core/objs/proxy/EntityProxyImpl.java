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
package org.apache.brooklyn.core.objs.proxy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.core.effector.EffectorWithBody;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.internal.EntityTransientCopyInternal;
import org.apache.brooklyn.core.entity.internal.EntityTransientCopyInternal.SpecialEntityTransientCopyInternal;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.core.mgmt.internal.EntityManagerInternal;
import org.apache.brooklyn.core.mgmt.internal.ManagementTransitionMode;
import org.apache.brooklyn.core.mgmt.rebind.RebindManagerImpl.RebindTracker;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;

/**
 * A dynamic proxy for an entity. Other entities etc should use these proxies when interacting
 * with the entity, rather than holding a reference to the specific object. That makes remoting
 * etc much simpler.
 * 
 * @author aled
 */
public class EntityProxyImpl extends AbstractBrooklynObjectProxyImpl<Entity> implements java.lang.reflect.InvocationHandler {
    
    // TODO Currently the proxy references the real entity and invokes methods on it directly.
    // As we work on remoting/distribution, this will be replaced by RPC.

    private static final Logger LOG = LoggerFactory.getLogger(EntityProxyImpl.class);

    private static final Set<MethodSignature> ENTITY_NON_EFFECTOR_METHODS = Sets.newLinkedHashSet();
    static {
        for (Method m : Entity.class.getMethods()) {
            ENTITY_NON_EFFECTOR_METHODS.add(new MethodSignature(m));
        }
        for (Method m : EntityLocal.class.getMethods()) {
            ENTITY_NON_EFFECTOR_METHODS.add(new MethodSignature(m));
        }
        for (Method m : EntityInternal.class.getMethods()) {
            ENTITY_NON_EFFECTOR_METHODS.add(new MethodSignature(m));
        }
    }

    private static final Set<MethodSignature> ENTITY_PERMITTED_READ_ONLY_METHODS = Sets.newLinkedHashSet();
    static {
        for (Method m : EntityTransientCopyInternal.class.getMethods()) {
            ENTITY_PERMITTED_READ_ONLY_METHODS.add(new MethodSignature(m));
        }
        if (!ENTITY_NON_EFFECTOR_METHODS.containsAll(ENTITY_PERMITTED_READ_ONLY_METHODS)) {
            Set<MethodSignature> extras = new LinkedHashSet<EntityProxyImpl.MethodSignature>(ENTITY_PERMITTED_READ_ONLY_METHODS);
            extras.removeAll(ENTITY_NON_EFFECTOR_METHODS);
            throw new IllegalStateException("Entity read-only methods contains items not known as Entity methods: "+
                extras);
        }
        for (Method m : SpecialEntityTransientCopyInternal.class.getMethods()) {
            ENTITY_PERMITTED_READ_ONLY_METHODS.add(new MethodSignature(m));
        }
    }

    public EntityProxyImpl(Entity entity) {
        super(checkNotNull(entity, "entity"));
    }

    @Override
    protected Entity getProxy(Entity instance, boolean requireNonProxy) {
        if (!requireNonProxy && !(instance instanceof AbstractEntity)) return instance;
        return ((AbstractEntity)instance).getProxy();
    }

    @Override
    protected void resetProxy(Entity instance, Entity preferredProxy) {
        ((AbstractEntity)instance).resetProxy(preferredProxy);
    }

    @Override
    protected boolean isPermittedReadOnlyMethod(AbstractBrooklynObjectProxyImpl.MethodSignature sig) {
        return ENTITY_PERMITTED_READ_ONLY_METHODS.contains(sig);
    }

    protected boolean isPermittedReadWriteStandardMethod(AbstractBrooklynObjectProxyImpl.MethodSignature sig) {
        return ENTITY_NON_EFFECTOR_METHODS.contains(sig);
    }

    @Override
    protected Object invokeOther(Method m, Object[] argsPreNullFix) throws IllegalAccessException, InvocationTargetException {
        MethodSignature sig = new MethodSignature(m);
        // not sure this is necessary, but it was done previously so has been maintained
        Object[] args = (argsPreNullFix == null) ? new Object[0] : argsPreNullFix;

        if (isPermittedReadWriteStandardMethod(sig)) {
            return m.invoke(delegate, args);
        }

        Effector<?> eff = findEffector(m, args);
        if (eff != null) {
            @SuppressWarnings("rawtypes")
            Map parameters = EffectorUtils.prepareArgsForEffectorAsMapFromArray(eff, args);
            @SuppressWarnings({ "unchecked", "rawtypes" })
            TaskAdaptable<?> task = ((EffectorWithBody)eff).getBody().newTask(delegate, eff, ConfigBag.newInstance(parameters));
            // as per LocalManagementContext.runAtEntity(Entity entity, TaskAdaptable<T> task)
            TaskTags.markInessential(task);
            return DynamicTasks.get(task.asTask(), delegate);

        } else {
            return super.invokeOther(m, args);
        }
    }

    private Effector<?> findEffector(Method m, Object[] args) {
        String name = m.getName();
        Set<Effector<?>> effectors = delegate.getEntityType().getEffectors();
        for (Effector<?> contender : effectors) {
            if (name.equals(contender.getName())) {
                return contender;
            }
        }
        return null;
    }

}
