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

import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Proxy;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * As EntityProxyImpl, but for policies etc.
 * Not as widely used as the entity proxy; only used where needed eg when rebinding an entity with a reference to a policy
 * which hasn't yet been rebinded.
 */
public class EntityAdjunctProxyImpl extends AbstractBrooklynObjectProxyImpl<EntityAdjunct> implements java.lang.reflect.InvocationHandler {

    // TODO Currently the proxy references the real entity and invokes methods on it directly.
    // As we work on remoting/distribution, this will be replaced by RPC.

    private static final Logger LOG = LoggerFactory.getLogger(EntityAdjunctProxyImpl.class);

    /** These are allowed to be null, assuming they are replaced later during rebind; but it is recommended to supply an ID in that case so hashcode/equals/etc is consistent. */
    public EntityAdjunctProxyImpl(EntityAdjunct adjunct) {
        super(adjunct);
    }

    public EntityAdjunctProxyImpl(String id) {
        this((EntityAdjunct) null);
        this.id = id;
    }

    public static void resetDelegate(EntityAdjunct proxy, EntityAdjunct preferredDelegate) {
        if (proxy==null) return;
        if (!Proxy.isProxyClass(proxy.getClass())) return;
        ((EntityAdjunctProxyImpl) Proxy.getInvocationHandler(proxy)).resetDelegate(proxy, proxy, preferredDelegate);
    }

    public static EntityAdjunct deproxy(EntityAdjunct proxy) {
        if (proxy==null) return proxy;
        if (!Proxy.isProxyClass(proxy.getClass())) return proxy;
        return deproxy( ((EntityAdjunctProxyImpl) Proxy.getInvocationHandler(proxy)).delegate );
    }

    @Override
    protected EntityAdjunct getProxy(EntityAdjunct instance, boolean requireNonProxy) {
        if (!requireNonProxy) return instance;
        return null;
    }

    @Override
    protected void resetProxy(EntityAdjunct instance, EntityAdjunct preferredProxy) {
        // no action needed (adjunct doesn't store proxy)
    }

    @Override
    protected boolean isPermittedReadOnlyMethod(MethodSignature sig) {
        // we could restrict by name as we do for entities; but for adjuncts that seems less important;
        // anything which needs a task is likely to fail in any case.
        // previously (before introducting proxy) we had full instances unmanaged so allowed such things there.
        return true;
    }

}
