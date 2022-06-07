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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.internal.EntityManagerInternal;
import org.apache.brooklyn.core.mgmt.internal.ManagementTransitionMode;
import org.apache.brooklyn.core.mgmt.rebind.RebindManagerImpl.RebindTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Methods lifted from EntityProxyImpl to support an AdjunctProxyImpl
 */
public abstract class AbstractBrooklynObjectProxyImpl<T extends BrooklynObject> implements java.lang.reflect.InvocationHandler {

    // TODO Currently the proxy references the real entity and invokes methods on it directly.
    // As we work on remoting/distribution, this will be replaced by RPC.

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBrooklynObjectProxyImpl.class);

    protected T delegate;
    protected Boolean isMaster;

    private WeakHashMap<T,Void> temporaryProxies = new WeakHashMap<T, Void>();

    private static final Set<MethodSignature> OBJECT_METHODS = Sets.newLinkedHashSet();
    static {
        for (Method m : Object.class.getMethods()) {
            OBJECT_METHODS.add(new MethodSignature(m));
        }
    }

    protected AbstractBrooklynObjectProxyImpl(T delegate) {
        this.delegate = delegate;
    }

    protected abstract T getProxy(T instance, boolean requireNonProxy);
    protected abstract void resetProxy(T instance, T preferredProxy);

    /** invoked to specify that a different underlying delegate should be used, 
     * e.g. because we are switching copy impls or switching primary/copy*/
    public synchronized void resetDelegate(T thisProxy, T preferredProxy, T newDelegate) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("updating "+Integer.toHexString(System.identityHashCode(thisProxy))
                +" to be the same as "+Integer.toHexString(System.identityHashCode(preferredProxy))
                +" pointing at "+Integer.toHexString(System.identityHashCode(newDelegate)) 
                +" ("+temporaryProxies.size()+" temporary proxies)");
        }

        T oldDelegate = delegate;
        this.delegate = newDelegate;
        this.isMaster = null;
        
        if (newDelegate==oldDelegate)
            return;
        
        /* we have to make sure that any newly created proxy of the newDelegate 
         * which have leaked (eg by being set as a child) also get repointed to this new delegate */
        if (oldDelegate!=null) {
            T temporaryProxy = getProxy(oldDelegate, true);
            if (temporaryProxy!=null) temporaryProxies.put(temporaryProxy, null);
            resetProxy(oldDelegate, preferredProxy);
        }
        if (newDelegate!=null) {   
            T temporaryProxy = getProxy(newDelegate, true);
            if (temporaryProxy!=null) temporaryProxies.put(temporaryProxy, null);
            resetProxy(newDelegate, preferredProxy);
        }
        
        // update any proxies which might be in use
        for (T tp: temporaryProxies.keySet()) {
            if (tp==thisProxy || tp==preferredProxy) continue;
            ((AbstractBrooklynObjectProxyImpl)(Proxy.getInvocationHandler(tp))).resetDelegate(tp, preferredProxy, newDelegate);
        }
    }
    
    @Override
    public String toString() {
        return delegate==null ? getClass().getName()+"[null]" : delegate.toString();
    }
    
    protected boolean isMaster() {
        if (isMaster!=null) return isMaster;
        if (delegate==null) return false;
        
        ManagementContext mgmt = ((EntityInternal)delegate).getManagementContext();
        ManagementTransitionMode mode = ((EntityManagerInternal)mgmt.getEntityManager()).getLastManagementTransitionMode(delegate.getId());
        Boolean ro = ((EntityInternal)delegate).getManagementSupport().isReadOnlyRaw();
        
        if (mode==null || ro==null) {
            // not configured yet
            return false;
        }
        boolean isMasterX = !mode.isReadOnly();
        if (isMasterX != !ro) {
            LOG.warn("Inconsistent read-only state for "+delegate+" (possibly rebinding); "
                + "management thinks "+isMasterX+" but entity thinks "+!ro);
            return false;
        }
        isMaster = isMasterX;
        return isMasterX;
    }

    /** calls permitted even when in RO / non-master mode */
    protected abstract boolean isPermittedReadOnlyMethod(MethodSignature sig);

    @Override
    public Object invoke(Object proxy, final Method m, final Object[] args) throws Throwable {
        if (proxy == null) {
            throw new IllegalArgumentException("Static methods not supported via proxy on entity "+delegate);
        }
        
        MethodSignature sig = new MethodSignature(m);

        Object result;
        if (delegate==null) {
            // allow access to toString (mainly for logging of errors during serialization)
            if ("toString".equals(sig.name)) return toString();
            throw new NullPointerException("Access to proxy before initialized, method "+m);
        } else if (OBJECT_METHODS.contains(sig)) {
            result = m.invoke(delegate, args);
        } else if (isPermittedReadOnlyMethod(sig)) {
            result = m.invoke(delegate, args);
        } else {
            if (!isMaster()) {
                if (isMaster==null || RebindTracker.isRebinding()) {
                    // rebinding or caller manipulating before management; permit all access
                    // (as of this writing, things seem to work fine without the isRebinding check;
                    // but including in it may allow us to tighten the methods in EntityTransientCopyInternal) 
                    result = m.invoke(delegate, args);
                } else {
                    throw new UnsupportedOperationException("Call to '"+sig+"' not permitted on read-only entity "+delegate);
                }
            } else {
                result = invokeOther(m, args);
            }
        }
        
        return result == delegate ? getProxy(delegate, false) : result;
    }

    protected Object invokeOther(Method m, Object[] args) throws IllegalAccessException, InvocationTargetException {
        return m.invoke(delegate, args);
    }

    static class MethodSignature {
        private final String name;
        private final Class<?>[] parameterTypes;
        
        MethodSignature(Method m) {
            name = m.getName();
            parameterTypes = m.getParameterTypes();
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(name, Arrays.hashCode(parameterTypes));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MethodSignature)) return false;
            MethodSignature o = (MethodSignature) obj;
            return name.equals(o.name) && Arrays.equals(parameterTypes, o.parameterTypes);
        }
        
        @Override
        public String toString() {
            return name+Arrays.toString(parameterTypes);
        }
    }
    
    @VisibleForTesting
    public T getDelegate() {
        return delegate;
    }
    
    @Override
    public boolean equals(Object obj) {
        return Objects.equal(delegate, obj);
    }
    
    @Override
    public int hashCode() {
        return delegate==null ? 0 : delegate.hashCode();
    }
}
