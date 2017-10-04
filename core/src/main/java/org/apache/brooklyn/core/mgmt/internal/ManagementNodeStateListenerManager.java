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
package org.apache.brooklyn.core.mgmt.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.core.mgmt.ManagementContextInjectable;
import org.apache.brooklyn.core.mgmt.usage.ManagementNodeStateListener;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Handles the notification of {@link ManagementNodeStateListener}s.
 * 
 * @see {@link BrooklynServerConfig#MANAGEMENT_NODE_STATE_LISTENERS} for configuring this.
 * @see {@link org.apache.brooklyn.core.mgmt.ha.HighAvailabilityManagerImpl#HighAvailabilityManagerImpl(ManagementContextInternal, ManagementNodeStateListener)}
 *      for how we get notified of the state-change.
 */
public class ManagementNodeStateListenerManager implements ManagementNodeStateListener {

    private static final Logger LOG = LoggerFactory.getLogger(ManagementNodeStateListenerManager.class);

    private final ManagementContextInternal mgmt;
    
    private final Object mutex = new Object();

    private final List<ManagementNodeStateListener> listeners = Lists.newCopyOnWriteArrayList();
    private ManagementNodeState lastPublishedVal;
    
    private final AtomicInteger listenerQueueSize = new AtomicInteger();
    
    private ListeningExecutorService listenerExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("brooklyn-managementnodestate-listener-%d")
            .build()));

    public ManagementNodeStateListenerManager(ManagementContextInternal managementContext) {
        this.mgmt = checkNotNull(managementContext, "managementContext");
        
        // Register a coercion from String->ManagementNodeStateListener, so that MANAGEMENT_NODE_STATE_LISTENERS defined in brooklyn.cfg
        // will be instantiated, given their class names.
        TypeCoercions.BrooklynCommonAdaptorTypeCoercions.registerInstanceForClassnameAdapter(
                new ClassLoaderUtils(this.getClass(), managementContext), 
                ManagementNodeStateListener.class);

        // Although changing listeners to Collection<ManagementNodeStateListener> is valid at compile time
        // the collection will contain any objects that could not be coerced by the function
        // declared above. Generally this means any string declared in brooklyn.properties
        // that is not a ManagementNodeStateListener.
        Collection<?> rawListeners = managementContext.getBrooklynProperties().getConfig(BrooklynServerConfig.MANAGEMENT_NODE_STATE_LISTENERS);
        if (rawListeners != null) {
            for (Object obj : rawListeners) {
                if (obj == null) {
                    throw new NullPointerException("null listener in config " + BrooklynServerConfig.MANAGEMENT_NODE_STATE_LISTENERS);
                } else if (!(obj instanceof ManagementNodeStateListener)) {
                    throw new ClassCastException("Configured object is not a "+ManagementNodeStateListener.class.getSimpleName()+". This probably means coercion failed: " + obj);
                } else {
                    ManagementNodeStateListener listener = (ManagementNodeStateListener) obj;
                    if (listener instanceof ManagementContextInjectable) {
                        ((ManagementContextInjectable) listener).setManagementContext(managementContext);
                    }
                    listeners.add((ManagementNodeStateListener)listener);
                }
            }
        }
    }

    @Override
    public void onStateChange(ManagementNodeState state) {
        // Filtering out duplicates/nulls, schedule the notification of the listeners with this latest value.
        synchronized (mutex) {
            if (state != null && lastPublishedVal != state) {
                LOG.debug("Notifying {} listener(s) of management-node state changed to {}", new Object[] {listeners.size(), state});
                lastPublishedVal = state;
                execOnListeners(new Function<ManagementNodeStateListener, Void>() {
                        @Override
                        public Void apply(ManagementNodeStateListener listener) {
                            listener.onStateChange(state);
                            return null;
                        }
                        @Override
                        public String toString() {
                            return "stateChange("+state+")";
                        }});
            }
        }
    }

    public void terminate() {
        // Wait for the listeners to finish + close the listeners
        Duration timeout = mgmt.getBrooklynProperties().getConfig(BrooklynServerConfig.MANAGEMENT_NODE_STATE_LISTENER_TERMINATION_TIMEOUT);
        if (listenerQueueSize.get() > 0) {
            LOG.info("Management-node-state-listener manager waiting for "+listenerQueueSize+" listener events for up to "+timeout);
        }
        List<ListenableFuture<?>> futures = Lists.newArrayList();
        for (final ManagementNodeStateListener listener : listeners) {
            ListenableFuture<?> future = listenerExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    if (listener instanceof Closeable) {
                        try {
                            ((Closeable)listener).close();
                        } catch (IOException | RuntimeException e) {
                            LOG.warn("Problem closing management-node-state listener "+listener, e);
                            Exceptions.propagateIfFatal(e);
                        }
                    }
                }});
            futures.add(future);
        }
        try {
            Futures.successfulAsList(futures).get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            LOG.warn("Problem terminiating management-node-state listeners (continuing)", e);
        } finally {
            listenerExecutor.shutdownNow();
        }
    }

    private void execOnListeners(final Function<ManagementNodeStateListener, Void> job) {
        for (final ManagementNodeStateListener listener : listeners) {
            listenerQueueSize.incrementAndGet();
            listenerExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        job.apply(listener);
                    } catch (RuntimeException e) {
                        LOG.error("Problem notifying listener "+listener+" of "+job, e);
                        Exceptions.propagateIfFatal(e);
                    } finally {
                        listenerQueueSize.decrementAndGet();
                    }
                }});
        }
    }
}
