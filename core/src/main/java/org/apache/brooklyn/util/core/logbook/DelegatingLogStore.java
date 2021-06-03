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
package org.apache.brooklyn.util.core.logbook;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class DelegatingLogStore implements LogStore {
    private static final Logger log = LoggerFactory.getLogger(DelegatingLogStore.class);

    private final ManagementContext mgmt;
    private LogStore delegate;


    public DelegatingLogStore(ManagementContext mgmt) {
        this.mgmt = mgmt;
        mgmt.addPropertiesReloadListener(new PropertiesListener());
    }

    public synchronized LogStore getDelegate() {
        if (delegate == null) {
            delegate = loadDelegate();
        }
        return delegate;
    }

    private LogStore loadDelegate() {
        StringConfigMap brooklynProperties = mgmt.getConfig();

        LogStore presetDelegate = brooklynProperties.getConfig(LogbookConfig.LOGBOOK_LOG_STORE_INSTANCE);
        if (presetDelegate != null) {
            log.trace("Brooklyn logbook: using pre-set log store {}", presetDelegate);
            return presetDelegate;
        }
        String className = brooklynProperties.getConfig(LogbookConfig.LOGBOOK_LOG_STORE_CLASSNAME);
        try {
            // TODO implement logic to allow to inject the impementation from bundle.
            // TODO refactor logic to use it also for the security provider (this comes from there DelegatingSecurityProvider)
//            String bundle = brooklynProperties.getConfig(LogbookConfig.SECURITY_PROVIDER_BUNDLE);
//            if (bundle!=null) {
//                String bundleVersion = brooklynProperties.getConfig(LogbookConfig.SECURITY_PROVIDER_BUNDLE_VERSION);
//                log.info("Brooklyn security: using security provider " + className + " from " + bundle+":"+bundleVersion);
//                BundleContext bundleContext = ((ManagementContextInternal)mgmt).getOsgiManager().get().getFramework().getBundleContext();
//                delegate = loadProviderFromBundle(mgmt, bundleContext, bundle, bundleVersion, className);
//            } else {
            log.info("Brooklyn Logbook: using log store " + className);
            ClassLoaderUtils clu = new ClassLoaderUtils(this, mgmt);
            Class<? extends LogStore> clazz = (Class<? extends LogStore>) clu.loadClass(className);
            delegate = createLogStoreProviderInstance(mgmt, clazz);
//            }
        } catch (Exception e) {
            log.warn("Brooklyn Logbook: unable to instantiate Log Store " + className, e);
            delegate = new StaticLogStore();
        }

        // TODO what must be removed here nad on the DelegatingSecurityProvider
        // Deprecated in 0.11.0. Add to release notes and remove in next release.
        ((BrooklynProperties) mgmt.getConfig()).put(LogbookConfig.LOGBOOK_LOG_STORE_INSTANCE, delegate);
        mgmt.getScratchpad().put(LogbookConfig.LOGBOOK_LOG_STORE_INSTANCE, delegate);

        return delegate;
    }

    public LogStore createLogStoreProviderInstance(ManagementContext mgmt,
                                                   Class<? extends LogStore> clazz) throws NoSuchMethodException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        Constructor<? extends LogStore> constructor = null;
        Object delegateO;
        try {
            constructor = clazz.getConstructor(ManagementContext.class);
        } catch (NoSuchMethodException e) {
            // ignore
        }
        if (constructor != null) {
            delegateO = constructor.newInstance(mgmt);
        } else {
            try {
                constructor = clazz.getConstructor();
            } catch (NoSuchMethodException e) {
                // ignore
            }
            if (constructor != null) {
                delegateO = constructor.newInstance();
            } else {
                throw new NoSuchMethodException("Log store " + clazz + " does not have required no-arg or 1-arg (mgmt) constructor");
            }
        }

        if (!(delegateO instanceof LogStore)) {
            // if classloaders get mangled it will be a different CL's SecurityProvider
            throw new ClassCastException("Delegate is either not a Log Store implementation or has an incompatible classloader: " + delegateO);
        }
        return (LogStore) delegateO;
    }

    @Override
    public List<String> query(LogBookQueryParams params) {
        return getDelegate().query(params);
    }

    @Override
    public List<String> getEntries(Integer from, Integer numberOfItems) throws IOException {
        return getDelegate().getEntries(from,numberOfItems);
    }

    private class PropertiesListener implements ManagementContext.PropertiesReloadListener {
        private static final long serialVersionUID = 8148722609022378917L;

        @Override
        public void reloaded() {
            log.debug("{} reloading Logbook log store configuration", DelegatingLogStore.this);
            synchronized (DelegatingLogStore.this) {
                loadDelegate();
            }
        }
    }
}
