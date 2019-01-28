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
package org.apache.brooklyn.rest.security.provider;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpSession;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegatingSecurityProvider implements SecurityProvider {

    private static final Logger log = LoggerFactory.getLogger(DelegatingSecurityProvider.class);
    protected final ManagementContext mgmt;

    public DelegatingSecurityProvider(ManagementContext mgmt) {
        this.mgmt = mgmt;
        mgmt.addPropertiesReloadListener(new PropertiesListener());
    }
    
    private SecurityProvider delegate;
    private final AtomicLong modCount = new AtomicLong();

    private class PropertiesListener implements ManagementContext.PropertiesReloadListener {
        private static final long serialVersionUID = 8148722609022378917L;

        @Override
        public void reloaded() {
            log.debug("{} reloading security provider", DelegatingSecurityProvider.this);
            synchronized (DelegatingSecurityProvider.this) {
                loadDelegate();
                invalidateExistingSessions();
            }
        }
    }

    public synchronized SecurityProvider getDelegate() {
        if (delegate == null) {
            delegate = loadDelegate();
        }
        return delegate;
    }

    @SuppressWarnings("unchecked")
    private synchronized SecurityProvider loadDelegate() {
        StringConfigMap brooklynProperties = mgmt.getConfig();

        SecurityProvider presetDelegate = brooklynProperties.getConfig(BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE);
        if (presetDelegate!=null) {
            log.trace("Brooklyn security: using pre-set security provider {}", presetDelegate);
            return presetDelegate;
        }
        
        String className = brooklynProperties.getConfig(BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME);

        if (delegate != null && BrooklynWebConfig.hasNoSecurityOptions(mgmt.getConfig())) {
            log.debug("Brooklyn security: {} refusing to change from {}: No security provider set in reloaded properties.",
                    this, delegate);
            return delegate;
        }

        try {
            String bundle = brooklynProperties.getConfig(BrooklynWebConfig.SECURITY_PROVIDER_BUNDLE);
            if (bundle!=null) {
                String bundleVersion = brooklynProperties.getConfig(BrooklynWebConfig.SECURITY_PROVIDER_BUNDLE_VERSION);
                log.info("Brooklyn security: using security provider " + className + " from " + bundle+":"+bundleVersion);
                BundleContext bundleContext = ((ManagementContextInternal)mgmt).getOsgiManager().get().getFramework().getBundleContext();
                delegate = loadProviderFromBundle(mgmt, bundleContext, bundle, bundleVersion, className);
            } else {
                log.info("Brooklyn security: using security provider " + className);
                ClassLoaderUtils clu = new ClassLoaderUtils(this, mgmt);
                Class<? extends SecurityProvider> clazz = (Class<? extends SecurityProvider>) clu.loadClass(className);
                delegate = createSecurityProviderInstance(mgmt, clazz);
            }
        } catch (Exception e) {
            log.warn("Brooklyn security: unable to instantiate security provider " + className + "; all logins are being disallowed", e);
            delegate = new BlackholeSecurityProvider();
        }

        // Deprecated in 0.11.0. Add to release notes and remove in next release.
        ((BrooklynProperties)mgmt.getConfig()).put(BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE, delegate);
        mgmt.getScratchpad().put(BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE, delegate);

        return delegate;
    }

    public static SecurityProvider loadProviderFromBundle(
        ManagementContext mgmt, BundleContext bundleContext,
        String symbolicName, String version, String className) {
        try {
            Collection<Bundle> bundles = getMatchingBundles(bundleContext, symbolicName, version);
            if (bundles.isEmpty()) {
                throw new IllegalStateException("No bundle " + symbolicName + ":" + version + " found");
            } else if (bundles.size() > 1) {
                log.warn("Brooklyn security: found multiple bundles matching symbolicName " + symbolicName + " and version " + version +
                    " while trying to load security provider " + className + ". Will use first one that loads the class successfully.");
            }
            SecurityProvider p = tryLoadClass(mgmt, className, bundles);
            if (p == null) {
                throw new ClassNotFoundException("Unable to load class " + className + " from bundle " + symbolicName + ":" + version);
            }
            return p;
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            throw new IllegalStateException("Can not load or create security provider " + className + " for bundle " + symbolicName + ":" + version, e);
        }
    }

    private static SecurityProvider tryLoadClass(ManagementContext mgmt, String className, Collection<Bundle> bundles)
        throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        for (Bundle b : bundles) {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends SecurityProvider> securityProviderType = (Class<? extends SecurityProvider>) b.loadClass(className);
                return DelegatingSecurityProvider.createSecurityProviderInstance(mgmt, securityProviderType);
            } catch (ClassNotFoundException e) {
            }
        }
        return null;
    }

    private static Collection<Bundle> getMatchingBundles(BundleContext bundleContext, final String symbolicName, final String version) {
        Collection<Bundle> bundles = new ArrayList<>();
        for (Bundle b : bundleContext.getBundles()) {
            if (b.getSymbolicName().equals(symbolicName) &&
                (version == null || b.getVersion().toString().equals(version))) {
                bundles.add(b);
            }
        }
        return bundles;
    }

    public static SecurityProvider createSecurityProviderInstance(ManagementContext mgmt,
            Class<? extends SecurityProvider> clazz) throws NoSuchMethodException, InstantiationException,
                    IllegalAccessException, InvocationTargetException {
        Constructor<? extends SecurityProvider> constructor = null;
        Object delegateO;
        try {
            constructor = clazz.getConstructor(ManagementContext.class);
        } catch (NoSuchMethodException e) {
            // ignore
        }
        if (constructor!=null) {
            delegateO = constructor.newInstance(mgmt);
        } else {
            try {
                constructor = clazz.getConstructor();
            } catch (NoSuchMethodException e) {
                // ignore
            }
            if (constructor!=null) {
                delegateO = constructor.newInstance();
            } else {
                throw new NoSuchMethodException("Security provider "+clazz+" does not have required no-arg or 1-arg (mgmt) constructor");
            }
        }
        
        if (!(delegateO instanceof SecurityProvider)) {
            // if classloaders get mangled it will be a different CL's SecurityProvider
            throw new ClassCastException("Delegate is either not a security provider or has an incompatible classloader: "+delegateO);
        }
        return (SecurityProvider) delegateO;
    }

    /**
     * Causes all existing sessions to be invalidated.
     */
    protected void invalidateExistingSessions() {
        modCount.incrementAndGet();
    }

    @Override
    public boolean isAuthenticated(HttpSession session) {
        return getDelegate().isAuthenticated(session);
    }

    @Override
    public boolean authenticate(HttpSession session, String user, String password) throws SecurityProviderDeniedAuthentication {
        boolean authenticated = getDelegate().authenticate(session, user, password);
        if (log.isTraceEnabled() && authenticated) {
            log.trace("User {} authenticated with provider {}", user, getDelegate());
        } else if (!authenticated && log.isDebugEnabled()) {
            log.debug("Failed authentication for user {} with provider {}", user, getDelegate());
        }
        return authenticated;
    }

    @Override
    public boolean logout(HttpSession session) { 
        return getDelegate().logout(session);
    }

    @Override
    public boolean requiresUserPass() {
        return getDelegate().requiresUserPass();
    }

    @Override
    public String toString() {
        return super.toString()+"["+getDelegate()+"]";
    }
    
}
