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
package org.apache.brooklyn.rest.security.jaas;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import javax.servlet.http.HttpSession;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.provider.DelegatingSecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// http://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASLMDevGuide.html

/**
 * <p>
 * JAAS module delegating authentication to the {@link SecurityProvider} implementation 
 * configured in {@literal brooklyn.properties}, key {@literal brooklyn.webconsole.security.provider}.
 * 
 * <p>
 * If used in an OSGi environment only implementations visible from {@literal brooklyn-rest-server} are usable by default.
 * To use a custom security provider add the following configuration to the its bundle in {@literal src/main/resources/OSGI-INF/bundle/security-provider.xml}:
 * 
 * <pre>
 * {@code
 *<?xml version="1.0" encoding="UTF-8"?>
 *<blueprint xmlns="http://www.osgi.org/xmlns/blueprint/v1.0.0"
 *           xmlns:jaas="http://karaf.apache.org/xmlns/jaas/v1.1.0"
 *           xmlns:ext="http://aries.apache.org/blueprint/xmlns/blueprint-ext/v1.0.0">
 *
 *    <jaas:config name="karaf" rank="1">
 *        <jaas:module className="org.apache.brooklyn.rest.security.jaas.BrooklynLoginModule"
 *                     flags="required">
 *            brooklyn.webconsole.security.provider.symbolicName=BUNDLE_SYMBOLIC_NAME
 *            brooklyn.webconsole.security.provider.version=BUNDLE_VERSION
 *        </jaas:module>
 *    </jaas:config>
 *
 *</blueprint>
 *}
 * </pre>
 */
// Needs an explicit "org.apache.karaf.jaas.config" Import-Package in the manifest!
public class BrooklynLoginModule implements LoginModule {
    private static final Logger log = LoggerFactory.getLogger(BrooklynLoginModule.class);

    private static class BrooklynPrincipal implements Principal {
        private String name;
        public BrooklynPrincipal(String name) {
            this.name = checkNotNull(name, "name");
        }
        @Override
        public String getName() {
            return name;
        }
        @Override
        public int hashCode() {
            return name.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof BrooklynPrincipal) {
                return name.equals(((BrooklynPrincipal)obj).name);
            }
            return false;
        }
        @Override
        public String toString() {
            return "BrooklynPrincipal[" +name + "]";
        }
    }
    private static final Principal DEFAULT_PRINCIPAL = new BrooklynPrincipal("brooklyn");
    public static final String PROPERTY_BUNDLE_SYMBOLIC_NAME = BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME.getName() + ".symbolicName";
    public static final String PROPERTY_BUNDLE_VERSION = BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME.getName() + ".version";

    private Map<String, ?> options;
    private BundleContext bundleContext;

    private static DelegatingSecurityProvider defaultProvider;
    private HttpSession providerSession;

    private SecurityProvider provider;
    private Subject subject;
    private CallbackHandler callbackHandler;
    private boolean loginSuccess;
    private boolean commitSuccess;

    public BrooklynLoginModule() {
    }

    private SecurityProvider getDefaultProvider() {
        if (defaultProvider == null) {
            createDefaultSecurityProvider(getManagementContext());
        }
        return defaultProvider;
    }

    private synchronized static SecurityProvider createDefaultSecurityProvider(ManagementContext mgmt) {
        if (defaultProvider == null) {
            defaultProvider = new DelegatingSecurityProvider(mgmt);
        }
        return defaultProvider;
    }

    private ManagementContext getManagementContext() {
        return ManagementContextHolder.getManagementContext();
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.options = options;

        this.bundleContext = (BundleContext) options.get(BundleContext.class.getName());

        loginSuccess = false;
        commitSuccess = false;

        initProvider();
    }

    private void initProvider() {
        StringConfigMap brooklynProperties = getManagementContext().getConfig();
        provider = brooklynProperties.getConfig(BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE);
        String symbolicName = (String) options.get(PROPERTY_BUNDLE_SYMBOLIC_NAME);
        String version = (String) options.get(PROPERTY_BUNDLE_VERSION);
        String className = (String) options.get(BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME.getName());
        if (className != null && symbolicName == null) {
            throw new IllegalStateException("Missing JAAS module property " + PROPERTY_BUNDLE_SYMBOLIC_NAME + " pointing at the bundle where to load the security provider from.");
        }
        if (provider != null) return;
        if (symbolicName != null) {
            if (className == null) {
                className = brooklynProperties.getConfig(BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME);
            }
            if (className != null) {
                try {
                    Collection<Bundle> bundles = getMatchingBundles(symbolicName, version);
                    if (bundles.isEmpty()) {
                        throw new IllegalStateException("No bundle " + symbolicName + ":" + version + " found");
                    } else if (bundles.size() > 1) {
                        log.warn("Found multiple bundles matching symbolicName " + symbolicName + " and version " + version + 
                                " while trying to load security provider " + className + ". Will use first one that loads the class successfully.");
                    }
                    provider = tryLoadClass(className, bundles);
                    if (provider == null) {
                        throw new ClassNotFoundException("Unable to load class " + className + " from bundle " + symbolicName + ":" + version);
                    }
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    throw new IllegalStateException("Can not load or create security provider " + className + " for bundle " + symbolicName + ":" + version, e);
                }
            }
        } else {
            log.debug("Delegating security provider loading to Brooklyn.");
            provider = getDefaultProvider();
        }

        log.debug("Using security provider " + provider);
    }

    private SecurityProvider tryLoadClass(String className, Collection<Bundle> bundles)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        for (Bundle b : bundles) {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends SecurityProvider> securityProviderType = (Class<? extends SecurityProvider>) b.loadClass(className);
                return DelegatingSecurityProvider.createSecurityProviderInstance(getManagementContext(), securityProviderType);
            } catch (ClassNotFoundException e) {
            }
        }
        return null;
    }

    private Collection<Bundle> getMatchingBundles(final String symbolicName, final String version) {
        Collection<Bundle> bundles = new ArrayList<>();
        for (Bundle b : bundleContext.getBundles()) {
            if (b.getSymbolicName().equals(symbolicName) &&
                    (version == null || b.getVersion().toString().equals(version))) {
                bundles.add(b);
            }
        }
        return bundles;
    }

    @Override
    public boolean login() throws LoginException {
        if (callbackHandler == null) {
            loginSuccess = false;
            throw new FailedLoginException("Username and password not available");
        }

        NameCallback cbName = new NameCallback("Username: ");
        PasswordCallback cbPassword = new PasswordCallback("Password: ", false);

        Callback[] callbacks = {cbName, cbPassword};

        try {
            callbackHandler.handle(callbacks);
        } catch (IOException ioe) {
            throw new LoginException(ioe.getMessage());
        } catch (UnsupportedCallbackException uce) {
            throw new LoginException(uce.getMessage() + " not available to obtain information from user");
        }
        String user = cbName.getName();
        String password = new String(cbPassword.getPassword());

        providerSession = new SecurityProviderHttpSession();
        if (!provider.authenticate(providerSession, user, password)) {
            loginSuccess = false;
            throw new FailedLoginException("Incorrect username or password");
        }

        loginSuccess = true;
        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        if (loginSuccess) {
            if (subject.isReadOnly()) {
                throw new LoginException("Can't commit read-only subject");
            }
            subject.getPrincipals().add(DEFAULT_PRINCIPAL);
        }

        commitSuccess = true;
        return loginSuccess;
    }

    @Override
    public boolean abort() throws LoginException {
        if (loginSuccess && commitSuccess) {
            removePrincipal();
        }
        return loginSuccess;
    }

    @Override
    public boolean logout() throws LoginException {
        removePrincipal();

        subject = null;
        callbackHandler = null;

        return true;
    }

    private void removePrincipal() throws LoginException {
        if (subject.isReadOnly()) {
            throw new LoginException("Read-only subject");
        }
        subject.getPrincipals().remove(DEFAULT_PRINCIPAL);
    }

}
