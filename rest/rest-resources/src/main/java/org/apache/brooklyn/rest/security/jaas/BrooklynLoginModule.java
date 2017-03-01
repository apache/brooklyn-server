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
import org.apache.brooklyn.util.text.Strings;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Request;
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

    /**
     * The session attribute set for authenticated users; for reference
     * (but should not be relied up to confirm authentication, as
     * the providers may impose additional criteria such as timeouts,
     * or a null user (no login) may be permitted)
     */
    public static final String AUTHENTICATED_USER_SESSION_ATTRIBUTE = "brooklyn.user";

    private static class BasicPrincipal implements Principal {
        private String name;
        public BasicPrincipal(String name) {
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
            if (obj instanceof BasicPrincipal) {
                return name.equals(((BasicPrincipal)obj).name);
            }
            return false;
        }
        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" +name + "]";
        }
    }
    public static class UserPrincipal extends BasicPrincipal {
        public UserPrincipal(String name) {
            super(name);
        }
    }
    public static class RolePrincipal extends BasicPrincipal {
        public RolePrincipal(String name) {
            super(name);
        }
    }

    public static final String PROPERTY_BUNDLE_SYMBOLIC_NAME = BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME.getName() + ".symbolicName";
    public static final String PROPERTY_BUNDLE_VERSION = BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME.getName() + ".version";
    /** SecurityProvider doesn't know about roles, just attach one by default. Use the one specified here or DEFAULT_ROLE */
    public static final String PROPERTY_ROLE = BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME.getName() + ".role";
    public static final String DEFAULT_ROLE = "webconsole";

    private Map<String, ?> options;
    private BundleContext bundleContext;

    private HttpSession providerSession;

    private SecurityProvider provider;
    private Subject subject;
    private CallbackHandler callbackHandler;
    private boolean loginSuccess;
    private boolean commitSuccess;
    private Collection<Principal> principals;

    public BrooklynLoginModule() {
    }

    private synchronized static SecurityProvider createDefaultSecurityProvider(ManagementContext mgmt) {
        return new DelegatingSecurityProvider(mgmt);
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
        provider = getManagementContext().getScratchpad().get(BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE);
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
            provider = createDefaultSecurityProvider(getManagementContext());
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

        Request req = getJettyRequest();
        if (req != null) {
            String remoteAddr = req.getRemoteAddr();
            providerSession.setAttribute(BrooklynWebConfig.REMOTE_ADDRESS_SESSION_ATTRIBUTE, remoteAddr);
        }

        if (!provider.authenticate(providerSession, user, password)) {
            loginSuccess = false;
            throw new FailedLoginException("Incorrect username or password");
        }

        if (user != null) {
            providerSession.setAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE, user);
        }

        principals = new ArrayList<>(2);
        principals.add(new UserPrincipal(user));
        // Could introduce a new interface SecurityRoleAware, implemented by
        // the SecurityProviders, returning the roles a user has assigned.
        // For now a static role is good enough.
        String role = (String) options.get(PROPERTY_ROLE);
        if (role == null) {
            role = DEFAULT_ROLE;
        }
        if (Strings.isNonEmpty(role)) {
            principals.add(new RolePrincipal(role));
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
            subject.getPrincipals().addAll(principals);
        }

        commitSuccess = true;
        return loginSuccess;
    }

    @Override
    public boolean abort() throws LoginException {
        if (loginSuccess && commitSuccess) {
            removePrincipal();
        }
        clear();
        return loginSuccess;
    }

    @Override
    public boolean logout() throws LoginException {
        Request req = getJettyRequest();
        if (req != null) {
            log.info("REST logging {} out",
                    providerSession.getAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE));
            provider.logout(req.getSession());
            req.getSession().removeAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE);
        } else {
            log.error("Request object not available for logout");
        }

        removePrincipal();
        clear();
        return true;
    }

    private void removePrincipal() throws LoginException {
        if (subject.isReadOnly()) {
            throw new LoginException("Read-only subject");
        }
        subject.getPrincipals().removeAll(principals);
    }

    private void clear() {
        subject = null;
        callbackHandler = null;
        principals = null;
    }

    private Request getJettyRequest() {
        HttpChannel<?> channel = HttpChannel.getCurrentHttpChannel();
        if (channel != null) {
             return channel.getRequest();
        } else {
            return null;
        }
    }

}
