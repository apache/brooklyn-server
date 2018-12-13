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

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.provider.DelegatingSecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

// http://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASLMDevGuide.html

/**
 * <p>
 * JAAS module delegating authentication to the {@link SecurityProvider} implementation
 * configured in {@literal brooklyn.properties}, key {@literal brooklyn.webconsole.security.provider}.
 * <p>
 * We have also supported configuring this as options in OSGi;
 * this is now deprecated, but see {@link #initProviderFromOptions(StringConfigMap)} for more info.
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
                return name.equals(((BasicPrincipal) obj).name);
            }
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + name + "]";
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
    /**
     * SecurityProvider doesn't know about roles, just attach one by default. Use the one specified here or DEFAULT_ROLE
     */
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
        try {
            this.subject = subject;
            this.callbackHandler = callbackHandler;
            this.options = options;
    
            this.bundleContext = (BundleContext) options.get(BundleContext.class.getName());
    
            loginSuccess = false;
            commitSuccess = false;
    
            initProvider();
            
        } catch (Exception e) {
            log.warn("Unable to initialize BrooklynLoginModule: "+e, e);
            Exceptions.propagateIfFatal(e);
        }
    }

    private void initProvider() {
        // use existing (shared) provider if there is one, for speed
        // (note this login module class gets a different instance on each request; caching the provider is a big efficiency gain) 
        StringConfigMap brooklynProperties = getManagementContext().getConfig();
        provider = brooklynProperties.getConfig(BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE);
        if (provider != null) return;
        provider = getManagementContext().getScratchpad().get(BrooklynWebConfig.SECURITY_PROVIDER_INSTANCE);
        if (provider != null) return;

        initProviderFromOptions(brooklynProperties);
        if (provider==null) {
            // no osgi options set, so use the standard properties-based one (usual path)
            provider = createDefaultSecurityProvider(getManagementContext());
        }

        log.debug("Using security provider " + provider);
    }

    /**
     * We have since switching to OSGi also allowed the provider to be specified as an option in the
     * OSGi blueprint. This has never been used AFAIK in the real world but it was the only way to specify a bundle. 
     * Note that only implementations visible from {@literal brooklyn-rest-server} are usable by default.
     * We now support specifying a bundle for the delegate, but this is being left in as deprecated.
     * To use this <b>deprecated</b> configuration with a custom security provider, 
     * add the following configuration to the its bundle in {@literal src/main/resources/OSGI-INF/bundle/security-provider.xml}:
     * <p>
     * <pre>
     * {@code
     * <?xml version="1.0" encoding="UTF-8"?>
     * <blueprint xmlns="http://www.osgi.org/xmlns/blueprint/v1.0.0"
     *           xmlns:jaas="http://karaf.apache.org/xmlns/jaas/v1.1.0"
     *           xmlns:ext="http://aries.apache.org/blueprint/xmlns/blueprint-ext/v1.0.0">
     *
     *    <jaas:config name="karaf" rank="1">
     *        <jaas:module className="org.apache.brooklyn.rest.security.jaas.BrooklynLoginModule" flags="required">
     *            brooklyn.webconsole.security.provider.symbolicName=BUNDLE_SYMBOLIC_NAME
     *            brooklyn.webconsole.security.provider.version=BUNDLE_VERSION
     *        </jaas:module>
     *    </jaas:config>
     *
     * </blueprint>
     * }
     * </pre>
     * @deprecated since 2019-01, use the brooklyn system properties 
     * {@link BrooklynWebConfig#SECURITY_PROVIDER_CLASSNAME},
     * {@link BrooklynWebConfig#SECURITY_PROVIDER_BUNDLE}, and
     * {@link BrooklynWebConfig#SECURITY_PROVIDER_BUNDLE_VERSION},
     */
    protected void initProviderFromOptions(StringConfigMap brooklynProperties) {
        // this uses *options* to determine the security provider to load
        // (not sure this is ever used)
        String symbolicName = (String) options.get(PROPERTY_BUNDLE_SYMBOLIC_NAME);
        String version = (String) options.get(PROPERTY_BUNDLE_VERSION);
        String className = (String) options.get(BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME.getName());
        if (className != null && symbolicName == null) {
            throw new IllegalStateException("Missing JAAS module property " + PROPERTY_BUNDLE_SYMBOLIC_NAME + " pointing at the bundle where to load the security provider from.");
        }
        if (symbolicName != null) {
            if (className == null) {
                className = brooklynProperties.getConfig(BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME);
            }
            if (className != null) {
                provider = loadProviderFromBundle(getManagementContext(), bundleContext, symbolicName, version, className);
            }
        }
    }

    public static SecurityProvider loadProviderFromBundle(
            ManagementContext mgmt, BundleContext bundleContext,
            String symbolicName, String version, String className) {
        try {
            Collection<Bundle> bundles = getMatchingBundles(bundleContext, symbolicName, version);
            if (bundles.isEmpty()) {
                throw new IllegalStateException("No bundle " + symbolicName + ":" + version + " found");
            } else if (bundles.size() > 1) {
                log.warn("Found multiple bundles matching symbolicName " + symbolicName + " and version " + version +
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

    @Override
    public boolean login() throws LoginException {
        try {
            log.info("ALEX BLM login - "+callbackHandler+" "+this+" "+provider);
            String user=null, password=null;
            
            if (provider.requiresUserPass()) {
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
                user = cbName.getName();
                password = new String(cbPassword.getPassword());
            }
    
    
            Request req = getJettyRequest();
            if (req != null) {
                providerSession = req.getSession(false);
            }
            log.info("GOT SESSION - "+providerSession);
            if (providerSession == null) {
                providerSession = new SecurityProviderHttpSession();
            }
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
            }
            
            loginSuccess = true;
            return true;
        } catch (LoginException e) {
            throw e;
        } catch (Exception e) {
            log.warn("Unexpected error during login: "+e, e);
            throw e;
        }
    }

    @Override
    public boolean commit() throws LoginException {
        log.info("ALEX BLM BR LOGIN - COMMIT");
        if (loginSuccess && principals!=null && !principals.isEmpty()) {
            // for oauth principals aren't set currently; they don't seem to be needed
            
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
        log.info("ALEX BLM abort");
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
        if (principals==null || principals.isEmpty()) return;
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
        return Optional.ofNullable(HttpConnection.getCurrentConnection())
                .map(HttpConnection::getHttpChannel)
                .map(HttpChannel::getRequest)
                .orElse(null);
    }

}
