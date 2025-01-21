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
package org.apache.brooklyn.rest.filter;

import com.google.common.collect.ImmutableList;

import java.security.Principal;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.LoginLogging;
import org.apache.brooklyn.rest.security.provider.DelegatingSecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider.SecurityProviderDeniedAuthentication;
import org.apache.brooklyn.rest.util.MultiSessionAttributeAdapter;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a filter that performs authentication with the {@link SecurityProvider}
 * as configured according to {@link BrooklynWebConfig#SECURITY_PROVIDER_CLASSNAME}.
 * 
 * This replaces the JAAS "BrooklynLoginModule" because that login module requires
 * Basic auth, which is not flexible enough to support redirect-based solutions like Oauth.
 * 
 * Unfortunately we seem to need two filters, the Jersey filter for the REST bundle,
 * and the Javax filter for the static content bundles (in brooklyn-ui/ui-modules).
 * (We could set up our own Jersey servlet or blueprint for the static content bundles
 * to re-use the Jersey filter, but that seems like overkill; and surely there's an easy
 * way to set the Javax filter to run for the REST bundle inside blueprint.xml, but a
 * few early attempts didn't succeed and the approach of having two filters seems easiest
 * (especially as they share code for the significant parts, in this class).
 * 
 * This does give us the opportunity to differentiate the redirect, so that
 * jersey (REST) requests don't redirect to the auth site, as the redirect requires human intervention.
 * 
 * More unfortunately, the session handlers for the multiple bundles and all different,
 * and the CXF JAX-RS bundles don't allow any configuration of the handlers
 * (see JettyHTTPServerEngine.addServant(..) call to configureSession).
 * So we cheat and modify the request's session handler so that we can use a shared
 * session handler. This means all webapps and jaxrs apps that use this filter will
 * be able to share their session handler, so happily when you logout from one,
 * you log out from all, and when you're authenticated in one you're authenticated in all.
 */
public class BrooklynSecurityProviderFilterHelper {

    private static final ConfigKey<String> UNAUTHENTICATED_ENDPOINTS =
            ConfigKeys.newStringConfigKey(BrooklynWebConfig.BASE_NAME_SECURITY + ".unauthenticated.endpoints",
                    "List of endpoints available without authentication e.g. a login page", "");

    private static final List<String> STATIC_CONTENT_EXTENSIONS = ImmutableList.of(
                ".html", ".htm", ".js", ".png", ".gif", ".jpg", ".svg"
            );

    public interface Responder {
        void error(String message, boolean requiresBasicAuth) throws SecurityProviderDeniedAuthentication;
    }
    
    /**
     * The session attribute set for authenticated users; for reference
     * (but should not be relied up to confirm authentication, as
     * the providers may impose additional criteria such as timeouts,
     * or a null user (no login) may be permitted)
     */
    public static final String AUTHENTICATED_USER_SESSION_ATTRIBUTE = "brooklyn.user";

    private static final Logger log = LoggerFactory.getLogger(BrooklynSecurityProviderFilterHelper.class);

    // TODO this should be parametrisable
    public static final String BASIC_REALM_NAME = "brooklyn";
    
    public static final String BASIC_REALM_HEADER_VALUE = "BASIC realm="+StringEscapes.JavaStringEscapes.wrapJavaString(BASIC_REALM_NAME);
    
    public void run(HttpServletRequest webRequest, ManagementContext mgmt, @Nullable ContainerRequestContext container) throws SecurityProviderDeniedAuthentication {
        SecurityProvider provider = getProvider(mgmt);
        MultiSessionAttributeAdapter preferredSessionWrapper = null;

        try{
            preferredSessionWrapper = MultiSessionAttributeAdapter.of(webRequest, false);
        }catch (WebApplicationException e){
            // there is no valid session

            abort(e.getResponse());
        }

        String unauthenticatedEndpoints = mgmt.getConfig().getConfig(UNAUTHENTICATED_ENDPOINTS);
        if (Strings.isNonBlank(unauthenticatedEndpoints)) {
            for (String s : unauthenticatedEndpoints.split(",")) {
                String accessibleEndpoint = s.startsWith("/") ? s : "/" + s;
                if (webRequest.getContextPath().equals(accessibleEndpoint)) {
                    return;
                }
            }
        }
        if (Strings.isNonBlank(mgmt.getConfig().getConfig(BrooklynSecurityProviderFilterJavax.LOGIN_FORM)) && isStaticContent(webRequest)) {
            // also allow pages' static content to be accessed
            return;
        }

        final HttpSession preferredSession1 = preferredSessionWrapper==null ? null : preferredSessionWrapper.getPreferredSession();
        
        if (log.isTraceEnabled()) {
            log.trace("{} checking {}", this, MultiSessionAttributeAdapter.info(webRequest));
        }
        if (provider.isAuthenticated(preferredSession1)) {
            log.trace("{} already authenticated - {}", this, preferredSession1);
            return;
        }


        String user = null, pass = null;
        if (provider.requiresUserPass()) {
            String authorization = webRequest.getHeader("Authorization");
            if (authorization != null) {
                if (authorization.length()<6) {
                    throw abort("Invalid authorization string (too short)", provider.requiresUserPass());
                }
                String userpass;
                try {
                    userpass = new String(Base64.decodeBase64(authorization.substring(6)));
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    throw abort("Invalid authorization string (not Base64)", provider.requiresUserPass());
                }
                int idxColon = userpass.indexOf(":");
                if (idxColon >= 0) {
                    user = userpass.substring(0, idxColon);
                    pass = userpass.substring(idxColon + 1);
                    if ("logout".equals(user) && "logout".equals(pass)) {
                        // logout:logout sent by UI to clear browser state; force failure here and suppress subsequent error messages in log, after a logout
                        throw abort("Reauthorization required after logout", provider.requiresUserPass());
                    }
                } else {
                    throw abort("Invalid authorization string (no colon after decoding)", provider.requiresUserPass());
                }
            } else {
                throw abort("Authorization required", provider.requiresUserPass());
            }
        }
        
        Supplier<HttpSession> sessionSupplier = () -> {
            return preferredSession1 != null ? preferredSession1 : MultiSessionAttributeAdapter.of(webRequest, true).getPreferredSession();
        };

        try{
            if (provider.authenticate(webRequest, sessionSupplier, user, pass)) {
                // gets new session created after authentication
                HttpSession preferredSession2 = sessionSupplier.get();
                preferredSession2.setAttribute(BrooklynWebConfig.REMOTE_ADDRESS_SESSION_ATTRIBUTE, webRequest.getRemoteAddr());
                if (user != null) {
                    preferredSession2.setAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE, user);
                } else {
                    if (container!=null) {
                        SecurityContext securityContext = container.getSecurityContext();
                        Principal userPrincipal = securityContext!=null ? securityContext.getUserPrincipal() : null;
                        if (userPrincipal!=null) user = userPrincipal.getName();
                    }
                }
                LoginLogging.logLoginIfNotLogged(preferredSession2, user,
                        MutableMap.of("origin", webRequest.getRemoteAddr(), "provider",
                                DelegatingSecurityProvider.getTarget(provider).getClass().getName()));

                return;
            }
        } catch (WebApplicationException e) {
            abort(e.getResponse());
        }

        throw abort("Authentication failed", provider.requiresUserPass());
    }

    boolean isStaticContent(HttpServletRequest webRequest) {
        String servletPath = webRequest.getServletPath();
        if (servletPath!=null) {
            if (servletPath.matches("\\/v[0-9]+\\/")) {
                // disallow API endpoints
                return false;
            }

            if (Objects.equals(stripTrailingSlash(webRequest.getContextPath()), stripTrailingSlash(servletPath))) {
                return true;
            }
            String servletPathLower = servletPath.toLowerCase(Locale.ROOT);
            if (STATIC_CONTENT_EXTENSIONS.stream().anyMatch(
                    //ext -> servletPath.endsWith(ext)  // <-- this seems allowed, but why? it's not effectively final!
                    servletPathLower::endsWith
                    )) return true;
        }
        return false;
    }

    private String stripTrailingSlash(String contextPath) {
        return Strings.removeFromEnd(contextPath, "/");
    }

    SecurityProviderDeniedAuthentication abort(String msg, boolean requiresUserPass) throws SecurityProviderDeniedAuthentication {
        ResponseBuilder response = Response.status(Status.UNAUTHORIZED);
        if (requiresUserPass) {
            response.header(HttpHeader.WWW_AUTHENTICATE.asString(), BASIC_REALM_HEADER_VALUE);
        }
        response.header(HttpHeader.CONTENT_TYPE.asString(), MediaType.TEXT_PLAIN);
        response.entity(msg);
        throw new SecurityProviderDeniedAuthentication(response.build());
    }

    void abort(Response response) throws SecurityProviderDeniedAuthentication {
        throw new SecurityProviderDeniedAuthentication(response);
    }

    SecurityProviderDeniedAuthentication redirect(String path, String msg) throws SecurityProviderDeniedAuthentication {
        ResponseBuilder response = Response.status(Status.FOUND);
        response.header(HttpHeader.LOCATION.asString(), path);
        response.entity(msg);
        throw new SecurityProviderDeniedAuthentication(response.build());
    }

    protected SecurityProvider getProvider(ManagementContext mgmt) {
        // we don't cache here (could, it might be faster) but the delegate does use a cache
        return new DelegatingSecurityProvider(mgmt);
    }

}
