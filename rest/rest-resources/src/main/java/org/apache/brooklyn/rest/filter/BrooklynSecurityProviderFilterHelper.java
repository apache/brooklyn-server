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

import java.lang.reflect.Field;
import java.util.Set;

import javax.servlet.ServletRequestWrapper;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.provider.DelegatingSecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider.SecurityProviderDeniedAuthentication;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.time.Time;
import org.apache.commons.codec.binary.Base64;
import org.apache.cxf.jaxrs.impl.tl.ThreadLocalHttpServletRequest;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandler.Context;
import org.eclipse.jetty.server.session.DefaultSessionCache;
import org.eclipse.jetty.server.session.Session;
import org.eclipse.jetty.server.session.SessionHandler;
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
    
    /** The first session handler encountered becomes the shared handler that replaces all others encountered. */
    private static SessionHandler sharedSessionHandler;
    
    public static boolean onInvalidate(String sessionId) {
        if (sharedSessionHandler==null || !sharedSessionHandler.isRunning()) return false;
        Set<SessionHandler> handlers = sharedSessionHandler.getSessionIdManager().getSessionHandlers();
        log.info("SessionIdManager "+sharedSessionHandler.getSessionIdManager()+" has "+handlers.size()+" handlers, on invalidate "+sessionId);
        for (SessionHandler sh: handlers) {
            try {
                log.info("  "+sh+" "+
                    (sh.getSessionCache() instanceof DefaultSessionCache ? ((DefaultSessionCache)sh.getSessionCache()).exists(sessionId) 
                        ? ((DefaultSessionCache)sh.getSessionCache()).doGet(sessionId) : "<ID-not-present>" : ""));
                Field f = SessionHandler.class.getDeclaredField("_context");
                f.setAccessible(true);
                ContextHandler.Context ctx = (Context) f.get(sh);
                // TODO why do we see multiple handlers with the session?  are they the same?
                log.info("    path "+ctx.getContextPath()+"  name "+ctx.getServletContextName());
            } catch (Exception e) {
                log.warn("Error checking session", e);
            }
        }
        final Session s1 = sharedSessionHandler.getSession(sessionId);
        if (s1==null) return false;
        log.info("INVALIDATING "+s1+" - "+s1.isResident()+" "+s1.isValid());
        
        new Thread("check session "+sessionId) {
            public void run() {
                log.info("RESIDENT "+s1.isResident());
                Object s = ((DefaultSessionCache)sharedSessionHandler.getSessionCache()).doGet(sessionId);
                log.info("LOOKUP GAVE "+s+(s!=null ? " RESIDENT "+((Session)s).isResident() + " VALID "+((Session)s).isValid() : ""));
                Time.sleep(500);
                s = ((DefaultSessionCache)sharedSessionHandler.getSessionCache()).doGet(sessionId);
                log.info("LOOKUP 2 GAVE "+s+(s!=null ? " RESIDENT "+((Session)s).isResident() + " VALID "+((Session)s).isValid() : ""));
                Time.sleep(500);
                s = ((DefaultSessionCache)sharedSessionHandler.getSessionCache()).doGet(sessionId);
                log.info("LOOKUP 3A GAVE "+s+(s!=null ? " RESIDENT "+((Session)s).isResident() + " VALID "+((Session)s).isValid() : ""));
                s = sharedSessionHandler.getSession(sessionId);
                log.info("LOOKUP 3B GAVE "+s+(s!=null ? " RESIDENT "+((Session)s).isResident() + " VALID "+((Session)s).isValid() : ""));
            }
        }.start();
        
//        sharedSessionHandler.invalidate(sessionId);
        
//        if (session==null) return false;
//        
//        try (LogClose lock = new LogClose(session.lock())) {
//            log.debug("Logout for {}, new thread got locker", session);
//            session.invalidate();
//            log.debug("Logout for {}, completed call to invalidate", session);
//        }
//
        return true;
    }
    
    public static class LogClose implements AutoCloseable {
        AutoCloseable delegate;
        LogClose(AutoCloseable delegate) {
            this.delegate = delegate;
            log.debug("Logout, received " + delegate);
        }
        @Override
        public void close() {
            try {
                log.debug("Logout, closing " + delegate);
                delegate.close();
                log.debug("Logout, closed " + delegate);
            } catch (Exception e) {
                log.debug("Logout, error closing " + delegate, e);
                throw Exceptions.propagate(e);
            }
        }
    }
    
    /* check all contexts for sessions; surprisingly hard to configure session management for karaf/pax web container.
     * they _really_ want each servlet to have their own sessions. how you're meant to do oauth for multiple servlets i don't know! */
    public HttpSession getSession(HttpServletRequest webRequest, ManagementContext mgmt, boolean create) throws SecurityProviderDeniedAuthentication {
        if (webRequest instanceof ThreadLocalHttpServletRequest) {
            // CXF requests get this, unwrap to get the jetty request
            webRequest = ((ThreadLocalHttpServletRequest)webRequest).get();
        }
        if (webRequest instanceof ServletRequestWrapper) {
            webRequest = (HttpServletRequest) ((ServletRequestWrapper)webRequest).getRequest();
        }
        if (webRequest instanceof Request) {
            if (sharedSessionHandler==null || !sharedSessionHandler.isRunning()) {
                synchronized (BrooklynSecurityProviderFilterHelper.class) {
                    // TODO should we change the store to use a primary?
                    if (sharedSessionHandler==null || !sharedSessionHandler.isRunning()) {
                        SessionHandler candidateSessionHandler = ((Request)webRequest).getSessionHandler();
                        if (candidateSessionHandler!=null && candidateSessionHandler.getSessionPath()==null) {
                            try {
                                Field f = SessionHandler.class.getDeclaredField("_sessionPath");
                                f.setAccessible(true);
                                f.set(candidateSessionHandler, "/");
                                log.debug("Brooklyn session cookie path hard-coded at / on "+candidateSessionHandler+" from "+webRequest);
                            } catch (Exception e) {
                                Exceptions.propagateIfFatal(e);
                                log.warn("Cannot reset session path for "+candidateSessionHandler+"; will refuse to use it for sessions");
                            }
                        }
                        if (candidateSessionHandler==null || !"/".equals(candidateSessionHandler.getSessionPath())) {
                            // refuse to make the shared handler be one which sets cookies against a sub-path;
                            // that would cause visits to other pages to request new sessions
                            // (and worse those sessions would also be given cookies form this page)
                            log.debug("Brooklyn shared session init, refusing to grant session for "+webRequest.getRequestURI()+" using "+candidateSessionHandler+
                                (candidateSessionHandler==null ? "" : ", "+"session cookie path would be for "+candidateSessionHandler.getSessionPath()));
                            if (create) {
                                throw redirect("/", "Initial log-in must be from root page");
                            }
                            return null;
                        }
                        sharedSessionHandler = candidateSessionHandler;
                        log.debug("Brooklyn shared session handler installed as "+sharedSessionHandler+" ("+sharedSessionHandler.getState()+") from "+webRequest+" "+webRequest.getRequestURI());
                    }
                }
            }
            if (sharedSessionHandler!=((Request)webRequest).getSessionHandler()) {
                // no need to update if multiple calls to this method for the same request
                log.trace("Brooklyn session manager replaced for {} {} - from {} to {}", webRequest.getRequestURI(), webRequest, ((Request)webRequest).getSessionHandler(), sharedSessionHandler);
                ((Request)webRequest).setSessionHandler(sharedSessionHandler);
                if (webRequest.getRequestedSessionId()!=null) {
                    HttpSession old = webRequest.getSession(false);
                    HttpSession s = sharedSessionHandler.getHttpSession(webRequest.getRequestedSessionId());
                    if (old!=null && old!=s) {
                        log.warn("Foreign session detected in request "+webRequest+"; replacing "+old+" with "+s);
                    }
                    if (s==null) {
                        log.debug("Brooklyn user-requested session not found, request "+webRequest+", requested session "+webRequest.getRequestedSessionId()+"; will create if needed");
                    } else if (sharedSessionHandler.isValid(s)) {
                        // later in the handling the session will be put in the original handler for the bundle;
                        // not ideal as the Session is tied to the shared handler, and that breaks invalidation,
                        // but custom invalidation in LogoutResource repairs that
                        ((Request)webRequest).setSession(s);
                    } else {
                        log.info("Brooklyn user-requested session not valid, request "+webRequest+", requested session "+webRequest.getRequestedSessionId()+"; will create if needed");
                    }
                }
            }
        } else {
            log.warn(this+" could not find valid session manager for "+webRequest.getRequestURI()+" ("+webRequest+"); Brooklyn session management may have errors");
        }

        HttpSession s = webRequest.getSession(false);
        if (s!=null) {
            log.trace("Session found for {} {}: {} ({})", webRequest.getRequestURI(), webRequest.getRequestedSessionId(), s, s.getAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE));
            log.info("Session found for {} {}: {} ({})", webRequest.getRequestURI(), webRequest.getRequestedSessionId(), s, s.getAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE));
            return s;
        }
        
        if (create) {
            HttpSession session = webRequest.getSession(true);
            // note, other processes may create the session, that's fine as it will use the right handler;
            // typically that's done when a browser is trying to access when not logged in, e.g. to a new server
            // where the auth doesn't need a session to tell the client he is unauthorized.
            // this means however that the log messages here might not be complete, someone else might create the session.
            // (not sure who _is_ creating the session when not logged in, and it would be more efficient not to have a session
            // for clients who aren't logged in, but it doesn't break things at least!)
            log.trace("Session being created for {} (wanted {}): {}", webRequest.getRequestURI(), webRequest.getRequestedSessionId(), session);
            log.info("Session being created for {} (wanted {}): {}", webRequest.getRequestURI(), webRequest.getRequestedSessionId(), session);
            return session;
        }
        
        return null;  // not found
    }
    
    public void run(HttpServletRequest webRequest, ManagementContext mgmt) throws SecurityProviderDeniedAuthentication {
        SecurityProvider provider = getProvider(mgmt);
        HttpSession session = getSession(webRequest, mgmt, false);
        
        if (provider.isAuthenticated(session)) {
            return;
        }
        
        String user = null, pass = null;
        if (provider.requiresUserPass()) {
            String authorization = webRequest.getHeader("Authorization");
            if (authorization != null) {
                String userpass = new String(Base64.decodeBase64(authorization.substring(6)));
                int idxColon = userpass.indexOf(":");
                if (idxColon >= 0) {
                    user = userpass.substring(0, idxColon);
                    pass = userpass.substring(idxColon + 1);
                } else {
                    throw abort("Invalid authorization string", provider.requiresUserPass());
                }
            } else {
                throw abort("Authorization required", provider.requiresUserPass());
            }
        }
        
        if (session==null) {
            // only create the session if an auth string is supplied
            session = getSession(webRequest, mgmt, true);
        }
        session.setAttribute(BrooklynWebConfig.REMOTE_ADDRESS_SESSION_ATTRIBUTE, webRequest.getRemoteAddr());
        
        if (provider.authenticate(session, user, pass)) {
            if (user != null) {
                session.setAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE, user);
            }
            return;
        }
    
        throw abort("Authentication failed", provider.requiresUserPass());
    }
    
    private SecurityProviderDeniedAuthentication abort(String msg, boolean requiresUserPass) throws SecurityProviderDeniedAuthentication {
        ResponseBuilder response = Response.status(Status.UNAUTHORIZED);
        if (requiresUserPass) {
            response.header(HttpHeader.WWW_AUTHENTICATE.asString(), BASIC_REALM_HEADER_VALUE);
        }
        response.header(HttpHeader.CONTENT_TYPE.asString(), MediaType.TEXT_PLAIN);
        response.entity(msg);
        throw new SecurityProviderDeniedAuthentication(response.build());
    }

    private SecurityProviderDeniedAuthentication redirect(String path, String msg) throws SecurityProviderDeniedAuthentication {
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
