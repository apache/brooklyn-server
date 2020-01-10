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
package org.apache.brooklyn.rest.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.EnumerationUtils;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.session.Session;
import org.eclipse.jetty.server.session.SessionHandler;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Convenience to assist working with multiple sessions, ensuring requests in different bundles can
 * get a consistent shared view of data.
 * <p>
 * Any processing that wants to {@link #getAttribute(String)}, {@link #setAttribute(String, Object)} or {@link #removeAttribute(String)}
 * in a way that all Brooklyn bundles will see (e.g. authentication, etc) should use the methods in this class,
 * as opposed to calling {@link HttpServletRequest#getSession()} then {@link HttpSession#getAttribute(String)}.
 * <p>
 * This class will follow heuristics to find a preferred shared session.  The heuristics are as follows:
 * <ul>
 * <li>We look at the {@link Server} for a {@link #KEY_PREFERRED_SESSION_HANDLER_INSTANCE}, and
 *     if it has a session with {@link #KEY_IS_PREFERRED}, we use it
 * <li>We look in all session handlers for a session marked as {@link #KEY_IS_PREFERRED}
 * <li>If there is a {@link #KEY_PREFERRED_SESSION_HANDLER_INSTANCE} at the server,
 *     we create a session there (if needed, and if we can, ie we have the request)
 *     and mark it (or one already existing there) as {@link #KEY_IS_PREFERRED}
 * <li>If there is no {@link #KEY_PREFERRED_SESSION_HANDLER_INSTANCE} at the server,
 *     we go through all bundles looking for the CXF one and we store that against the {@link Server}
 *     (this should only happen once, will log warnings if not found)
 * <li>Finally we mark the originating session as the {@link #KEY_IS_PREFERRED} so that it
 *     is used in preference to others, including ones at the {@link #KEY_PREFERRED_SESSION_HANDLER_INSTANCE},
 *     if we were invoked in a way where we couldn't find such a handler (ie no such bundle) and we couldn't create one there
 * </ul>
 * This class may have the occasional quirk if run in parallel but we can live with that,
 * and the danger I think is confined to inconsistent sessions.
 * It logs in that case and other fringe cases.
 * <p>
 * In future we may wish to make this more configurable, so bundles can specify a priority
 * or a configuration property could specify the preferred bundles and/or context paths.
 * But for now hard-coding CXF is fine.
 * <p>
 * Obviously code that bypasses this and uses the {@link HttpServletRequest#getSession()}
 * will only read/write things in their session, and it won't be visible to requests from other bundles.
 * <p>
 * Previously we tried sharing sessions across bundles; you'll see in git history the code for that;
 * however it was messy, and ended up being dodgy when we invalidate, and likely very dodgy if the
 * system auto-invalidates (e.g. time- or memory-based expiry), as code there is quite fragile assuming
 * sessions are one-to-one tied to their handlers.
 */
public class MultiSessionAttributeAdapter {

    private static final Logger log = LoggerFactory.getLogger(MultiSessionAttributeAdapter.class);
    
    private static final String KEY_PREFERRED_SESSION_HANDLER_INSTANCE = "org.apache.brooklyn.server.PreferredSessionHandlerInstance";
    private static final String KEY_IS_PREFERRED = "org.apache.brooklyn.server.IsPreferred";

    public final static ConfigKey<Long> MAX_SESSION_AGE = ConfigKeys.newLongConfigKey(
            "org.apache.brooklyn.server.maxSessionAge", "Max session age in seconds");

    public final static ConfigKey<Integer> MAX_INACTIVE_INTERVAL = ConfigKeys.newIntegerConfigKey(
            "org.apache.brooklyn.server.maxInactiveInterval", "Max inactive interval in seconds",
            3601);

    private static final Object PREFERRED_SYMBOLIC_NAME =
        "org.apache.cxf.cxf-rt-transports-http";
        //// our bundle here doesn't have a session handler; sessions to the REST API get the handler from CXF
        //"org.apache.brooklyn.rest.rest-resources";
    
    private final HttpSession preferredSession;
    private final HttpSession localSession;
    private final ManagementContext mgmt;

    private boolean silentlyAcceptLocalOnlyValues = false;
    private boolean setLocalValuesAlso = false;
    private boolean setAllKnownSessions = false;
    
    private static final Factory FACTORY = new Factory();

    protected MultiSessionAttributeAdapter(HttpSession preferredSession, HttpSession localSession, HttpServletRequest request) {
        this.preferredSession = preferredSession;
        this.localSession = localSession;
        ServletContext servletContext = request!=null ? request.getServletContext() : localSession!=null ? localSession.getServletContext() : preferredSession!=null ? preferredSession.getServletContext() : null;
        if(servletContext != null){
            this.mgmt = new ManagementContextProvider(servletContext).getManagementContext();
        }
        else{
            this.mgmt = null;
        }
        resetExpiration();
    }

    public MultiSessionAttributeAdapter(HttpSession preferredSession, HttpSession session) {
        this(preferredSession, session, null);
    }

    public static MultiSessionAttributeAdapter of(HttpServletRequest r) {
        return of(r, true);
    }
    /** May return null iff create is false */
    public static MultiSessionAttributeAdapter of(HttpServletRequest r, boolean create) {
        HttpSession session = r.getSession(create);
        if (session==null) return null;
        return new MultiSessionAttributeAdapter(FACTORY.findPreferredSession(r), session, r);
    }
    
    /** Where the request isn't available, and the preferred session is expected to exist.
     * Note we cannot create a new session at the preferred handler without a request. */
    public static MultiSessionAttributeAdapter of(HttpSession session) {
        return new MultiSessionAttributeAdapter(FACTORY.findPreferredSession(session, null), session);
    }


    protected static class Factory {

        private HttpSession findPreferredSession(HttpServletRequest r) {
            if (r.getSession(false)==null) {
                log.warn("Creating session", new Exception("source of created session"));
                r.getSession();
            }
            return findPreferredSession(r.getSession(), r);
        }

        private HttpSession findPreferredSession(HttpSession localSession, HttpServletRequest optionalRequest) {
            HttpSession preferredSession = findValidPreferredSession(localSession, optionalRequest);

            //TODO just check this the first time preferred session is accessed on a given request (when it is looked up)

            ManagementContext mgmt = null;
            ServletContext servletContext = optionalRequest!=null ? optionalRequest.getServletContext() : localSession!=null ? localSession.getServletContext() : preferredSession!=null ? preferredSession.getServletContext() : null;
            if(servletContext != null){
                mgmt = new ManagementContextProvider(servletContext).getManagementContext();
            }

            boolean isValid = ((Session)preferredSession).isValid();
            if (!isValid) {
                throw new SessionExpiredException("Session invalidated", SessionErrors.SESSION_INVALIDATED, optionalRequest);
            }

            if(mgmt !=null){
                Long maxSessionAge = mgmt.getConfig().getConfig(MAX_SESSION_AGE);
                if (maxSessionAge!=null) {
                    if (isAgeExceeded(preferredSession, maxSessionAge)) {
                        invalidateAllSession(preferredSession, localSession);
                        throw new SessionExpiredException("Max session age exceeded", SessionErrors.SESSION_AGE_EXCEEDED, optionalRequest);
                    }
                }
            }

            return preferredSession;
        }

        private boolean isAgeExceeded(HttpSession preferredSession, Long maxSessionAge) {
            return preferredSession.getCreationTime() + maxSessionAge*1000 < System.currentTimeMillis();
        }

        private void invalidateAllSession(HttpSession preferredSession, HttpSession localSession) {
            Server server = ((Session)preferredSession).getSessionHandler().getServer();
            final Handler[] handlers = server.getChildHandlersByClass(SessionHandler.class);
            List<String> invalidatedSessions = new ArrayList<>();
            if (handlers!=null) {
                for (Handler h: handlers) {
                    Session session = ((SessionHandler)h).getSession(preferredSession.getId());
                    if (session!=null) {
                        invalidatedSessions.add(session.getId());
                        session.invalidate();
                    }
                }
            }
            if(!invalidatedSessions.contains(localSession.getId())){
                localSession.invalidate();
            }
        }

        private HttpSession findValidPreferredSession(HttpSession localSession, HttpServletRequest optionalRequest) {
            if (localSession instanceof Session) {
                SessionHandler preferredHandler = getPreferredJettyHandler((Session)localSession, true, true);
                HttpSession preferredSession = preferredHandler==null ? null : preferredHandler.getHttpSession(localSession.getId());
                if (log.isTraceEnabled()) {
                    log.trace("Preferred session for "+info(optionalRequest, localSession)+": "+
                        (preferredSession!=null ? info(preferredSession) : "none, willl make new session in "+info(preferredHandler)));
                }
                if (preferredSession!=null) {
                    return preferredSession;
                }
                if (preferredHandler!=null) {
                    if (optionalRequest!=null) { 
                        HttpSession result = preferredHandler.newHttpSession(optionalRequest);
                        // bigger than HouseKeeper.sessionScavengeInterval: 3600
                        // https://www.eclipse.org/jetty/documentation/9.4.x/session-configuration-housekeeper.html
                        if (log.isTraceEnabled()) {
                            log.trace("Creating new session "+info(result)+" to be preferred for " + info(optionalRequest, localSession));
                        }
                        return result;
                    }
                    // the server has a preferred handler, but no session yet; fall back to marking on the session 
                    log.warn("No request so cannot create preferred session at preferred handler "+info(preferredHandler)+" for "+info(optionalRequest, localSession)+"; will exceptionally mark the calling session as the preferred one");
                    markSessionAsPreferred(localSession, " (request came in for "+info(optionalRequest, localSession)+")");
                    return localSession;
                } else {
                    // shouldn't come here; at minimum it should have returned the local session's handler
                    log.warn("Unexpected failure to find a handler for "+info(optionalRequest, localSession));
                }
            } else {
                log.warn("Unsupported session impl in "+info(optionalRequest, localSession));
            }
            return localSession;
        }
        
        private SessionHandler getPreferredJettyHandler(Session localSession, boolean allowHandlerThatDoesntHaveSession, boolean markAndReturnThisIfNoneFound) {
            SessionHandler localHandler = ((Session)localSession).getSessionHandler();
            Server server = localHandler.getServer();
            // NB: this can also be useful: ((DefaultSessionIdManager)localHandler.getSessionIdManager())
    
            if (server!=null) {
                Session sessionAtServerGlobalPreferredHandler = null;
                
                // does the server have a globally preferred handler
                SessionHandler preferredServerGlobalSessionHandler = getServerGlobalPreferredHandler(server);
                if (preferredServerGlobalSessionHandler!=null) {
                    sessionAtServerGlobalPreferredHandler = preferredServerGlobalSessionHandler.getSession(localSession.getId());
                    if (sessionAtServerGlobalPreferredHandler!=null && Boolean.TRUE.equals( sessionAtServerGlobalPreferredHandler.getAttribute(KEY_IS_PREFERRED)) ) {
                        return preferredServerGlobalSessionHandler;
                    }
                }

                Handler[] handlers = server.getChildHandlersByClass(SessionHandler.class);

                // if there is a session marked, use it, unless the server has a preferred session handler and it has an equivalent session
                // this way if a session is marked (from use in a context where we don't have a web request) it will be used
                SessionHandler preferredHandlerForMarkedSession = findPeerSessionMarkedPreferred(localSession.getId(), handlers);
                if (preferredHandlerForMarkedSession!=null) return preferredHandlerForMarkedSession;

                // nothing marked as preferred; if server global handler has a session, mark it as preferred
                // this way it will get found quickly on subsequent requests
                if (sessionAtServerGlobalPreferredHandler!=null) {
                    sessionAtServerGlobalPreferredHandler.setAttribute(KEY_IS_PREFERRED, true);
                    return preferredServerGlobalSessionHandler;
                }

                if (allowHandlerThatDoesntHaveSession && preferredServerGlobalSessionHandler!=null) {
                    return preferredServerGlobalSessionHandler;
                }
                if (preferredServerGlobalSessionHandler==null) {
                    preferredServerGlobalSessionHandler = findPreferredBundleHandler(localSession, server, handlers);
                    if (preferredServerGlobalSessionHandler!=null) {
                        // recurse
                        return getPreferredJettyHandler(localSession, allowHandlerThatDoesntHaveSession, markAndReturnThisIfNoneFound);
                    }
                }

                if (markAndReturnThisIfNoneFound) {
                    // nothing detected as preferred ... let's mark this session as the preferred one
                    markSessionAsPreferred(localSession, " (this is the handler that the request came in on)");
                    return localHandler;
                }

            } else {
                log.warn("Could not find server for "+info(localSession));
            }
            return null;
        }

        protected void markSessionAsPreferred(HttpSession localSession, String msg) {
            if (log.isTraceEnabled()) {
                log.trace("Recording on "+info(localSession)+" that it is the preferred session"+msg);
            }
            localSession.setAttribute(KEY_IS_PREFERRED, true);
        }

        protected SessionHandler findPreferredBundleHandler(Session localSession, Server server, Handler[] handlers) {
            if (PREFERRED_SYMBOLIC_NAME==null) return null;

            SessionHandler preferredHandler = null;

            if (handlers != null) {
                for (Handler handler: handlers) {
                    SessionHandler sh = (SessionHandler) handler;
                    ContextHandler.Context ctx = getContext(sh);
                    if (ctx!=null) {
                        BundleContext bundle = (BundleContext) ctx.getAttribute("osgi-bundlecontext");
                        if (bundle!=null) {
                            if (PREFERRED_SYMBOLIC_NAME.equals(bundle.getBundle().getSymbolicName())) {
                                if (preferredHandler==null) {
                                    preferredHandler = sh;
                                    server.setAttribute(KEY_PREFERRED_SESSION_HANDLER_INSTANCE, sh);
                                    log.trace("Recording "+info(sh)+" as server-wide preferred session handler");
                                } else {
                                    log.warn("Multiple preferred session handlers detected; keeping "+info(preferredHandler)+", ignoring "+info(sh));
                                }
                            }
                        }
                    }
                }
            }
            if (preferredHandler==null) {
                log.warn("Did not find handler in bundle "+PREFERRED_SYMBOLIC_NAME+"; not using server-wide handler; check whether bundle is installed!");
            }
            return preferredHandler;
        }

        protected SessionHandler findPeerSessionMarkedPreferred(String localSessionId, Handler[] handlers) {
            SessionHandler preferredHandler = null;
            // are any sessions themselves marked as primary
            if (handlers != null) {
                for (Handler h: handlers) {
                    SessionHandler sh = (SessionHandler)h;
                    Session sessionHere = sh.getSession(localSessionId);
                    if (sessionHere!=null) {
                        if (Boolean.TRUE.equals(sessionHere.getAttribute(KEY_IS_PREFERRED))) {
                            if (preferredHandler!=null) {
                                // could occasionally happen on race, but should be extremely unlikely
                                log.warn("Multiple sessions marked as preferred for "+localSessionId+"; using "+info(preferredHandler)+" not "+info(sh));
                                sessionHere.setAttribute(KEY_IS_PREFERRED, null);
                            } else {
                                preferredHandler = sh;
                            }
                        }
                    }
                }
            }
            return preferredHandler;
        }

        protected SessionHandler getServerGlobalPreferredHandler(Server server) {
            SessionHandler preferredHandler = (SessionHandler) server.getAttribute(KEY_PREFERRED_SESSION_HANDLER_INSTANCE);
            if (preferredHandler!=null) {
                if (preferredHandler.isRunning()) {
                    if (log.isTraceEnabled()) {
                        log.trace("Found "+info(preferredHandler)+" as server-wide preferred handler");
                    }
                    return preferredHandler;
                }
                log.warn("Preferred session handler "+info(preferredHandler)+" detected on server is not running; resetting");
            }
            return null;
        }

        enum SessionErrors {
            SESSION_INVALIDATED, SESSION_AGE_EXCEEDED
        }

        private class SessionExpiredException extends WebApplicationException {
            public SessionExpiredException(String message, SessionErrors error_status, HttpServletRequest optionalRequest) {
                super(message, buildExceptionResponse(error_status, optionalRequest, message));
            }
        }

        private static Response buildExceptionResponse(SessionErrors error_status, HttpServletRequest optionalRequest, String message) {
            String mediaType;
            String responseData;

            if(requestIsHtml(optionalRequest)){
                mediaType = MediaType.TEXT_HTML;
                StringBuilder sb = new StringBuilder("<p>")
                        .append(message)
                        .append("</p>\n")
                        .append("<p>")
                        .append("Please go <a href=\"")
                        .append(optionalRequest.getRequestURL())
                        .append("\">here</a> to refresh.")
                        .append("</p>");
                responseData = sb.toString();
            }else{
                mediaType = MediaType.APPLICATION_JSON;
                JsonObject jsonEntity = new JsonObject();
                jsonEntity.addProperty(error_status.toString(), true);
                responseData = jsonEntity.toString();
            }
            return Response.status(Response.Status.FORBIDDEN)
                    .header(HttpHeader.CONTENT_TYPE.asString(), mediaType)
                    .entity(responseData).build();
        }

        private static boolean requestIsHtml(HttpServletRequest optionalRequest) {
            Set headerList = separateOneLineMediaTypes(EnumerationUtils.toList(optionalRequest.getHeaders(HttpHeaders.ACCEPT)));
            Set defaultMediaTypes = ImmutableSet.of(MediaType.TEXT_HTML, MediaType.APPLICATION_XHTML_XML, MediaType.APPLICATION_XML);
            if(CollectionUtils.containsAny(headerList,defaultMediaTypes)){
                return true;
            }
            return false;
        }

        private static Set separateOneLineMediaTypes(List<String> toList) {
            Set<String> mediatypes = new HashSet<>();
            toList.stream().forEach(headerLine -> mediatypes.addAll(Arrays.asList(headerLine.split(",|,\\s"))));
            return mediatypes;
        }
    }

    private static String getContextPath(Handler h) {
        if (h instanceof SessionHandler) {
            ContextHandler.Context ctx = getContext((SessionHandler)h);
            if (ctx!=null) {
                return ctx.getContextPath();
            }
        }
        return null;
    }

    private static String getBundle(Handler h) {
        if (h instanceof SessionHandler) {
            ContextHandler.Context ctx = getContext((SessionHandler)h);
            if (ctx!=null) {
                BundleContext bundle = (BundleContext) ctx.getAttribute("osgi-bundlecontext");
                if (bundle!=null) return bundle.getBundle().getSymbolicName();
            }
        }
        return null;
    }

    private static String getContextPath(HttpSession h) {
        if (h instanceof Session) {
            return getContextPath( ((Session)h).getSessionHandler() );
        }
        return null;
    }

    private static String getBundle(HttpSession h) {
        if (h instanceof Session) {
            ContextHandler.Context ctx = getContext(((Session)h).getSessionHandler());
            if (ctx!=null) {
                BundleContext bundle = (BundleContext) ctx.getAttribute("osgi-bundlecontext");
                if (bundle!=null) return bundle.getBundle().getSymbolicName();
            }
        }
        return null;
    }

    protected static ContextHandler.Context getContext(SessionHandler sh) {
        try {
            Field f = SessionHandler.class.getDeclaredField("_context");
            f.setAccessible(true);
            return (ContextHandler.Context) f.get(sh);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // else ignore, security doesn't allow finding context
            return null;
        }
    }
    
    private static String info(HttpServletRequest req, HttpSession sess) {
        if (req!=null) return info(req);
        return info(sess);
    }

    public static String info(HttpServletRequest r) {
        if (r==null) return "null";
        return ""+r+"["+r.getRequestURI()+"@"+info(r.getSession(false))+"]";
    }

    public static String info(SessionHandler h) {
        if (h==null) return "null";
        return ""+h+"["+(Strings.isBlank(getContextPath(h)) ? getBundle(h) : getContextPath(h))+"]";
    }

    public static String info(HttpSession s) {
        if (s==null) return "null";
        String hh = getContextPath(s);
        if (Strings.isBlank(hh)) hh = getBundle(s);
        if (Strings.isBlank(hh)) {
            hh = s instanceof Session ? info( ((Session)s).getSessionHandler() ) : "<non-jetty>";
        }
        return ""+s+"["+s.getId()+" @ "+hh+"]";
    }


    public MultiSessionAttributeAdapter configureWhetherToSilentlyAcceptLocalOnlyValues(boolean silentlyAcceptLocalOnlyValues) {
        this.silentlyAcceptLocalOnlyValues = silentlyAcceptLocalOnlyValues;
        return this;
    }
    public MultiSessionAttributeAdapter configureWhetherToSetLocalValuesAlso(boolean setLocalValuesAlso) {
        this.setLocalValuesAlso = setLocalValuesAlso;
        return this;
    }
    public MultiSessionAttributeAdapter configureWhetherToSetInAll(boolean setAllKnownSessions) {
        this.setAllKnownSessions = setAllKnownSessions;
        return this;
    }
    
    
    public Object getAttribute(String name) {
        Object v = preferredSession.getAttribute(name);
        if (v==null) {
            v = localSession.getAttribute(name);
            if (v!=null && !silentlyAcceptLocalOnlyValues) {
                log.warn(this+" found value for '"+name+"' in local session but not in preferred session; ensure value is written using this class if it is going to be read with this class");
                preferredSession.setAttribute(name, v);
            }
        }
        return v;
    }

    public void setAttribute(String name, Object value) {
        if (setAllKnownSessions) {
            Handler[] hh = getSessionHandlers();
            if (hh!=null) {
                for (Handler h: hh) {
                    Session ss = ((SessionHandler)h).getSession(localSession.getId());
                    if (ss!=null) {
                        ss.setAttribute(name, value);
                    }
                }
                return;
            } else {
                if (!setLocalValuesAlso) {
                    // can't do all, but at least to local
                    configureWhetherToSetLocalValuesAlso(true);
                }
            }
        }
        preferredSession.setAttribute(name, value);
        if (setLocalValuesAlso) {
            localSession.setAttribute(name, value);
        }
    }
    
    public void removeAttribute(String name) {
        if (setAllKnownSessions) {
            Handler[] hh = getSessionHandlers();
            if (hh!=null) {
                for (Handler h: hh) {
                    Session ss = ((SessionHandler)h).getSession(localSession.getId());
                    if (ss!=null) {
                        ss.removeAttribute(name);
                    }
                }
                return;
            } else {
                if (!setLocalValuesAlso) {
                    // can't do all, but at least to local
                    configureWhetherToSetLocalValuesAlso(true);
                }
            }
        }
        preferredSession.removeAttribute(name);
        if (setLocalValuesAlso) {
            localSession.removeAttribute(name);
        }
    }

    protected Handler[] getSessionHandlers() {
        Server srv = getServer();
        Handler[] handlers = null;
        if (srv!=null) {
            handlers = srv.getChildHandlersByClass(SessionHandler.class);
        }
        return handlers;
    }

    protected Server getServer() {
        Server server = null;
        if (localSession instanceof Session) {
            server = ((Session)localSession).getSessionHandler().getServer();
            if (server!=null) return server;
        }
        if (preferredSession instanceof Session) {
            server = ((Session)preferredSession).getSessionHandler().getServer();
            if (server!=null) return server;
        }
        return null;
    }

    public HttpSession getPreferredSession() {
        return preferredSession;
    }

    public HttpSession getOriginalSession() {
        return localSession;
    }

    public String getId() {
        return getPreferredSession().getId();
    }

    public MultiSessionAttributeAdapter resetExpiration() {
        // force all sessions with this ID to be marked used so they are not expired
        // (if _any_ session with this ID is expired, then they all are, even if another
        // with the same ID is in use or has a later expiry)
        Integer maxInativeInterval = MAX_INACTIVE_INTERVAL.getDefaultValue();
        if(this.mgmt != null){
            maxInativeInterval = mgmt.getConfig().getConfig(MAX_INACTIVE_INTERVAL);
        }
        Handler[] hh = getSessionHandlers();
        if (hh!=null) {
            for (Handler h: hh) {
                Session ss = ((SessionHandler)h).getSession(getId());
                if (ss!=null) {
                    ss.setMaxInactiveInterval(maxInativeInterval);
                }
            }
        }
        return this;
    }
}
