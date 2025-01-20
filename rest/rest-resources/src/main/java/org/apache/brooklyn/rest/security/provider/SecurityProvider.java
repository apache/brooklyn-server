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

import java.util.function.Supplier;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.rest.util.MultiSessionAttributeAdapter;

/**
 * The SecurityProvider is responsible for doing authentication.
 *
 * A class should either have a constructor receiving a BrooklynProperties or it should have a no-arg constructor.
 */
public interface SecurityProvider {

    /**
     * Header for return to the user a helper message related to the unauthorized response
     */
    public static final String UNAUTHORIZED_MESSAGE_HEADER = "X_BROOKLYN_UNAUTHORIZED_MESSAGE";


    /** If user supplied a value session, this passes that in so the {@link SecurityProvider}
     * can check whether the user has previously authenticated, e.g. via an {@link HttpSession#setAttribute(String, Object)}
     * done by {@link #authenticate(HttpServletRequest, Supplier, String, String)}.
     * <p>
     * Note that this will be the {@link MultiSessionAttributeAdapter#getPreferredSession()}.
     * <p>
     * If the user didn't request a session or they requested a session which is not known here,
     * the argument will be null.
     */
    public boolean isAuthenticated(@Nullable HttpSession session);
    
    /** whether this provider requires a user/pass; if this returns false, the framework can
     * send null/null as the user/pass to {@link #authenticate(HttpServletRequest, Supplier, String, String)},
     * and should do that if user/pass info is not immediately available
     * (ie for things like oauth, the framework should not require basic auth if this method returns false)
     */
    public boolean requiresUserPass();
    
    /** Perform the authentication. If {@link #requiresUserPass()} returns false, user/pass may be null;
     * otherwise the framework will guarantee the basic auth is in effect and these values are set.
     * The provider should not send a response but should throw {@link SecurityProviderDeniedAuthentication}
     * if a custom response is required. It can include a response in that exception,
     * e.g. to provide more information or supply a redirect. 
     * <p>
     * It should not create a session via {@link HttpServletRequest#getSession()}, especially if
     * auth is not successful (easy for DOS attack to chew up memory), and even on auth it should use
     * the {@link Supplier} given here to get a session (that will create a session) to install.
     * (Note that this will return the {@link MultiSessionAttributeAdapter#getPreferredSession()},
     * not the request's local session.)
     * <p>
     * On successful auth this method may {@link HttpSession#setAttribute(String, Object)} so that
     * {@link #isAuthenticated(HttpSession)} can return quickly on subsequent requests.
     * If so, see {@link #logout(HttpSession)} about clearing those values. */
    public boolean authenticate(HttpServletRequest request, Supplier<HttpSession> sessionSupplierOnSuccess, String user, String pass) throws SecurityProviderDeniedAuthentication;
    
    /** Will get invoked on explicit REST API callback. 
     * The preferred session according to {@link MultiSessionAttributeAdapter} will be passed,
     * just as for other methods here.
     * <p>
     * Implementations here may remove any provider-specific attributes which cache authentication
     * (although the session will be invalidated so that may be overkill). */
    public boolean logout(HttpSession session);
    
    public static class SecurityProviderDeniedAuthentication extends Exception {

        private static final long serialVersionUID = -3048228939219746783L;
        private final Response response;
        public SecurityProviderDeniedAuthentication() { this(null); }
        public SecurityProviderDeniedAuthentication(Response r) {
            this.response = r;
        }
        public Response getResponse() {
            return response;
        }
    }

}
