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

import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response;

/**
 * The SecurityProvider is responsible for doing authentication.
 *
 * A class should either have a constructor receiving a BrooklynProperties or it should have a no-arg constructor.
 */
public interface SecurityProvider {

    public boolean isAuthenticated(HttpSession session);
    /** whether this provider requires a user/pass; if this returns false, the framework can
     * send null/null as the user/pass to {@link #authenticate(HttpSession, String, String)},
     * and should do that if user/pass info is not immediately available
     * (ie for things like oauth, the framework should not require basic auth if this method returns false)
     */
    public boolean requiresUserPass();
    /** Perform the authentication. If {@link #requiresUserPass()} returns false, user/pass may be null;
     * otherwise the framework will guarantee the basic auth is in effect and these values are set.
     * The provider should not send a response but should throw {@link SecurityProviderDeniedAuthentication}
     * if a custom response is required. It can include a response in that exception,
     * e.g. to provide more information or supply a redirect. */
    public boolean authenticate(HttpSession session, String user, String pass) throws SecurityProviderDeniedAuthentication;
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
