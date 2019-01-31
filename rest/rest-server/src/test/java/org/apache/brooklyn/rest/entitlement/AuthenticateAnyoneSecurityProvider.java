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
package org.apache.brooklyn.rest.entitlement;

import java.util.function.Supplier;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.brooklyn.rest.security.provider.AnyoneSecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider;

/** allows anyone to access, but does require a non-null user (any password) to be supplied via Basic auth,
 * in contrast to {@link AnyoneSecurityProvider} */
public class AuthenticateAnyoneSecurityProvider implements SecurityProvider {

    @Override
    public boolean isAuthenticated(HttpSession session) {
        return false;
    }

    @Override
    public boolean authenticate(HttpServletRequest request, Supplier<HttpSession> sessionSupplierOnSuccess, String user, String pass) throws SecurityProviderDeniedAuthentication {
        return user != null;
    }

    @Override
    public boolean logout(HttpSession session) {
        return false;
    }

    @Override
    public boolean requiresUserPass() {
        return true;
    }
}
