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

import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.servlet.http.HttpSession;

import java.util.function.Supplier;

/**
 * Some helper tests for use in development
 */
public class LdapSecurityProviderLiveTest {

    /**
     * Use this for testing against an local openldap or Apache Directory Studio test server
     * @throws SecurityProvider.SecurityProviderDeniedAuthentication
     */
    @Test(groups = {"Live"})
    public void testLiveLdapServerADStudio() throws SecurityProvider.SecurityProviderDeniedAuthentication {
        boolean authenticated = isAuthenticated("ldap://localhost:10389/", "example.com", "Users", "username", "password");

        Assert.assertTrue(authenticated);
    }

    /**
     * Use these tests for testing against an active directory providing ldap
     *
     * Modify the following constants to work with your DC
     *
     */

    public static final String LDAP_URL = "ldap://IP_ADDRESSS:389/";
    public static final String LDAP_REALM = "dc2.example.org";
    public static final String ORGANIZATION_UNIT = "MyUsers";
    public static final String PASSWORD = "password";

    @Test(groups = {"Live"})
    public void testLiveLdapServerAD() throws SecurityProvider.SecurityProviderDeniedAuthentication {
        boolean authenticated = isAuthenticated(LDAP_URL, LDAP_REALM, ORGANIZATION_UNIT, "My Common Name", PASSWORD);

        Assert.assertTrue(authenticated);
    }

    @Test(groups = {"Live"})
    public void testLiveLdapServerADOld() throws SecurityProvider.SecurityProviderDeniedAuthentication {
        boolean authenticated = isAuthenticated(LDAP_URL, LDAP_REALM, ORGANIZATION_UNIT, "DOMAIN\\MyUser", PASSWORD);

        Assert.assertTrue(authenticated);
    }

    @Test(groups = {"Live"})
    public void testLiveLdapServerUserAtFQDN() throws SecurityProvider.SecurityProviderDeniedAuthentication {
        boolean authenticated = isAuthenticated(LDAP_URL, LDAP_REALM, ORGANIZATION_UNIT, "MyUser@" + LDAP_REALM, PASSWORD);

        Assert.assertTrue(authenticated);
    }

    private boolean isAuthenticated(String ldapUrl, String ldapRealm, String organizationUnit, String s, String s2) throws SecurityProvider.SecurityProviderDeniedAuthentication {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider(ldapUrl, ldapRealm, organizationUnit);

        return ldapSecurityProvider.authenticate(null, new Supplier<HttpSession>() {
            @Override
            public HttpSession get() {
                return Mockito.mock(HttpSession.class);
            }
        }, s, s2);
    }
}
