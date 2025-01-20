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

import org.testng.annotations.Test;

import javax.naming.NamingException;

import static org.testng.Assert.assertEquals;

public class LdapSecurityProviderTest {

    @Test
    public void testNoRealm() throws NamingException {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", "example.org", "Users");
        assertEquals(ldapSecurityProvider.getSecurityPrincipal("Me"), "CN=Me,OU=Users,DC=example,DC=org");

        // for legacy compatibility aaa,OU=bbb is is supported; but OU=aaa,OU=bbb or OU=aaa.bbb is preferred
        ldapSecurityProvider = new LdapSecurityProvider("url", "DC=example.org,DC=DOMAIN", "Users.Parent,OU=OtherParent");
        assertEquals(ldapSecurityProvider.getSecurityPrincipal("Me"), "CN=Me,OU=Users,OU=Parent,OU=OtherParent,DC=example,DC=org,DC=DOMAIN");
    }

    @Test
    public void testDomain() throws NamingException {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", null, "Users");
        assertEquals(ldapSecurityProvider.getSecurityPrincipal("MyDomain\\Me"), "MyDomain\\Me");
    }

    @Test
    public void testAllowedDomainByRegexListMatch() throws NamingException {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", null, "Users");
        assertEquals(ldapSecurityProvider.getSecurityPrincipal("username@example.org"), "username@example.org");
    }

}