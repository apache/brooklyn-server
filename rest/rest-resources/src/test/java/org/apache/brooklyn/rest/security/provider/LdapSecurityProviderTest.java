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

import org.junit.rules.ExpectedException;
import org.testng.annotations.Test;

import javax.naming.NamingException;

import static org.testng.Assert.*;

public class LdapSecurityProviderTest {

    @Test
    public void testNoDomain() throws NamingException {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", "example.org", "Users");

        assertEquals(ldapSecurityProvider.getUserDN("Me"), "cn=Me,ou=Users,dc=example,dc=org");
    }

    @Test
    public void testAllowedDomainByRegexDirectMatch() throws NamingException {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", "example.org", "MyDomain", "Users");

        assertEquals(ldapSecurityProvider.getUserDN("MyDomain\\Me"), "cn=Me,ou=Users,dc=mydomain");
    }

    @Test
    public void testAllowedDomainByRegexListMatch() throws NamingException {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", "example.org", "MyDomain|OtherDomain", "Users");

        assertEquals(ldapSecurityProvider.getUserDN("MyDomain\\Me"), "cn=Me,ou=Users,dc=mydomain");
    }

    @Test
    public void testAllowedDomainByRegexWildcardMatch() throws NamingException {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", "example.org", ".*Domain", "Users");

        assertEquals(ldapSecurityProvider.getUserDN("MyDomain\\Me"), "cn=Me,ou=Users,dc=mydomain");
        assertEquals(ldapSecurityProvider.getUserDN("OtherDomain\\Me"), "cn=Me,ou=Users,dc=otherdomain");
    }

    @Test
    public void testDefaultDomainIsAlsoAnAllowedDomainInUserString() throws NamingException{
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", "example.org", "MyDomain", "Users");

        assertEquals(ldapSecurityProvider.getUserDN("example.org\\Me"), "cn=Me,ou=Users,dc=example,dc=org");
    }

    @Test(expectedExceptions=NamingException.class)
    public void testNotAllowedDomain() throws NamingException {
        LdapSecurityProvider ldapSecurityProvider = new LdapSecurityProvider("url", "example.org", "MyDomain", "Users");

        ldapSecurityProvider.getUserDN("OtherDomain\\Me");
    }

    // TODO Test only one of regex or default - do both - 2 tests

    // TODO Test no allowed regex

    // TODO Test neither - other constructor should fail
}