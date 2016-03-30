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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

// http://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASLMDevGuide.html
public class BrooklynLoginModuleTest extends BrooklynMgmtUnitTestSupport {
    private static final String ACCEPTED_USER = "user";
    private static final String ACCEPTED_PASSWORD = "password";
    private static final String DEFAULT_ROLE = "webconsole";
    private CallbackHandler GOOD_CB_HANDLER = new TestCallbackHandler(
            ACCEPTED_USER,
            ACCEPTED_PASSWORD);
    private CallbackHandler BAD_CB_HANDLER = new TestCallbackHandler(
            ACCEPTED_USER + ".invalid",
            ACCEPTED_PASSWORD + ".invalid");

    private Subject subject;
    private Map<String, ?> sharedState;
    private Map<String, ?> options;

    private BrooklynLoginModule module;

    @Override
    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.addFrom(ImmutableMap.of(
                BrooklynWebConfig.USERS, ACCEPTED_USER,
                BrooklynWebConfig.PASSWORD_FOR_USER("user"), ACCEPTED_PASSWORD));
        mgmt = LocalManagementContextForTests.builder(true).useProperties(properties).build();
        ManagementContextHolder.setManagementContextStatic(mgmt);

        super.setUp();

        subject = new Subject();
        sharedState = MutableMap.of();
        options = ImmutableMap.of();

        module = new BrooklynLoginModule();
    }

    @Test
    public void testMissingCallback() throws LoginException {
        module.initialize(subject, null, sharedState, options);
        try {
            module.login();
            fail("Login is supposed to fail due to missing callback");
        } catch (FailedLoginException e) {
            // Expected, ignore
        }
        assertFalse(module.commit(), "commit");
        assertEmptyPrincipals();
        assertFalse(module.abort(), "abort");
    }

    @Test
    public void testFailedLoginCommitAbort() throws LoginException {
        badLogin();
        assertFalse(module.commit(), "commit");
        assertEmptyPrincipals();
        assertFalse(module.abort(), "abort");
    }

    @Test
    public void testFailedLoginCommitAbortReadOnly() throws LoginException {
        subject.setReadOnly();
        badLogin();
        assertFalse(module.commit(), "commit");
        assertEmptyPrincipals();
        assertFalse(module.abort(), "abort");
    }

    @Test
    public void testFailedLoginAbort() throws LoginException {
        badLogin();
        assertFalse(module.abort(), "abort");
        assertEmptyPrincipals();
    }

    @Test
    public void testSuccessfulLoginCommitLogout() throws LoginException {
        goodLogin();
        assertTrue(module.commit(), "commit");
        assertBrooklynPrincipal();
        assertTrue(module.logout(), "logout");
        assertEmptyPrincipals();
    }

    @Test
    public void testSuccessfulLoginCommitAbort() throws LoginException {
        goodLogin();
        assertTrue(module.commit(), "commit");
        assertBrooklynPrincipal();
        assertTrue(module.abort(), "logout");
        assertEmptyPrincipals();
    }

    @Test
    public void testSuccessfulLoginCommitAbortReadOnly() throws LoginException {
        subject.setReadOnly();
        goodLogin();
        try {
            module.commit();
            fail("Commit expected to throw");
        } catch (LoginException e) {
            // Expected
        }
        assertTrue(module.abort());
    }

    @Test
    public void testSuccessfulLoginAbort() throws LoginException {
        goodLogin();
        assertTrue(module.abort(), "abort");
        assertEmptyPrincipals();
    }
    
    @Test
    public void testCustomRole() throws LoginException {
        String role = "users";
        options = ImmutableMap.<String, Object>of(BrooklynLoginModule.PROPERTY_ROLE, role);
        goodLogin();
        assertTrue(module.commit(), "commit");
        assertBrooklynPrincipal(role);
    }

    private void goodLogin() throws LoginException {
        module.initialize(subject, GOOD_CB_HANDLER, sharedState, options);
        assertTrue(module.login(), "login");
        assertEmptyPrincipals();
    }

    private void badLogin() throws LoginException {
        module.initialize(subject, BAD_CB_HANDLER, sharedState, options);
        try {
            module.login();
            fail("Login is supposed to fail due to invalid username+password pair");
        } catch (FailedLoginException e) {
            // Expected, ignore
        }
    }

    private void assertBrooklynPrincipal() {
        assertBrooklynPrincipal(DEFAULT_ROLE);
    }
    private void assertBrooklynPrincipal(String role) {
        assertEquals(subject.getPrincipals(), ImmutableSet.of(
                new BrooklynLoginModule.UserPrincipal(ACCEPTED_USER),
                new BrooklynLoginModule.RolePrincipal(role)));
    }

    private void assertEmptyPrincipals() {
        assertEquals(subject.getPrincipals().size(), 0);
    }

}
