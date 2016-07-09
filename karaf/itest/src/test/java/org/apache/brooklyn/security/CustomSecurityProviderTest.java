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
package org.apache.brooklyn.security;

import static org.apache.brooklyn.KarafTestUtils.defaultOptionsWith;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.ops4j.pax.exam.CoreOptions.streamBundle;

import java.io.IOException;

import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.jaas.BrooklynLoginModule;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.IntegrationTest;
import org.apache.karaf.features.BootFinished;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.util.Filter;
import org.ops4j.pax.tinybundles.core.TinyBundles;
import org.osgi.framework.Constants;

import com.google.common.collect.ImmutableSet;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
@Category(IntegrationTest.class)
public class CustomSecurityProviderTest {
    private static final String WEBCONSOLE_REALM = "webconsole";

    /**
     * To make sure the tests run only when the boot features are fully
     * installed
     */
    @Inject
    @Filter(timeout = 120000)
    BootFinished bootFinished;
    
    @Inject
    @Filter(timeout = 120000)
    ManagementContext managementContext;

    @Configuration
    public static Option[] configuration() throws Exception {
        return defaultOptionsWith(
            streamBundle(TinyBundles.bundle()
                .add(CustomSecurityProvider.class)
                .add("OSGI-INF/blueprint/security.xml", CustomSecurityProviderTest.class.getResource("/custom-security-bp.xml"))
                .set(Constants.BUNDLE_MANIFESTVERSION, "2") // defaults to 1 which doesn't work
                .set(Constants.BUNDLE_SYMBOLICNAME, "org.apache.brooklyn.test.security")
                .set(Constants.BUNDLE_VERSION, "1.0.0")
                .set(Constants.DYNAMICIMPORT_PACKAGE, "*")
                .set(Constants.EXPORT_PACKAGE, CustomSecurityProvider.class.getPackage().getName())
                .build())
            // Uncomment this for remote debugging the tests on port 5005
            // ,KarafDistributionOption.debugConfiguration()
        );
    }

    @Before
    public void setUp() {
        // Works only before initializing the security provider (i.e. before first use)
        // TODO Dirty hack to inject the needed properties. Improve once managementContext is configurable.
        // Alternatively re-register a test managementContext service (how?)
        BrooklynProperties brooklynProperties = (BrooklynProperties)managementContext.getConfig();
        brooklynProperties.put(BrooklynWebConfig.SECURITY_PROVIDER_CLASSNAME.getName(), CustomSecurityProvider.class.getCanonicalName());
    }

    @Test(expected = FailedLoginException.class)
    public void checkLoginFails() throws LoginException {
        assertRealmRegisteredEventually(WEBCONSOLE_REALM);
        doLogin("invalid", "auth");
    }

    @Test
    public void checkLoginSucceeds() throws LoginException {
        assertRealmRegisteredEventually(WEBCONSOLE_REALM);
        String user = "custom";
        LoginContext lc = doLogin(user, "password");
        Subject subject = lc.getSubject();
        assertNotNull(subject);
        assertEquals(subject.getPrincipals(), ImmutableSet.of(
                new BrooklynLoginModule.UserPrincipal(user),
                new BrooklynLoginModule.RolePrincipal("users")));
    }

    private LoginContext doLogin(final String username, final String password) throws LoginException {
        assertRealmRegisteredEventually(WEBCONSOLE_REALM);
        LoginContext lc = new LoginContext(WEBCONSOLE_REALM, new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (int i = 0; i < callbacks.length; i++) {
                    Callback callback = callbacks[i];
                    if (callback instanceof PasswordCallback) {
                        PasswordCallback passwordCallback = (PasswordCallback)callback;
                        passwordCallback.setPassword(password.toCharArray());
                    } else if (callback instanceof NameCallback) {
                        NameCallback nameCallback = (NameCallback)callback;
                        nameCallback.setName(username);
                    }
                }
            }
        });
        lc.login();
        return lc;
    }

    private void assertRealmRegisteredEventually(final String userPassRealm) {
        // Need to wait a bit for the realm to get registered, any OSGi way to do this?
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                javax.security.auth.login.Configuration initialConfig = javax.security.auth.login.Configuration.getConfiguration();
                AppConfigurationEntry[] realm = initialConfig.getAppConfigurationEntry(userPassRealm);
                assertNotNull(realm);
            }
        });
    }

}
