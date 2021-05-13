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

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.function.Supplier;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.util.MultiSessionAttributeAdapter;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import sun.util.resources.cldr.dav.LocaleNames_dav;

/**
 * A {@link SecurityProvider} implementation that relies on LDAP to authenticate.
 *
 * @author Peter Veentjer.
 */
public class LdapSecurityProvider extends AbstractSecurityProvider implements SecurityProvider {

    public static final Logger LOG = LoggerFactory.getLogger(LdapSecurityProvider.class);

    public final static ConfigKey<String> LDAP_REALM_REGEX = ConfigKeys.newStringConfigKey(
            BrooklynWebConfig.BASE_NAME_SECURITY+".ldap.allowed_realms_regex",
            "Regular expression for allowed realms / domains" , "");

    public static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";

    private final String ldapUrl;
    private final String defaultLdapRealm;
    private final String organizationUnit;
    private String allowedRealmsRegex;

    public LdapSecurityProvider(ManagementContext mgmt) {
        StringConfigMap properties = mgmt.getConfig();
        ldapUrl = properties.getConfig(BrooklynWebConfig.LDAP_URL);
        Strings.checkNonEmpty(ldapUrl, "LDAP security provider configuration missing required property "+BrooklynWebConfig.LDAP_URL);

        String realmConfig = properties.getConfig(BrooklynWebConfig.LDAP_REALM);
        if (Strings.isNonBlank(realmConfig)) {
            defaultLdapRealm = CharMatcher.isNot('"').retainFrom(realmConfig);
        } else {
            defaultLdapRealm = "";
        }
        allowedRealmsRegex = properties.getConfig(LDAP_REALM_REGEX);
        if(Strings.isEmpty(defaultLdapRealm) && Strings.isEmpty(allowedRealmsRegex)) {
            throw new IllegalArgumentException("LDAP security provider configuration missing at least one of " +
                    "required properties "+BrooklynWebConfig.LDAP_REALM + " or " + LDAP_REALM_REGEX);
        }

        if(Strings.isBlank(properties.getConfig(BrooklynWebConfig.LDAP_OU))) {
            LOG.info("Setting LDAP ou attribute to: Users");
            organizationUnit = "Users";
        } else {
            organizationUnit = CharMatcher.isNot('"').retainFrom(properties.getConfig(BrooklynWebConfig.LDAP_OU));
        }
    }

    public LdapSecurityProvider(String ldapUrl, String ldapRealm, String organizationUnit) {
        this.ldapUrl = ldapUrl;
        this.defaultLdapRealm = ldapRealm;
        this.organizationUnit = organizationUnit;
    }

    public LdapSecurityProvider(String ldapUrl, String ldapRealm, String allowedRealmsRegex, String organizationUnit) {
        this(ldapUrl, ldapRealm, organizationUnit);
        this.allowedRealmsRegex = allowedRealmsRegex;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public boolean authenticate(HttpServletRequest request, Supplier<HttpSession> sessionSupplierOnSuccess, String user, String pass) throws SecurityProviderDeniedAuthentication {
        if (user==null) return false;
        checkCanLoad();

        if (Strings.isBlank(pass)) {
            // InitialDirContext doesn't do authentication if no password is supplied!
            return false;
        }

        try {
            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, ldapUrl);
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, getUserDN(user));
            env.put(Context.SECURITY_CREDENTIALS, pass);

            new InitialDirContext(env);  // will throw if password is invalid
            return allow(sessionSupplierOnSuccess.get(), user);
        } catch (NamingException e) {
            return false;
        }
    }

    /**
     * Returns the LDAP path for the user
     *
     * @param user
     * @return String
     */
    protected String getUserDN(String user) throws NamingException {
        String[] split = user.split("\\\\");
        String realm = defaultLdapRealm;
        String username = user;
        if (split.length==2) {
            realm = split[0];
            username = split[1];
            assertAllowedDomain(realm);
        }
        List<String> domain = Lists.transform(Arrays.asList(realm.split("\\.")), new Function<String, String>() {
            @Override
            public String apply(String input) {
                return "dc=" + input;
            }
        });

        String dc = Joiner.on(",").join(domain).toLowerCase();
        return "cn=" + username + ",ou=" + organizationUnit + "," + dc;
    }

    private void assertAllowedDomain(String realm) throws NamingException {
        if (defaultLdapRealm.equals(realm)) {
            return;
        }
        if (Strings.isNonBlank(allowedRealmsRegex) && realm.matches(allowedRealmsRegex)) {
            return;
        }
        throw new NamingException("The domain " + realm + " has not been configured as a brooklyn allowed domain using " +
                "the config key " + LDAP_REALM_REGEX);
    }

    static boolean triedLoading = false;
    public synchronized static void checkCanLoad() {
        if (triedLoading) return;
        try {
            Class.forName(LDAP_CONTEXT_FACTORY);
            triedLoading = true;
        } catch (Throwable e) {
            throw Exceptions.propagate(new ClassNotFoundException("Unable to load LDAP classes ("+LDAP_CONTEXT_FACTORY+") required for Brooklyn LDAP security provider"));
        }
    }
    
    @Override
    public boolean requiresUserPass() {
        return true;
    }
}
