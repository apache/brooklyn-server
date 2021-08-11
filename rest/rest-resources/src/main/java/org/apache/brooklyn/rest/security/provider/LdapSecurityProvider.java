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

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.StringConfigMap;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext.USER_GROUPS;

/**
 * A {@link SecurityProvider} implementation that relies on LDAP to authenticate.
 *
 * CONFIG EXAMPLE
 * brooklyn.webconsole.security.ldap.url=ldap://<server>:<port>/
 * brooklyn.webconsole.security.ldap.realm=<,>realm>
 * brooklyn.webconsole.security.ldap.ou=<ou>.<parent_ou>
 * brooklyn.webconsole.security.ldap.fetch_user_group=true
 * brooklyn.webconsole.security.ldap.group_config_key=<role_resolver_config_key>
 * brooklyn.webconsole.security.ldap.login_info_log=true
 *
 * @author Peter Veentjer.
 */
public class LdapSecurityProvider extends AbstractSecurityProvider implements SecurityProvider {

    public static final Logger LOG = LoggerFactory.getLogger(LdapSecurityProvider.class);

    public static final String LDAP_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
    private final String ldapUrl;
    private final String defaultLdapRealm;
    private final String organizationUnit;
    private boolean logUserLoginAttempt;
    private boolean fetchUserGroups = false;
    private List<String> validGroups;

    public LdapSecurityProvider(ManagementContext mgmt) {
        StringConfigMap properties = mgmt.getConfig();
        ldapUrl = properties.getConfig(BrooklynWebConfig.LDAP_URL);
        Strings.checkNonEmpty(ldapUrl, "LDAP security provider configuration missing required property " + BrooklynWebConfig.LDAP_URL);
        fetchUserGroups = properties.getConfig(BrooklynWebConfig.LDAP_FETCH_USER_GROUPS);
        logUserLoginAttempt = properties.getConfig(BrooklynWebConfig.LDAP_LOGIN_INFO_LOG);
        String prefix = properties.getConfig(BrooklynWebConfig.GROUP_CONFIG_KEY_NAME);
        if (fetchUserGroups && Strings.isNonBlank(prefix)) {
            validGroups = getConfiguredGroups(properties, prefix);
        } else {
            validGroups = ImmutableList.of();
        }

        String realmConfig = properties.getConfig(BrooklynWebConfig.LDAP_REALM);
        if (Strings.isNonBlank(realmConfig)) {
            defaultLdapRealm = CharMatcher.isNot('"').retainFrom(realmConfig);
        } else {
            defaultLdapRealm = "";
        }

        if (Strings.isBlank(properties.getConfig(BrooklynWebConfig.LDAP_OU))) {
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public boolean authenticate(HttpServletRequest request, Supplier<HttpSession> sessionSupplierOnSuccess, String user, String pass) throws SecurityProviderDeniedAuthentication {
        if (user == null) return false;
        checkCanLoad();

        if (Strings.isBlank(pass)) {
            // InitialDirContext doesn't do authentication if no password is supplied!
            return false;
        }
        addToInfoLog("Login attempt with " + user);
        try {
            Hashtable env = new Hashtable();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, ldapUrl);
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, getSecurityPrincipal(user));
            env.put(Context.SECURITY_CREDENTIALS, pass);

            DirContext ctx = new InitialDirContext(env);// will throw if password is invalid
            if (fetchUserGroups) {
                List<String> userGroups = getUserGroups(user, ctx);
                if (userGroups.isEmpty()) {
                    addToInfoLog("Unsuccessful for " + user);
                    LOG.trace("User {} is not member of any group", user);
                    return false;
                }
                // adds user groups to the session
                sessionSupplierOnSuccess.get().setAttribute(USER_GROUPS, userGroups);
                addToInfoLog("Authentication successful for user " + user + ", in relevant LDAP groups "+userGroups);
            } else {
                addToInfoLog("Successful for " + user);
            }
            return allow(sessionSupplierOnSuccess.get(), user);
        } catch (NamingException e) {
            addToInfoLog("Unsuccessful for " + user);
            return false;
        }
    }

    private List<String> getConfiguredGroups(StringConfigMap properties, String prefix) {
        ImmutableList.Builder<String> configuredGroupsBuilder = ImmutableList.builder();
        StringConfigMap roles = properties.submap(ConfigPredicates.nameStartsWith(prefix + "."));
        for (Map.Entry<ConfigKey<?>, ?> entry : roles.getAllConfigLocalRaw().entrySet()) {
            configuredGroupsBuilder.add(Strings.removeFromStart(entry.getKey().getName(), prefix + "."));
        }
        return configuredGroupsBuilder.build();
    }

    private void addToInfoLog(String s) {
        if (logUserLoginAttempt) {
            LOG.info(s);
        }
    }

    private List<String> getUserGroups(String user, DirContext ctx) throws NamingException {
        ImmutableList.Builder<String> groupsListBuilder = ImmutableList.builder();

        SearchControls ctls = new SearchControls();
        ctls.setReturningAttributes(new String[]{"memberOf"});

        NamingEnumeration<?> answer = ctx.search(buildUserContainer(), "(&(objectclass=user)(sAMAccountName=" + getAccountName(user) + "))", ctls);

        while (answer.hasMore()) {
            SearchResult rslt = (SearchResult) answer.next();
            Attributes attrs = rslt.getAttributes();

            Attribute memberOf = attrs.get("memberOf");
            if (memberOf != null) {
                NamingEnumeration<?> groups = memberOf.getAll();
                while (groups.hasMore()) {
                    groupsListBuilder.add(getGroupName(groups.next().toString()));
                }
            }
        }
        ImmutableList<String> ldapGroups = groupsListBuilder.build();
        if (LOG.isTraceEnabled()) {
            LOG.trace("LDAP groups for {}: {}", user, ldapGroups);
        }
        // only store return the LDAP groups with a matching role in the configuration file
        return ldapGroups.stream().filter(ldapGroup -> validGroups.contains(ldapGroup)).collect(Collectors.toList());
    }

    private String buildUserContainer() {
        StringBuilder userContainerBuilder = new StringBuilder();
        for (String s : organizationUnit.split("\\.")) {
            userContainerBuilder.append("OU=").append(s).append(",");
        }
        for (String s : defaultLdapRealm.split("\\.")) {
            userContainerBuilder.append("DC=").append(s).append(",");
        }
        return StringUtils.chop(userContainerBuilder.toString());
    }

    private String getAccountName(String user) {
        if (user.contains("\\")) {
            String[] split = user.split("\\\\");
            return split[split.length - 1];
        }
        return user;
    }

    /**
     * Returns the name of deepest name of the group ignoring upper levels of nesting
     *
     * @param groupAttribute
     * @return
     */
    private String getGroupName(String groupAttribute) {
        // CN=groupName,OU=OuGroups,OU=Users,DC=dc,DC=example,DC=com
        Pattern groupNamePatter = Pattern.compile("^CN=(?<groupName>[a-zA-Z0-9_-]+),*");
        Matcher m = groupNamePatter.matcher(groupAttribute);
        if (m.find()) {
            return m.group(1);
        }
        throw new IllegalStateException("Not valid group found in " + groupAttribute);
    }

    /**
     * Returns the LDAP path for the user
     *
     * @param user
     * @return String
     */
    protected String getSecurityPrincipal(String user) throws NamingException {
        if (user.contains("@") || user.contains("\\")) {
            return user;
        }

        List<String> domain = Lists.transform(Arrays.asList(defaultLdapRealm.split("\\.")), new Function<String, String>() {
            @Override
            public String apply(String input) {
                return "dc=" + input;
            }
        });

        String dc = Joiner.on(",").join(domain).toLowerCase();
        return "cn=" + user + ",ou=" + organizationUnit + "," + dc;
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
