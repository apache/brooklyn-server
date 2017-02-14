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

package org.apache.brooklyn.location.jclouds;

import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;

import java.security.KeyPair;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.crypto.SecureKeys;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.functions.Sha512Crypt;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.scriptbuilder.domain.LiteralStatement;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.statements.login.AdminAccess;
import org.jclouds.scriptbuilder.statements.login.ReplaceShadowPasswordEntry;
import org.jclouds.scriptbuilder.statements.ssh.AuthorizeRSAPublicKeys;
import org.jclouds.scriptbuilder.statements.ssh.SshStatements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class CreateUserStatements {

    private static final Logger LOG = LoggerFactory.getLogger(CreateUserStatements.class);

    private final LoginCredentials createdUserCredentials;
    private final List<Statement> statements;

    CreateUserStatements(LoginCredentials creds, List<Statement> statements) {
        this.createdUserCredentials = creds;
        this.statements = statements;
    }

    public LoginCredentials credentials() {
        return createdUserCredentials;
    }

    public List<Statement> statements() {
        return statements;
    }

    /**
     * Returns the commands required to create the user, to be used for connecting (e.g. over ssh)
     * to the machine; also returns the expected login credentials.
     * <p>
     * The returned login credentials may be null if we haven't done any user-setup and no specific
     * user was supplied (i.e. if {@code dontCreateUser} was true and {@code user} was null or blank).
     * In which case, the caller should use the jclouds node's login credentials.
     * <p>
     * There are quite a few configuration options. Depending on their values, the user-creation
     * behaves differently:
     * <ul>
     * <li>{@code dontCreateUser} says not to run any user-setup commands at all. If {@code user} is
     * non-empty (including with the default value), then that user will subsequently be used,
     * otherwise the (inferred) {@code loginUser} will be used.
     * <li>{@code loginUser} refers to the existing user that jclouds should use when setting up the VM.
     * Normally this will be inferred from the image (i.e. doesn't need to be explicitly set), but sometimes
     * the image gets it wrong so this can be a handy override.
     * <li>{@code user} is the username for brooklyn to subsequently use when ssh'ing to the machine.
     * If not explicitly set, its value will default to the username of the user running brooklyn.
     * <ul>
     * <li>If the {@code user} value is null or empty, then the (inferred) {@code loginUser} will
     * subsequently be used, setting up the password/authorizedKeys for that loginUser.
     * <li>If the {@code user} is "root", then setup the password/authorizedKeys for root.
     * <li>If the {@code user} equals the (inferred) {@code loginUser}, then don't try to create this
     * user but instead just setup the password/authorizedKeys for the user.
     * <li>Otherwise create the given user, setting up the password/authorizedKeys (unless
     * {@code dontCreateUser} is set, obviously).
     * </ul>
     * <li>{@code publicKeyData} is the key to authorize (i.e. add to .ssh/authorized_keys),
     * if not null or blank. Note the default is to use {@code ~/.ssh/id_rsa.pub} or {@code ~/.ssh/id_dsa.pub}
     * if either of those files exist for the user running brooklyn.
     * Related is {@code publicKeyFile}, which is used to populate publicKeyData.
     * <li>{@code password} is the password to set for the user. If null or blank, then a random password
     * will be auto-generated and set.
     * <li>{@code privateKeyData} is the key to use when subsequent ssh'ing, if not null or blank.
     * Note the default is to use {@code ~/.ssh/id_rsa} or {@code ~/.ssh/id_dsa}.
     * The subsequent preferences for ssh'ing are:
     * <ul>
     * <li>Use the {@code privateKeyData} if not null or blank (including if using default)
     * <li>Use the {@code password} (or the auto-generated password if that is blank).
     * </ul>
     * <li>{@code grantUserSudo} determines whether or not the created user may run the sudo command.</li>
     * </ul>
     *
     * @param image  The image being used to create the VM
     * @param config Configuration for creating the VM
     * @return The commands required to create the user, along with the expected login credentials for that user,
     * or null if we are just going to use those from jclouds.
     */
    public static CreateUserStatements get(JcloudsLocation location, @Nullable Image image, ConfigBag config) {
        //NB: private key is not installed remotely, just used to get/validate the public key
        Preconditions.checkNotNull(location, "location argument required");
        String user = Preconditions.checkNotNull(location.getUser(config), "user required");
        final boolean isWindows = location.isWindows(image, config);
        final String explicitLoginUser = config.get(JcloudsLocation.LOGIN_USER);
        final String loginUser = groovyTruth(explicitLoginUser)
                ? explicitLoginUser
                : (image != null && image.getDefaultCredentials() != null)
                        ? image.getDefaultCredentials().identity
                        : null;
        final boolean dontCreateUser = config.get(JcloudsLocation.DONT_CREATE_USER);
        final boolean grantUserSudo = config.get(JcloudsLocation.GRANT_USER_SUDO);
        final LocationConfigUtils.OsCredential credential = LocationConfigUtils.getOsCredential(config);
        credential.checkNoErrors().logAnyWarnings();
        final String passwordToSet =
                Strings.isNonBlank(credential.getPassword()) ? credential.getPassword() : Identifiers.makeRandomId(12);
        final List<Statement> statements = Lists.newArrayList();
        LoginCredentials createdUserCreds = null;

        if (dontCreateUser) {
            // dontCreateUser:
            // if caller has not specified a user, we'll just continue to use the loginUser;
            // if caller *has*, we set up our credentials assuming that user and credentials already exist

            if (Strings.isBlank(user)) {
                // createdUserCreds returned from this method will be null;
                // we will use the creds returned by jclouds on the node
                LOG.info("Not setting up user {} (subsequently using loginUser {})", user, loginUser);
                config.put(JcloudsLocation.USER, loginUser);

            } else {
                LOG.info("Not creating user {}, and not installing its password or authorizing keys (assuming it exists)", user);

                if (credential.isUsingPassword()) {
                    createdUserCreds = LoginCredentials.builder().user(user).password(credential.getPassword()).build();
                    if (Boolean.FALSE.equals(config.get(JcloudsLocation.DISABLE_ROOT_AND_PASSWORD_SSH))) {
                        statements.add(SshStatements.sshdConfig(ImmutableMap.of("PasswordAuthentication", "yes")));
                    }
                } else if (credential.hasKey()) {
                    createdUserCreds = LoginCredentials.builder().user(user).privateKey(credential.getPrivateKeyData()).build();
                }
            }

        } else if (isWindows) {
            // TODO Generate statements to create the user.
            // createdUserCreds returned from this method will be null;
            // we will use the creds returned by jclouds on the node
            LOG.warn("Not creating or configuring user on Windows VM, despite " + JcloudsLocation.DONT_CREATE_USER.getName() + " set to false");

            // TODO extractVmCredentials() will use user:publicKeyData defaults, if we don't override this.
            // For linux, how would we configure Brooklyn to use the node.getCredentials() - i.e. the version
            // that the cloud automatically generated?
            if (config.get(JcloudsLocation.USER) != null) config.put(JcloudsLocation.USER, "");
            if (config.get(JcloudsLocation.PASSWORD) != null) config.put(JcloudsLocation.PASSWORD, "");
            if (config.get(JcloudsLocation.PRIVATE_KEY_DATA) != null) config.put(JcloudsLocation.PRIVATE_KEY_DATA, "");
            if (config.get(JcloudsLocation.PRIVATE_KEY_FILE) != null) config.put(JcloudsLocation.PRIVATE_KEY_FILE, "");
            if (config.get(JcloudsLocation.PUBLIC_KEY_DATA) != null) config.put(JcloudsLocation.PUBLIC_KEY_DATA, "");
            if (config.get(JcloudsLocation.PUBLIC_KEY_FILE) != null) config.put(JcloudsLocation.PUBLIC_KEY_FILE, "");

        } else if (Strings.isBlank(user) || user.equals(loginUser) || user.equals(JcloudsLocation.ROOT_USERNAME)) {
            boolean useKey = Strings.isNonBlank(credential.getPublicKeyData());

            // For subsequent ssh'ing, we'll be using the loginUser
            if (Strings.isBlank(user)) {
                user = loginUser;
                config.put(JcloudsLocation.USER, user);
            }

            // Using the pre-existing loginUser; setup the publicKey/password so can login as expected

            // *Always* change the password (unless dontCreateUser was specified)
            statements.add(new ReplaceShadowPasswordEntry(Sha512Crypt.function(), user, passwordToSet));
            createdUserCreds = LoginCredentials.builder().user(user).password(passwordToSet).build();

            if (useKey) {
                // NB: further keys are added from config *after* user creation
                statements.add(new AuthorizeRSAPublicKeys("~" + user + "/.ssh", ImmutableList.of(credential.getPublicKeyData()), null));
                if (Strings.isNonBlank(credential.getPrivateKeyData())) {
                    createdUserCreds = LoginCredentials.builder().user(user).privateKey(credential.getPrivateKeyData()).build();
                }
            }

            if (!useKey || Boolean.FALSE.equals(config.get(JcloudsLocation.DISABLE_ROOT_AND_PASSWORD_SSH))) {
                // ensure password is permitted for ssh
                statements.add(SshStatements.sshdConfig(ImmutableMap.of("PasswordAuthentication", "yes")));
                if (user.equals(JcloudsLocation.ROOT_USERNAME)) {
                    statements.add(SshStatements.sshdConfig(ImmutableMap.of("PermitRootLogin", "yes")));
                }
            }

        } else {
            String pubKey = credential.getPublicKeyData();
            String privKey = credential.getPrivateKeyData();

            if (credential.isEmpty()) {
                /*
                 * TODO have an explicit `create_new_key_per_machine` config key.
                 * error if privateKeyData is set in this case.
                 * publicKeyData automatically added to EXTRA_SSH_KEY_URLS_TO_AUTH.
                 *
                 * if this config key is not set, use a key `brooklyn_id_rsa` and `.pub` in `MGMT_BASE_DIR`,
                 * with permission 0600, creating it if necessary, and logging the fact that this was created.
                 */
                // TODO JcloudsLocation used to log this once only: loggedSshKeysHint.compareAndSet(false, true).
                if (!config.containsKey(JcloudsLocation.PRIVATE_KEY_FILE)) {
                    LOG.info("Default SSH keys not found or not usable; will create new keys for each machine. " +
                                    "Create ~/.ssh/id_rsa or set {} / {} / {} as appropriate for this location " +
                                    "if you wish to be able to log in without Brooklyn.",
                            new Object[]{JcloudsLocation.PRIVATE_KEY_FILE.getName(), JcloudsLocation.PRIVATE_KEY_PASSPHRASE.getName(), JcloudsLocation.PASSWORD.getName()});
                }
                KeyPair newKeyPair = SecureKeys.newKeyPair();
                pubKey = SecureKeys.toPub(newKeyPair);
                privKey = SecureKeys.toPem(newKeyPair);
                LOG.debug("Brooklyn key being created for " + user + " at new machine " + location + " is:\n" + privKey);
            }

            // Create the user
            // note AdminAccess requires _all_ fields set, due to http://code.google.com/p/jclouds/issues/detail?id=1095
            AdminAccess.Builder adminBuilder = AdminAccess.builder()
                    .adminUsername(user)
                    .grantSudoToAdminUser(grantUserSudo);
            adminBuilder.cryptFunction(Sha512Crypt.function());

            boolean useKey = Strings.isNonBlank(pubKey);
            adminBuilder.cryptFunction(Sha512Crypt.function());

            // always set this password; if not supplied, it will be a random string
            adminBuilder.adminPassword(passwordToSet);
            // log the password also, in case we need it
            LOG.debug("Password '{}' being created for user '{}' at the machine we are about to provision in {}; {}",
                    new Object[]{passwordToSet, user, location, useKey ? "however a key will be used to access it" : "this will be the only way to log in"});

            if (grantUserSudo && config.get(JcloudsLocationConfig.DISABLE_ROOT_AND_PASSWORD_SSH)) {
                // the default - set root password which we forget, because we have sudo acct
                // (and lock out root and passwords from ssh)
                adminBuilder.resetLoginPassword(true);
                adminBuilder.loginPassword(Identifiers.makeRandomId(12));
            } else {
                adminBuilder.resetLoginPassword(false);
                adminBuilder.loginPassword(Identifiers.makeRandomId(12) + "-ignored");
            }

            if (useKey) {
                adminBuilder.authorizeAdminPublicKey(true).adminPublicKey(pubKey);
            } else {
                adminBuilder.authorizeAdminPublicKey(false).adminPublicKey(Identifiers.makeRandomId(12) + "-ignored");
            }

            // jclouds wants us to give it the private key, otherwise it might refuse to authorize the public key
            // (in AdminAccess.build, if adminUsername != null && adminPassword != null);
            // we don't want to give it the private key, but we *do* want the public key authorized;
            // this code seems to trigger that.
            // (we build the creds below)
            adminBuilder.installAdminPrivateKey(false).adminPrivateKey(Identifiers.makeRandomId(12) + "-ignored");

            // lock SSH means no root login and no passwordless login
            // if we're using a password or we don't have sudo, then don't do this!
            adminBuilder.lockSsh(useKey && grantUserSudo && config.get(JcloudsLocationConfig.DISABLE_ROOT_AND_PASSWORD_SSH));

            statements.add(adminBuilder.build());

            if (useKey) {
                createdUserCreds = LoginCredentials.builder().user(user).privateKey(privKey).build();
            } else if (passwordToSet != null) {
                createdUserCreds = LoginCredentials.builder().user(user).password(passwordToSet).build();
            }

            if (!useKey || Boolean.FALSE.equals(config.get(JcloudsLocation.DISABLE_ROOT_AND_PASSWORD_SSH))) {
                // ensure password is permitted for ssh
                statements.add(SshStatements.sshdConfig(ImmutableMap.of("PasswordAuthentication", "yes")));
            }
        }

        String customTemplateOptionsScript = config.get(JcloudsLocation.CUSTOM_TEMPLATE_OPTIONS_SCRIPT_CONTENTS);
        if (Strings.isNonBlank(customTemplateOptionsScript)) {
            statements.add(new LiteralStatement(customTemplateOptionsScript));
        }

        LOG.debug("Machine we are about to create in {} will be customized with: {}", location, statements);

        return new CreateUserStatements(createdUserCreds, statements);
    }

}
