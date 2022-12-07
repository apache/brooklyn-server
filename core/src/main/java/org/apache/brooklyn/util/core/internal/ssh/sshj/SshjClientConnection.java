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
package org.apache.brooklyn.util.core.internal.ssh.sshj;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.*;
import java.util.List;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.keyprovider.OpenSSHKeyFile;
import net.schmizz.sshj.userauth.password.PasswordUtils;

import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.internal.ssh.SshAbstractTool.SshAction;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.crypto.params.AsymmetricKeyParameter;
import org.bouncycastle.crypto.util.OpenSSHPrivateKeyUtil;
import org.bouncycastle.crypto.util.PrivateKeyInfoFactory;
import org.bouncycastle.openssl.MiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemReader;
import org.bouncycastle.util.io.pem.PemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.net.HostAndPort;

/** based on code from jclouds */
public class SshjClientConnection implements SshAction<SSHClient> {

    private static final Logger LOG = LoggerFactory.getLogger(SshjClientConnection.class);

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        protected HostAndPort hostAndPort;
        protected String username;
        protected String password;
        protected String privateKeyPassphrase;
        protected String privateKeyData;
        protected File privateKeyFile;
        protected long connectTimeout;
        protected long sessionTimeout;
        protected boolean strictHostKeyChecking;

        public Builder hostAndPort(HostAndPort hostAndPort) {
            this.hostAndPort = hostAndPort;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String val) {
            this.password = val;
            return this;
        }

        /** @deprecated use privateKeyData */
        @Deprecated
        public Builder privateKey(String val) {
            this.privateKeyData = val;
            return this;
        }

        public Builder privateKeyPassphrase(String val) {
            this.privateKeyPassphrase = val;
            return this;
        }
        
        public Builder privateKeyData(String val) {
            this.privateKeyData = val;
            return this;
        }
        
        public Builder privateKeyFile(File val) {
            this.privateKeyFile = val;
            return this;
        }
        
        public Builder strictHostKeyChecking(boolean val) {
            this.strictHostKeyChecking = val;
            return this;
        }

        public Builder connectTimeout(long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder sessionTimeout(long sessionTimeout) {
            this.sessionTimeout = sessionTimeout;
            return this;
        }

        public SshjClientConnection build() {
            return new SshjClientConnection(this);
        }

        protected static Builder fromSSHClientConnection(SshjClientConnection in) {
            return new Builder().hostAndPort(in.getHostAndPort()).connectTimeout(in.getConnectTimeout()).sessionTimeout(
                    in.getSessionTimeout()).username(in.username).password(in.password).privateKey(in.privateKeyData).privateKeyFile(in.privateKeyFile);
        }
    }

    private final HostAndPort hostAndPort;
    private final String username;
    private final String password;
    private final String privateKeyPassphrase;
    private final String privateKeyData;
    private final File privateKeyFile;
    private final boolean strictHostKeyChecking;
    private final int connectTimeout;
    private final int sessionTimeout;
    
    SSHClient ssh;

    private SshjClientConnection(Builder builder) {
        this.hostAndPort = checkNotNull(builder.hostAndPort);
        this.username = builder.username;
        this.password = builder.password;
        this.privateKeyPassphrase = builder.privateKeyPassphrase;
        this.privateKeyData = builder.privateKeyData;
        this.privateKeyFile = builder.privateKeyFile;
        this.strictHostKeyChecking = builder.strictHostKeyChecking;
        this.connectTimeout = checkInt("connectTimeout", builder.connectTimeout, Integer.MAX_VALUE);
        this.sessionTimeout = checkInt("sessionTimeout", builder.sessionTimeout, Integer.MAX_VALUE);
    }

    static Integer checkInt(String context, long value, Integer ifTooLarge) {
        if (value > Integer.MAX_VALUE) {
            LOG.warn("Value '"+value+"' for "+context+" too large in SshjClientConnection; using "+value);
            return ifTooLarge;
        }
        return (int)value;
    }

    public boolean isConnected() {
        return ssh != null && ssh.isConnected();
    }

    public boolean isAuthenticated() {
        return ssh != null && ssh.isAuthenticated();
    }

    @Override
    public void clear() {
        if (ssh != null && ssh.isConnected()) {
            try {
                if (LOG.isTraceEnabled()) LOG.trace("Disconnecting SshjClientConnection {} ({})", this, System.identityHashCode(this));
                ssh.disconnect();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) LOG.debug("<< exception disconnecting from {}: {}", e, e.getMessage());
            }
        }
        ssh = null;
    }

    static String convertOpensshPrivateKeyToRsaPrivateKeyData(String opensshPrivateKeyNewFormatData) throws IOException {
        AsymmetricKeyParameter privateKeyParameters;
        try (StringReader r = new StringReader(opensshPrivateKeyNewFormatData)) {
            PemReader pemReader = new PemReader(r);
            byte[] privateKeyContent = pemReader.readPemObject().getContent();
            privateKeyParameters = OpenSSHPrivateKeyUtil.parsePrivateKeyBlob(privateKeyContent);
        }
        PrivateKeyInfo keyInfo = PrivateKeyInfoFactory.createPrivateKeyInfo(privateKeyParameters);
        StringWriter sw = new StringWriter();
        PemWriter pw = new PemWriter(sw);
        pw.writeObject(new MiscPEMGenerator(keyInfo).generate());
        pw.close();
        return sw.toString();
    }

    @Override
    public SSHClient create() throws Exception {
        if (LOG.isTraceEnabled()) LOG.trace("Connecting SshjClientConnection {} ({})", this, System.identityHashCode(this));
        ssh = new net.schmizz.sshj.SSHClient();
        if (!strictHostKeyChecking) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
        }
        if (connectTimeout != 0) {
            ssh.setConnectTimeout(connectTimeout);
        }
        if (sessionTimeout != 0) {
            ssh.setTimeout(sessionTimeout);
        }
        ssh.connect(hostAndPort.getHost(), hostAndPort.getPortOrDefault(22));
        
        if (password != null) {
            ssh.authPassword(username, password);
        } else {
            boolean auth = false;

            String actualPrivateKeyData = privateKeyData;


            List<Exception> errors = MutableList.of();

            if (Strings.isBlank(actualPrivateKeyData) && privateKeyFile!=null) {
                actualPrivateKeyData = Streams.readFullyStringAndClose(new FileInputStream(privateKeyFile));
            }

            try {
                if (Strings.isNonBlank(actualPrivateKeyData) && actualPrivateKeyData.trim().matches("(?s)-+ *BEGIN OPENSSH PRIVATE KEY.*")) {
                    // bouncy castle has low-level routines for OPENSSH PRIVATE KEY format
                    // but does not recognize it in the PEM file, so we handle it specially
                    // (but note passphrase is not supported)
                    if (Strings.isNonBlank(privateKeyPassphrase)) throw new IllegalArgumentException(
                            "Passphrase not supported with proprietary OPENSSH PRIVATE KEY format. " +
                                    "Use RSA or other standard. If using ssh-keygen this means passing `-m pem`.");

                    OpenSSHKeyFile key = new OpenSSHKeyFile();
                    key.init(convertOpensshPrivateKeyToRsaPrivateKeyData(actualPrivateKeyData), null,
                            null /* passphrase would have been used earlier, but isn't supported by BC */);
                    auth = true;
                    ssh.authPublickey(username, key);
                }
            } catch (Exception e) {
                errors.add(e);
            }

            if (!auth && privateKeyData != null) {
                try {
                    OpenSSHKeyFile key = new OpenSSHKeyFile();
                    key.init(privateKeyData, null,
                            JavaGroovyEquivalents.groovyTruth(privateKeyPassphrase) ?
                                    PasswordUtils.createOneOff(privateKeyPassphrase.toCharArray())
                                    : null);
                    auth = true;
                    ssh.authPublickey(username, key);
                } catch (Exception e) {
                    errors.add(e);
                }
            }

            if (!auth && privateKeyFile != null) {
                try {
                    OpenSSHKeyFile key = new OpenSSHKeyFile();
                    key.init(privateKeyFile,
                            JavaGroovyEquivalents.groovyTruth(privateKeyPassphrase) ?
                                    PasswordUtils.createOneOff(privateKeyPassphrase.toCharArray())
                                    : null);
                    auth = true;
                    ssh.authPublickey(username, key);
                } catch (Exception e) {
                    errors.add(e);
                }
            }

            if (!errors.isEmpty()) {
                if (errors.size()>1) LOG.warn("Invalid keys supplied, multiple errors. First will be rethrown. All errors are: "+errors);
                throw Exceptions.propagate("Unsupported key type", errors);
            }

            if (!auth) {
                // Accept defaults (in ~/.ssh)
                ssh.authPublickey(username);
            }
        }
        
        return ssh;
    }

    /**
     * @return host and port, where port if not present defaults to {@code 22}
     */
    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    /**
     * @return username used in this ssh
     */
    public String getUsername() {
        return username;
    }

    /**
     * 
     * @return how long to wait for the initial connection to be made
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * 
     * @return how long to keep the ssh open, or {@code 0} for indefinitely
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * 
     * @return the current ssh or {@code null} if not connected
     */
    public SSHClient getSSHClient() {
        return ssh;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SshjClientConnection that = SshjClientConnection.class.cast(o);
        return equal(this.hostAndPort, that.hostAndPort) && equal(this.username, that.username) 
                && equal(this.password, that.password) && equal(this.privateKeyData, that.privateKeyData)
                && equal(this.privateKeyFile, that.privateKeyFile) && equal(this.ssh, that.ssh);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(hostAndPort, username, password, privateKeyData, ssh);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("")
                .add("hostAndPort", hostAndPort)
                .add("user", username)
                .add("ssh", ssh != null ? ssh.hashCode() : null)
                .add("password", (password != null ? "xxxxxx" : null))
                .add("privateKeyFile", privateKeyFile)
                .add("privateKey", (privateKeyData != null ? "xxxxxx" : null))
                .add("connectTimeout", connectTimeout)
                .add("sessionTimeout", sessionTimeout).toString();
    }
}
