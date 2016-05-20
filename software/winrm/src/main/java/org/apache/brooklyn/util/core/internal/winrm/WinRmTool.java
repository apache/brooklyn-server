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
package org.apache.brooklyn.util.core.internal.winrm;

import static org.apache.brooklyn.core.config.ConfigKeys.newConfigKey;
import static org.apache.brooklyn.core.config.ConfigKeys.newIntegerConfigKey;
import static org.apache.brooklyn.core.config.ConfigKeys.newStringConfigKey;

import java.io.InputStream;
import java.util.List;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

@Beta
public interface WinRmTool {

    /** Public-facing global config keys for Brooklyn are defined in ConfigKeys, 
     * and have this prefix prepended to the config keys in this class. 
     * These keys are detected from entity/global config and automatically applied to ssh executions. */
    String BROOKLYN_CONFIG_KEY_PREFIX = Preconditions.checkNotNull(BrooklynConfigKeys.BROOKLYN_WINRM_CONFIG_KEY_PREFIX, 
            "static final initializer classload ordering problem");
    
    ConfigKey<String> PROP_HOST = newStringConfigKey("host", "Host to connect to (required)", null);
    ConfigKey<Integer> PROP_PORT = ConfigKeys.newIntegerConfigKey("port", "WinRM port to use when connecting to the remote machine");
    ConfigKey<Boolean> USE_HTTPS_WINRM = ConfigKeys.newBooleanConfigKey("winrm.useHttps", "The parameter tells the machine sensors whether the winrm port is over https. If the parameter is true then 5986 will be used as a winrm port.", false);
    ConfigKey<Integer> RETRIES_OF_NETWORK_FAILURES = ConfigKeys.newIntegerConfigKey("retriesOfNetworkFailures", "The parameter sets the number of retries for connection failures. If you use high value, consider taking care for the machine's network.", 4);
    /**
     * Flag which tells winrm whether to use Basic Authentication
     * or Negotiate plus NTLM.
     * winrm.useNtlm parameter could be a subject to change.
     * TODO Winrm supports several authentication mechanisms so it would be better to replace it with a prioritised list of authentication mechanisms to try.
     */
    @Beta
    ConfigKey<Boolean> USE_NTLM = ConfigKeys.newBooleanConfigKey("winrm.useNtlm", "The parameter configures tells the machine sensors whether the winrm port is over https. If the parameter is true then 5986 will be used as a winrm port.", true);

    @Beta
    ConfigKey<String> COMPUTER_NAME = ConfigKeys.newStringConfigKey("winrm.computerName", "Windows Computer Name to use for authentication.");
    ConfigKey<String> PROP_USER = newStringConfigKey("user", "User to connect as", null);
    ConfigKey<String> PROP_PASSWORD = newStringConfigKey("password", "Password to use to connect", null);
    ConfigKey<String> OPERATION_TIMEOUT = newStringConfigKey("winrm.operationTimeout", "WinRM OperationTimeout. If no output is available before the wsman:OperationTimeout expires, " +
            "the server MUST return a WSManFault with the Code attribute equal to \"2150858793\". When the client receives this fault, it will issue another Receive request. winrm4j also sets the tcp socket timeout to a rounded up value", "1m");

    // TODO See SshTool#PROP_SSH_TRIES, where it was called "sshTries"; remove duplication? Merge into one well-named thing?
    ConfigKey<Integer> PROP_EXEC_TRIES = ConfigKeys.newIntegerConfigKey(
            "execTries", 
            "Max number of times to attempt WinRM operations", 
            1);

    ConfigKey<Duration> PROP_EXEC_RETRY_DELAY = newConfigKey(
            Duration.class,
            "execRetryDelay",
            "Max time between retries (backing off exponentially to this delay)",
            Duration.TEN_SECONDS);

    // May be ignored by implementations that more efficiently copy the file.
    @Beta
    ConfigKey<Integer> COPY_FILE_CHUNK_SIZE_BYTES = ConfigKeys.newIntegerConfigKey(
            "windows.copy.file.size.bytes",
            "Size of file chunks (in bytes) to be used when copying a file to the remote server", 
            1024);

    /**
     * @deprecated since 0.9.0; use {@link #executeCommand(List)} to avoid ambiguity between native command and power shell.
     */
    @Deprecated
    WinRmToolResponse executeScript(List<String> commands);

    /**
     * @since 0.9.0
     */
    WinRmToolResponse executeCommand(List<String> commands);

    WinRmToolResponse executePs(List<String> commands);
    
    WinRmToolResponse copyToServer(InputStream source, String destination);
}
