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
package org.apache.brooklyn.location.winrm;

import static org.apache.brooklyn.core.config.ConfigKeys.newConfigKeyWithPrefix;
import static org.apache.brooklyn.core.config.ConfigKeys.newStringConfigKey;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigUtils;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.AbstractMachineLocation;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.mgmt.ManagementContextInjectable;
import org.apache.brooklyn.location.ssh.CanResolveOnBoxDir;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.winrm.WinRmTool;
import org.apache.brooklyn.util.core.internal.winrm.WinRmToolResponse;
import org.apache.brooklyn.util.core.internal.winrm.winrm4j.Winrm4jTool;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.TypeToken;

public class WinRmMachineLocation extends AbstractMachineLocation implements MachineLocation, CanResolveOnBoxDir {

    private static final Logger LOG = LoggerFactory.getLogger(WinRmMachineLocation.class);

    public static final ConfigKey<InetAddress> ADDRESS = ConfigKeys.newConfigKey(
            InetAddress.class,
            "address",
            "Address of the remote machine");

    public static final ConfigKey<Integer> WINRM_CONFIG_PORT = newConfigKeyWithPrefix(BrooklynConfigKeys.BROOKLYN_WINRM_CONFIG_KEY_PREFIX, WinRmTool.PROP_PORT);
    public static final ConfigKey<String> OPERATION_TIMEOUT = newConfigKeyWithPrefix(BrooklynConfigKeys.BROOKLYN_WINRM_CONFIG_KEY_PREFIX, WinRmTool.OPERATION_TIMEOUT);
    public static final ConfigKey<Integer> RETRIES_OF_NETWORK_FAILURES = newConfigKeyWithPrefix(BrooklynConfigKeys.BROOKLYN_WINRM_CONFIG_KEY_PREFIX, WinRmTool.RETRIES_OF_NETWORK_FAILURES);
    public static final ConfigKey<Boolean> USE_HTTPS_WINRM = WinRmTool.USE_HTTPS_WINRM;
    public static final ConfigKey<String> WAIT_WINDOWS_TO_START = newStringConfigKey("waitWindowsToStart",
            "By default Brooklyn will return the machine immediately after Brooklyn is able to WinRM. Sometimes restart could happen after a Windows VM is provisioned. This could be because of System Upgrade or other." +
            " By setting this config key to 60s, 5m or other X Duration of time Brooklyn will wait X amount of time for disconnect to occur. If connection failure occurs it will wait X amount of time for the machine to come up.", null);


    /**
     * Flag which tells winrm whether to use Basic Authentication
     * or Negotiate plus NTLM.
     * winrm.useNtlm parameter could be a subject to change.
     * TODO Winrm supports several authentication mechanisms so it would be better to replace it with a prioritised list of authentication mechanisms to try.
     */
    @Beta
    public static final ConfigKey<Boolean> USE_NTLM = WinRmTool.USE_NTLM;

    // TODO merge with {link SshTool#PROP_USER} and {@link SshMachineLocation#user}?
    public static final ConfigKey<String> USER = WinRmTool.PROP_USER;

    // TODO merge with {link SshTool#PROP_PASSWORD}?
    public static final ConfigKey<String> PASSWORD = WinRmTool.PROP_PASSWORD;

    // TODO Delete once winrm4j supports this better?
    public static final ConfigKey<Integer> COPY_FILE_CHUNK_SIZE_BYTES = WinRmTool.COPY_FILE_CHUNK_SIZE_BYTES;

    // Note that SshTool's implementation class *must* use a different key name. Both may be used 
    // within a location's configuration to indicate the implementation to use for WinRmTool and 
    // for SshTool - that will require two different configuration values.
    public static final ConfigKey<String> WINRM_TOOL_CLASS = ConfigKeys.newConfigKeyWithPrefixRemoved(
            BrooklynConfigKeys.BROOKLYN_WINRM_CONFIG_KEY_PREFIX,
            Preconditions.checkNotNull(BrooklynConfigKeys.WINRM_TOOL_CLASS, "static final initializer classload ordering problem"));

    /** 
     * Prefix for config key:values to be passed to the winrm tool on construction. For example, 
     * one could define the location below. When executing winrm commands, it would instantiate
     * an instance of {@code com.acme.brooklyn.MyCustomWinrmTool}, calling its constructor with a
     * {@code Map<String, Object>} that contained the configuration. In this case, the map would
     * include: {@code address=1.2.3.4}; {@code user=myname}; and {@code myparam=myvalue}.
     * 
     * <pre>
     * {@code
     * brooklyn.location.named.myLocation = byon:(hosts=1.2.3.4,user=myname)
     * brooklyn.location.named.myLocation.winrmToolClass = com.acme.brooklyn.MyCustomWinrmTool
     * brooklyn.location.named.myLocation.winrmToolClass.myparam = myvalue
     * }
     * }
     * </pre>
     * <p>
     */
    public static final String WINRM_TOOL_CLASS_PROPERTIES_PREFIX = WINRM_TOOL_CLASS.getName()+".";

    /**
     * @deprecated since 0.9.0; config never read; will be removed in future version.
     */
    @Deprecated
    public static final ConfigKey<Integer> EXECUTION_ATTEMPTS = ConfigKeys.newIntegerConfigKey(
            "windows.exec.attempts",
            "Number of attempts to execute a remote command",
            1);
    
    // TODO See SshTool#PROP_SSH_TRIES, where it was called "sshTries"; remove duplication? Merge into one well-named thing?
    public static final ConfigKey<Integer> EXEC_TRIES = WinRmTool.PROP_EXEC_TRIES;

    @SuppressWarnings("serial")
    public static final ConfigKey<Iterable<String>> PRIVATE_ADDRESSES = ConfigKeys.newConfigKey(
            new TypeToken<Iterable<String>>() {},
            "privateAddresses",
            "Private addresses of this machine, e.g. those within the private network", 
            null);

    @SuppressWarnings("serial")
    public static final ConfigKey<Map<Integer, String>> TCP_PORT_MAPPINGS = ConfigKeys.newConfigKey(
            new TypeToken<Map<Integer, String>>() {},
            "tcpPortMappings",
            "NAT'ed ports, giving the mapping from private TCP port to a public host:port", 
            null);

    public static final Set<HasConfigKey<?>> ALL_WINRM_CONFIG_KEYS =
            ImmutableSet.<HasConfigKey<?>>builder()
                    .addAll(ConfigUtils.getStaticKeysOnClass(WinRmMachineLocation.class))
                    .addAll(ConfigUtils.getStaticKeysOnClass(WinRmTool.class))
                    .build();

    public static final Set<String> ALL_WINRM_CONFIG_KEY_NAMES =
            ImmutableSet.copyOf(Iterables.transform(ALL_WINRM_CONFIG_KEYS, new Function<HasConfigKey<?>,String>() {
                @Override
                public String apply(HasConfigKey<?> input) {
                    return input.getConfigKey().getName();
                }
            }));
    
    @Override
    public void init() {
        super.init();

        // Register any pre-existing port-mappings with the PortForwardManager
        Map<Integer, String> tcpPortMappings = getConfig(TCP_PORT_MAPPINGS);
        if (tcpPortMappings != null) {
            PortForwardManager pfm = (PortForwardManager) getManagementContext().getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
            for (Map.Entry<Integer, String> entry : tcpPortMappings.entrySet()) {
                int targetPort = entry.getKey();
                HostAndPort publicEndpoint = HostAndPort.fromString(entry.getValue());
                if (!publicEndpoint.hasPort()) {
                    throw new IllegalArgumentException("Invalid portMapping ('"+entry.getValue()+"') for port "+targetPort+" in machine "+this);
                }
                pfm.associate(publicEndpoint.getHostText(), publicEndpoint, this, targetPort);
            }
        }
    }

    @Override
    protected MachineDetails detectMachineDetails() {
        // TODO: detect actual machine details via winRM
        return UNKNOWN_MACHINE_DETAILS;
    }

    public String getUser() {
        return config().get(USER);
    }

    @Override
    public InetAddress getAddress() {
        return getConfig(ADDRESS);
    }

    @Override
    public OsDetails getOsDetails() {
        return null;
    }

    @Override
    public MachineDetails getMachineDetails() {
        return null;
    }

    @Nullable
    @Override
    public String getHostname() {
        InetAddress address = getAddress();
        return (address != null) ? address.getHostAddress() : null;
    }

    @Nullable
    protected String getHostAndPort() {
        String host = getHostname();
        return (host == null) ? null : host + ":" + getDefaultPort();
    }

    public int getPort() {
        Maybe<Object> raw = config().getRaw(WinRmTool.PROP_PORT);
        if (raw.orNull() == null && config().getRaw(WINRM_CONFIG_PORT).orNull() != null) {
            return config().get(WINRM_CONFIG_PORT);
        } else {
            Integer result = config().get(WinRmTool.PROP_PORT);
            return (result != null) ? result : getDefaultPort();
        }
    }

    private int getDefaultPort() {
        return getConfig(USE_HTTPS_WINRM) ? 5986 : 5985;
    }

    @Override
    public Set<String> getPublicAddresses() {
        InetAddress address = getAddress();
        return (address == null) ? ImmutableSet.<String>of() : ImmutableSet.of(address.getHostAddress());
    }
    
    @Override
    public Set<String> getPrivateAddresses() {
        Iterable<String> result = getConfig(PRIVATE_ADDRESSES);
        return (result == null) ? ImmutableSet.<String>of() : ImmutableSet.copyOf(result);
    }

    /**
     * @deprecated since 0.9.0; use {@link #executeCommand(String)}
     */
    @Deprecated
    public WinRmToolResponse executeScript(String script) {
        return executeCommand(script);
    }

    /**
     * @deprecated since 0.9.0; use {@link #executeCommand(List)}
     */
    @Deprecated
    public WinRmToolResponse executeScript(List<String> script) {
        return executeCommand(script);
    }
    
    /**
     * @deprecated since 0.9.0; use {@link #executeCommand(Map, List)}
     */
    @Deprecated
    public WinRmToolResponse executeScript(Map<?,?> props, List<String> script) {
        return executeCommand(props, script);
    }

    /**
     * @since 0.9.0 (previously was {@code executeScript(String)}
     */
    public WinRmToolResponse executeCommand(String script) {
        return executeCommand(ImmutableList.of(script));
    }

    /**
     * @since 0.9.0 (previously was {@code executeScript(List)}
     */
    public WinRmToolResponse executeCommand(List<String> script) {
        return executeCommand(ImmutableMap.of(), script);
    }
    
    /**
     * @since 0.9.0 (previously was {@code executeScript(Map, List)}
     */
    public WinRmToolResponse executeCommand(Map<?,?> props, List<String> script) {
        WinRmTool tool = newWinRmTool(props);
        return tool.executeCommand(script);
    }

    public WinRmToolResponse executePsScript(String psScript) {
        return executePsScript(ImmutableMap.of(), ImmutableList.of(psScript));
    }

    public WinRmToolResponse executePsScript(List<String> psScript) {
        return executePsScript(ImmutableMap.of(), psScript);
    }
    
    public WinRmToolResponse executePsScript(Map<?,?> props, List<String> psScript) {
        WinRmTool tool = newWinRmTool(props);
        return tool.executePs(psScript);
    }

    protected WinRmTool newWinRmTool(Map<?,?> props) {
        // TODO See comments/TODOs in SshMachineLocation.connectSsh()
        try {
            ConfigBag args = new ConfigBag();

            for (Map.Entry<ConfigKey<?>, ?> entry: config().getBag().getAllConfigAsConfigKeyMap().entrySet()) {
    
                boolean include = false;
                String keyName = entry.getKey().getName();
                if (keyName.startsWith(WinRmTool.BROOKLYN_CONFIG_KEY_PREFIX)) {
                    keyName = Strings.removeFromStart(keyName, WinRmTool.BROOKLYN_CONFIG_KEY_PREFIX);
                    include = true;
                }
                
                if (keyName.startsWith(WINRM_TOOL_CLASS_PROPERTIES_PREFIX)) {
                    keyName = Strings.removeFromStart(keyName, WINRM_TOOL_CLASS_PROPERTIES_PREFIX);
                    include = true;
                }
                
                if (ALL_WINRM_CONFIG_KEY_NAMES.contains(keyName)) {
                    // key should be included, and does not need to be changed
    
                    // TODO make this config-setting mechanism more universal
                    // currently e.g. it will not admit a tool-specific property.
                    // thinking either we know about the tool here,
                    // or we don't allow unadorned keys to be set
                    // (require use of BROOKLYN_CONFIG_KEY_PREFIX)
                    include = true;
                }

                if (include) {
                    args.putStringKey(keyName, config().get(entry.getKey()));
                }
            }
            
            args.putAll(props);
            args.configure(SshTool.PROP_HOST, getAddress().getHostAddress());
            args.configure(WinRmTool.USE_NTLM, getConfig(WinRmMachineLocation.USE_NTLM));
            args.configure(WinRmTool.PROP_PORT, getPort());

            if (LOG.isTraceEnabled()) LOG.trace("creating WinRM session for "+Sanitizer.sanitize(args));

            // look up tool class
            String toolClass = args.get(WINRM_TOOL_CLASS);
            if (toolClass == null) toolClass = Winrm4jTool.class.getName();
            WinRmTool tool = (WinRmTool) new ClassLoaderUtils(this, getManagementContext()).loadClass(toolClass).getConstructor(Map.class).newInstance(args.getAllConfig());
            if (tool instanceof ManagementContextInjectable) {
                ((ManagementContextInjectable)tool).setManagementContext(getManagementContext());
            }

            if (LOG.isTraceEnabled()) LOG.trace("using ssh-tool {} (of type {}); props ", tool, toolClass);

            return tool;
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    public int copyTo(File source, String destination) {
        FileInputStream sourceStream = null;
        try {
            sourceStream = new FileInputStream(source);
            return copyTo(sourceStream, destination);
        } catch (FileNotFoundException e) {
            throw Exceptions.propagate(e);
        } finally {
            if (sourceStream != null) {
                Streams.closeQuietly(sourceStream);
            }
        }
    }

    public int copyTo(InputStream source, String destination) {
        return copyTo(ImmutableMap.of(), source, destination);
    }
    
    public int copyTo(Map<?,?> props, InputStream source, String destination) {
        WinRmTool tool = newWinRmTool(props);
        WinRmToolResponse response = tool.copyToServer(source, destination);
        return response.getStatusCode();
    }

    public static String getDefaultUserMetadataString(ConfigurationSupportInternal config) {
        // Using an encoded command obviates the need to escape
        String unencodePowershell = Joiner.on("\r\n").join(ImmutableList.of(
                // Allow TS connections
                "$RDP = Get-WmiObject -Class Win32_TerminalServiceSetting -ComputerName $env:computername -Namespace root\\CIMV2\\TerminalServices -Authentication PacketPrivacy",
                "$RDP.SetAllowTSConnections(1,1)",
                "Set-ExecutionPolicy Unrestricted -Force",
                // Set unlimited values for remote execution limits
                "Set-Item WSMan:\\localhost\\Shell\\MaxConcurrentUsers 100",
                "Set-Item WSMan:\\localhost\\Shell\\MaxMemoryPerShellMB 0",
                "Set-Item WSMan:\\localhost\\Shell\\MaxProcessesPerShell 0",
                "Set-Item WSMan:\\localhost\\Shell\\MaxShellsPerUser 0",
                "New-ItemProperty \"HKLM:\\System\\CurrentControlSet\\Control\\LSA\" -Name \"SuppressExtendedProtection\" -Value 1 -PropertyType \"DWord\"",
                // The following allows scripts to re-authenticate with local credential - this is required
                // as certain operations cannot be performed with remote credentials
                "$allowed = @('WSMAN/*')",
                "$key = 'hklm:\\SOFTWARE\\Policies\\Microsoft\\Windows\\CredentialsDelegation'",
                "if (!(Test-Path $key)) {",
                "    md $key",
                "}",
                "New-ItemProperty -Path $key -Name AllowFreshCredentials -Value 1 -PropertyType Dword -Force",
                "New-ItemProperty -Path $key -Name AllowFreshCredentialsWhenNTLMOnly -Value 1 -PropertyType Dword -Force",
                "$credKey = Join-Path $key 'AllowFreshCredentials'",
                "if (!(Test-Path $credKey)) {",
                "    md $credkey",
                "}",
                "$ntlmKey = Join-Path $key 'AllowFreshCredentialsWhenNTLMOnly'",
                "if (!(Test-Path $ntlmKey)) {",
                "    md $ntlmKey",
                "}",
                "$i = 1",
                "$allowed |% {",
                "    # Script does not take into account existing entries in this key",
                "    New-ItemProperty -Path $credKey -Name $i -Value $_ -PropertyType String -Force",
                "    New-ItemProperty -Path $ntlmKey -Name $i -Value $_ -PropertyType String -Force",
                "    $i++",
                "}"
        ));

        // FIXME USE_HTTPS_WINRM
        // Missing generate certificate step.
        // https://support.microsoft.com/en-us/kb/2019527
        //
        // One possible approach is to generate a certificate and append it to this command.
        // http://stackoverflow.com/questions/1615871/creating-an-x509-certificate-in-java-without-bouncycastle
        //
        // @neykov:
        // The certificate is best generated on the machine, without leaving it ever (for self-signed case). On the other hand it's not possible to get the public part at this step.
        // I see this setup step dissappearing longer term (or keeping it minimalistic).
        // Instead do something like the jclouds init sequence where it connects with whatever is provided by the cloud, then configuring it to our liking.
        boolean useSecureWinrm = config.getBag().get(USE_HTTPS_WINRM);

        boolean basicAuth = !config.getBag().get(USE_NTLM), allowUnencrypted = !useSecureWinrm;
        int port = useSecureWinrm ? 5986 : 5985;

        String encoded = new String(Base64.encodeBase64(unencodePowershell.getBytes(Charsets.UTF_16LE)));
        return String.format("winrm quickconfig -q & " +
                "winrm set winrm/config/service/auth @{Basic=\"%1$s\"} & " +
                "winrm set winrm/config/service @{AllowUnencrypted=\"%2$s\"} & " +
                "winrm set winrm/config/winrs @{MaxConcurrentUsers=\"100\"} & " +
                "winrm set winrm/config/winrs @{MaxMemoryPerShellMB=\"0\"} & " +
                "winrm set winrm/config/winrs @{MaxProcessesPerShell=\"0\"} & " +
                "winrm set winrm/config/winrs @{MaxShellsPerUser=\"0\"} & " +
                "netsh advfirewall firewall add rule name=RDP dir=in protocol=tcp localport=3389 action=allow profile=any & " +
                "netsh advfirewall firewall add rule name=WinRM dir=in protocol=tcp localport=%3$d action=allow profile=any & " +
                "powershell -EncodedCommand ",
                basicAuth,
                allowUnencrypted,
                port
                )
                + encoded;
        /* TODO: Find out why scripts with new line characters aren't working on AWS. The following appears as if it *should*
           work but doesn't - the script simply isn't run. By connecting to the machine via RDP, you can get the script
           from 'http://169.254.169.254/latest/user-data', and running it at the command prompt works, but for some
           reason the script isn't run when the VM is provisioned
        */
//        return Joiner.on("\r\n").join(ImmutableList.of(
//                "winrm quickconfig -q",
//                "winrm set winrm/config/service/auth @{Basic=\"true\"}",
//                "winrm set winrm/config/client @{AllowUnencrypted=\"true\"}",
//                "winrm set winrm/config/service @{AllowUnencrypted=\"true\"}",
//                "netsh advfirewall firewall add rule name=RDP dir=in protocol=tcp localport=3389 action=allow profile=any",
//                "netsh advfirewall firewall add rule name=WinRM dir=in protocol=tcp localport=5985 action=allow profile=any",
//                // Using an encoded command necessitates the need to escape. The unencoded command is as follows:
//                // $RDP = Get-WmiObject -Class Win32_TerminalServiceSetting -ComputerName $env:computername -Namespace root\CIMV2\TerminalServices -Authentication PacketPrivacy
//                // $Result = $RDP.SetAllowTSConnections(1,1)
//                "powershell -EncodedCommand JABSAEQAUAAgAD0AIABHAGUAdAAtAFcAbQBpAE8AYgBqAGUAYwB0ACAALQBDAGwAYQBzAHMAI" +
//                        "ABXAGkAbgAzADIAXwBUAGUAcgBtAGkAbgBhAGwAUwBlAHIAdgBpAGMAZQBTAGUAdAB0AGkAbgBnACAALQBDAG8AbQBwA" +
//                        "HUAdABlAHIATgBhAG0AZQAgACQAZQBuAHYAOgBjAG8AbQBwAHUAdABlAHIAbgBhAG0AZQAgAC0ATgBhAG0AZQBzAHAAY" +
//                        "QBjAGUAIAByAG8AbwB0AFwAQwBJAE0AVgAyAFwAVABlAHIAbQBpAG4AYQBsAFMAZQByAHYAaQBjAGUAcwAgAC0AQQB1A" +
//                        "HQAaABlAG4AdABpAGMAYQB0AGkAbwBuACAAUABhAGMAawBlAHQAUAByAGkAdgBhAGMAeQANAAoAJABSAGUAcwB1AGwAd" +
//                        "AAgAD0AIAAkAFIARABQAC4AUwBlAHQAQQBsAGwAbwB3AFQAUwBDAG8AbgBuAGUAYwB0AGkAbwBuAHMAKAAxACwAMQApAA=="
//        ));
    }

    @Override
    public String resolveOnBoxDirFor(Entity entity, String unresolvedPath) {
        // TODO this is simplistic, writes at c:\ for HOME
        if (unresolvedPath.startsWith("./") || unresolvedPath.startsWith("~/")) {
            unresolvedPath = "C:\\"+unresolvedPath.substring(2);
        }
        return unresolvedPath.replaceAll("/", "\\");
    }

}
