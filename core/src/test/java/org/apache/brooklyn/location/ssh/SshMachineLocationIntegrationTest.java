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
package org.apache.brooklyn.location.ssh;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.InetAddress;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.crypto.SecureKeys;
import org.apache.brooklyn.util.core.file.ArchiveUtils;
import org.apache.brooklyn.util.core.internal.ssh.SshException;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.ssh.sshj.SshjTool;
import org.apache.brooklyn.util.core.internal.ssh.sshj.SshjTool.SshjToolBuilder;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class SshMachineLocationIntegrationTest extends SshMachineLocationTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntity.class);

    @Override
    protected BrooklynProperties getBrooklynProperties() {
        // Requires location named "localhost-passphrase", which it expects to find in local 
        // brooklyn.properties (or brooklyn.cfg in karaf).
        return BrooklynProperties.Factory.newDefault();
    }
    
    @Override
    protected SshMachineLocation newHost() {
        return mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", Networking.getLocalHost()));
    }

    // Overridden just to make it integration (because `newHost()` returns a real ssh'ing host)
    @Test(groups="Integration")
    @Override
    public void testSshExecScript() throws Exception {
        super.testSshExecScript();
    }
    
    // Overridden just to make it integration (because `newHost()` returns a real ssh'ing host)
    @Test(groups="Integration")
    @Override
    public void testSshExecCommands() throws Exception {
        super.testSshExecCommands();
    }
    
    @Test(groups="Integration")
    public void testIsSshableWhenTrue() throws Exception {
        assertTrue(host.isSshable());
    }

    // Overridden just to make it integration (because `newHost()` returns a real ssh'ing host)
    @Test(groups="Integration")
    @Override
    public void testDoesNotLogPasswordsInEnvironmentVariables() {
        super.testDoesNotLogPasswordsInEnvironmentVariables();
    }

    // Overrides super, because expect real machine details (rather than asserting our stub data)
    @Test(groups = "Integration")
    @Override
    public void testGetMachineDetails() throws Exception {
        BasicExecutionManager execManager = new BasicExecutionManager("mycontextid");
        BasicExecutionContext execContext = new BasicExecutionContext(execManager);
        try {
            MachineDetails details = execContext.submit(new Callable<MachineDetails>() {
                @Override
                public MachineDetails call() {
                    return host.getMachineDetails();
                }}).get();
            LOG.info("machineDetails="+details);
            assertNotNull(details);
        } finally {
            execManager.shutdownNow();
        }
    }

    // Overrides and disables super, because real machine won't give extra stdout
    @Test(enabled=false)
    @Override
    public void testGetMachineDetailsWithExtraStdout() throws Exception {
        throw new UnsupportedOperationException("Test disabled because real machine does not have extra stdout");
    }

    @Test(groups = "Integration")
    public void testCopyFileTo() throws Exception {
        File dest = Os.newTempFile(getClass(), ".dest.tmp");
        File src = Os.newTempFile(getClass(), ".src.tmp");
        try {
            Files.write("abc", src, Charsets.UTF_8);
            host.copyTo(src, dest);
            assertEquals("abc", Files.readFirstLine(dest, Charsets.UTF_8));
        } finally {
            src.delete();
            dest.delete();
        }
    }

    // Note: requires `ssh localhost` to be setup such that no password is required    
    @Test(groups = "Integration")
    public void testCopyStreamTo() throws Exception {
        String contents = "abc";
        File dest = new File(Os.tmp(), "sshMachineLocationTest_dest.tmp");
        try {
            host.copyTo(Streams.newInputStreamWithContents(contents), dest.getAbsolutePath());
            assertEquals("abc", Files.readFirstLine(dest, Charsets.UTF_8));
        } finally {
            dest.delete();
        }
    }

    // Requires internet connectivity; on guest wifi etc can fail with things like
    // "Welcome to Virgin Trains" etc.
    @Test(groups = "Integration")
    public void testInstallUrlTo() throws Exception {
        File dest = new File(Os.tmp(), "sshMachineLocationTest_dir/");
        dest.mkdir();
        try {
            int result = host.installTo("https://raw.github.com/brooklyncentral/brooklyn/master/README.md", Urls.mergePaths(dest.getAbsolutePath(), "README.md"));
            assertEquals(result, 0);
            String contents = ArchiveUtils.readFullyString(new File(dest, "README.md"));
            assertTrue(contents.contains("http://brooklyncentral.github.com"), "contents missing expected phrase; contains:\n"+contents);
        } finally {
            dest.delete();
        }
    }
    
    @Test(groups = "Integration")
    public void testInstallClasspathCopyTo() throws Exception {
        File dest = new File(Os.tmp(), "sshMachineLocationTest_dir/");
        dest.mkdir();
        try {
            int result = host.installTo("classpath://brooklyn/config/sample.properties", Urls.mergePaths(dest.getAbsolutePath(), "sample.properties"));
            assertEquals(result, 0);
            String contents = ArchiveUtils.readFullyString(new File(dest, "sample.properties"));
            assertTrue(contents.contains("Property 1"), "contents missing expected phrase; contains:\n"+contents);
        } finally {
            dest.delete();
        }
    }

    // See http://superuser.com/a/698251/497648 for choice of unreachable IP.
    // Even with 240.0.0.0, it takes a long time (75 seconds in office network).
    //
    // Previously, "123.123.123.123" would seemingly hang on some (home/airport) networks.
    // Times out (with 123.123.123.123) in 2m7s on Ubuntu Vivid (syn retries set to 6)
    //
    // Make sure we fail, waiting for longer than the 70 second TCP timeout.
    @Test(groups = "Integration")
    public void testIsSshableWhenFalse() throws Exception {
        String unreachableIp = "240.0.0.0";
        final SshMachineLocation unreachableHost = new SshMachineLocation(MutableMap.of("address", InetAddress.getByName(unreachableIp)));
        System.out.println(unreachableHost.getAddress());
        Asserts.assertReturnsEventually(new Runnable() {
            @Override
            public void run() {
                assertFalse(unreachableHost.isSshable());
            }},
            Duration.minutes(3));
    }

    // For issue #230
    @Test(groups = "Integration")
    public void testOverridingPropertyOnExec() throws Exception {
        SshMachineLocation host = new SshMachineLocation(MutableMap.of("address", Networking.getLocalHost(), "sshPrivateKeyData", "wrongdata"));
        
        OutputStream outStream = new ByteArrayOutputStream();
        String expectedName = Os.user();
        host.execCommands(MutableMap.of("sshPrivateKeyData", null, "out", outStream), "my summary", ImmutableList.of("whoami"));
        String outString = outStream.toString();
        
        assertTrue(outString.contains(expectedName), "outString="+outString);
    }

    @Test(groups = "Integration", expectedExceptions={IllegalStateException.class, SshException.class})
    public void testSshRunWithInvalidUserFails() throws Exception {
        SshMachineLocation badHost = new SshMachineLocation(MutableMap.of("user", "doesnotexist", "address", Networking.getLocalHost()));
        badHost.execScript("mysummary", ImmutableList.of("whoami; exit"));
    }
    
    // Note: requires `named:localhost-passphrase` set up with a key whose passphrase is "localhost"
    // * create the key with:
    //      ssh-keygen -t rsa -N "brooklyn" -f ~/.ssh/id_rsa_passphrase
    //      ssh-copy-id localhost
    // * create brooklyn.properties, containing:
    //      brooklyn.location.named.localhost-passphrase=localhost
    //      brooklyn.location.named.localhost-passphrase.privateKeyFile=~/.ssh/id_rsa_passphrase
    //      brooklyn.location.named.localhost-passphrase.privateKeyPassphrase=brooklyn
    @Test(groups = "Integration")
    public void testExtractingConnectablePassphraselessKey() throws Exception {
        Maybe<LocationSpec<? extends Location>> lhps = mgmt.getLocationRegistry().getLocationSpec("named:localhost-passphrase");
        Preconditions.checkArgument(lhps.isPresent(), "This test requires a localhost named location called 'localhost-passphrase' (which should have a passphrase set)");
        LocalhostMachineProvisioningLocation lhp = (LocalhostMachineProvisioningLocation) mgmt.getLocationManager().createLocation(lhps.get());
        SshMachineLocation sm = lhp.obtain();
        
        SshjToolBuilder builder = SshjTool.builder().host(sm.getAddress().getHostName()).user(sm.getUser());
        
        KeyPair data = sm.findKeyPair();
        if (data!=null) builder.privateKeyData(SecureKeys.toPem(data));
        String password = sm.findPassword();
        if (password!=null) builder.password(password);
        SshjTool tool = builder.build();
        tool.connect();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int result = tool.execCommands(MutableMap.<String,Object>of("out", out), Arrays.asList("date"));
        Assert.assertTrue(out.toString().contains(" 20"), "out="+out);
        assertEquals(result, 0);
    }

    @Test(groups = "Integration")
    public void testExecScriptScriptDirFlagIsRespected() throws Exception {
        // For explanation of (some of) the magic behind this command, see http://stackoverflow.com/a/229606/68898
        final String command = "if [[ \"$0\" == \"/var/tmp/\"* ]]; then true; else false; fi";

        LocalhostMachineProvisioningLocation lhp = mgmt.getLocationManager().createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class));
        SshMachineLocation sm = lhp.obtain();

        Map<String, Object> props = ImmutableMap.<String, Object>builder()
                .put(SshTool.PROP_SCRIPT_DIR.getName(), "/var/tmp")
                .build();
        int rc = sm.execScript(props, "Test script directory execution", ImmutableList.of(command));
        assertEquals(rc, 0);
    }

    @Test(groups = "Integration")
    public void testLocationScriptDirConfigIsRespected() throws Exception {
        // For explanation of (some of) the magic behind this command, see http://stackoverflow.com/a/229606/68898
        final String command = "if [[ \"$0\" == \"/var/tmp/\"* ]]; then true; else false; fi";

        Map<String, Object> locationConfig = ImmutableMap.<String, Object>builder()
                .put(SshMachineLocation.SCRIPT_DIR.getName(), "/var/tmp")
                .build();

        LocalhostMachineProvisioningLocation lhp = 
            mgmt.getLocationManager().createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class)
                .configure(locationConfig));
        SshMachineLocation sm = lhp.obtain();

        int rc = sm.execScript("Test script directory execution", ImmutableList.of(command));
        assertEquals(rc, 0);
    }
    
    @Test(groups = "Integration")
    public void testMissingLocationScriptDirIsAlsoOkay() throws Exception {
        final String command = "echo hello";

        Map<String, Object> locationConfig = ImmutableMap.<String, Object>builder()
//                .put(SshMachineLocation.SCRIPT_DIR.getName(), "/var/tmp")
                .build();

        LocalhostMachineProvisioningLocation lhp = 
            mgmt.getLocationManager().createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class)
                .configure(locationConfig));
        SshMachineLocation sm = lhp.obtain();

        int rc = sm.execScript("Test script directory execution", ImmutableList.of(command));
        assertEquals(rc, 0);
    }
}
