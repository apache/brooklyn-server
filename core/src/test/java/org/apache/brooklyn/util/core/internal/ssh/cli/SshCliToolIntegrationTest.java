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
package org.apache.brooklyn.util.core.internal.ssh.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.internal.ssh.SshException;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.ssh.SshToolAbstractIntegrationTest;
import org.apache.brooklyn.util.os.Os;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

import static org.testng.Assert.*;

/**
 * Test the operation of the {@link SshCliTool} utility class.
 */
public class SshCliToolIntegrationTest extends SshToolAbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(SshCliToolIntegrationTest.class);
    
    @Override
    protected SshTool newUnregisteredTool(Map<String,?> flags) {
        return new SshCliTool(flags);
    }

    @Test(groups = {"Integration"})
    public void testFlags() throws Exception {
        final SshTool localtool = newTool(ImmutableMap.of("sshFlags", "-vvv -tt", "host", "localhost"));
        tools.add(localtool);
        try {
            localtool.connect();
            Map<String,Object> props = new LinkedHashMap<String, Object>();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            props.put("out", out);
            props.put("err", err);
            int exitcode = localtool.execScript(props, Arrays.asList("echo hello err > /dev/stderr"), null);
            Assert.assertEquals(0, exitcode, "exitCode="+exitcode+", but expected 0");
            log.debug("OUT from ssh -vvv command is: "+out);
            log.debug("ERR from ssh -vvv command is: "+err);
            assertFalse(err.toString().contains("hello err"), "hello found where it shouldn't have been, in stderr (should have been tty merged to stdout): "+err);
            assertTrue(out.toString().contains("hello err"), "no hello in stdout: "+err);
            // look for word 'ssh' to confirm we got verbose output
            assertTrue(err.toString().toLowerCase().contains("ssh"), "no mention of ssh in stderr: "+err);
        } catch (SshException e) {
            if (!e.toString().contains("failed to connect")) throw e;
        }
    }

    // Need to have at least one test method here (rather than just inherited) for eclipse to recognize it
    @Test(enabled = false)
    public void testDummy() throws Exception {
    }
    
    // TODO When running mvn on the command line (for Aled), this test hangs when prompting for a password (but works in the IDE!)
    // Doing .connect() isn't enough; need to cause ssh or scp to be invoked
    @Override
    @Test(enabled=false, groups = {"Integration"})
    public void testConnectWithInvalidUserThrowsException() throws Exception {
        final SshTool localtool = newTool(ImmutableMap.of("user", "wronguser", "host", "localhost", "privateKeyFile", "~/.ssh/id_rsa"));
        tools.add(localtool);
        try {
            localtool.connect();
            int result = localtool.execScript(ImmutableMap.<String,Object>of(), ImmutableList.of("date"));
            fail("exitCode="+result+", but expected exception");
        } catch (SshException e) {
            if (!e.toString().contains("failed to connect")) throw e;
        }
    }
    
    // TODO ssh-cli doesn't support pass-phrases yet
    @Override
    @Test(enabled=false, groups = {"Integration"})
    public void testSshKeyWithPassphrase() throws Exception {
        super.testSshKeyWithPassphrase();
    }

    // Setting last modified date not yet supported for cli-based ssh
    @Override
    @Test(enabled=false, groups = {"Integration"})
    public void testCopyToServerWithLastModifiedDate() throws Exception {
        super.testCopyToServerWithLastModifiedDate();
    }
    
    @Override
    @Test(groups = {"Integration"})
    public void testExecReturningNonZeroExitCode() throws Exception {
        int exitcode = tool.execCommands(MutableMap.<String,Object>of(), ImmutableList.of("exit 123"));
        assertEquals(exitcode, 123);
    }

    @Test(groups = {"Integration"})
    public void testSshExecutable() throws IOException {

        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("ssh-executable.sh")).getPath();
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        perms.add(PosixFilePermission.OWNER_READ);
        Files.setPosixFilePermissions(Paths.get(path), perms);

        final SshTool localTool = newTool(ImmutableMap.of(
                "sshExecutable", path,
                "user", Os.user(),
                "host", "localhost",
                "privateKeyData", "myKeyData",
                "password", "testPassword"));
        tools.add(localTool);

        try {
            localTool.connect();
            Map<String,Object> props = new LinkedHashMap<>();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            props.put("out", out);
            props.put("err", err);
            int exitcode = localTool.execScript(props, Arrays.asList("echo hello err > /dev/stderr"), null);
            Assert.assertEquals(0, exitcode, "exitCode=" + exitcode + ", but expected 0");
            log.debug("OUT from ssh -vvv command is: " + out);
            log.debug("ERR from ssh -vvv command is: " + err);

            // Look for the rest of env vars to confirm we got them passed to sshExecutable.
            String stdout = out.toString();
            assertTrue(stdout.contains("SSH_USER=" + Os.user()), "no SSH_USER in stdout: " + out);
            assertTrue(stdout.contains("SSH_HOST=localhost"), "no SSH_HOST in stdout: " + out);
            assertTrue(stdout.contains("SSH_PASSWORD=testPassword"), "no SSH_PASSWORD in stdout: " + out);
            assertTrue(stdout.contains("SSH_COMMAND_BODY=/tmp/brooklyn-"), "no SSH_COMMAND_BODY in stdout: " + out);
            assertTrue(stdout.contains("SSH_TEMP_KEY_FILE=/tmp/sshcopy-"), "no SSH_TEMP_KEY_FILE in stdout: " + out);
            assertTrue(stdout.contains("myKeyData"), "no SSH_TEMP_KEY_FILE content in stdout: " + out);

        } catch (SshException e) {
            if (!e.toString().contains("failed to connect")) throw e;
        }
    }

    @Test(groups = {"Integration"})
    public void testScpExecutable() throws IOException {

        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("scp-executable.sh")).getPath();
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        perms.add(PosixFilePermission.OWNER_READ);
        Files.setPosixFilePermissions(Paths.get(path), perms);

        final SshTool localTool = newTool(ImmutableMap.of(
                "scpExecutable", path,
                "user", Os.user(),
                "host", "localhost",
//                "privateKeyData", "myKeyData", // TODO: loops to itself to copy the key file, skip in the test.
                "password", "testPassword"));
        tools.add(localTool);

        try {
            localTool.connect();
            Map<String,Object> props = new LinkedHashMap<>();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            props.put("out", out);
            props.put("err", err);
            int exitcode = localTool.copyToServer(props, "echo hello world!\n".getBytes(), remoteFilePath);

            Assert.assertEquals(0, exitcode, "exitCode=" + exitcode + ", but expected 0");

            String copiedFileContent = new String(Files.readAllBytes(Paths.get(remoteFilePath)));
            log.info("Contents of copied file with custom scpExecutable: " + copiedFileContent);

            // Look for the rest of env vars to confirm we got them passed to scpExecutable.
            assertTrue(copiedFileContent.contains("echo hello world!"), "no command in the remote file: " + out);
            assertTrue(copiedFileContent.contains("SCP_PASSWORD=testPassword"), "no SCP_PASSWORD in the remote file: " + out);
//            assertTrue(copiedFileContent.contains("SCP_TEMP_KEY_FILE="), "no SCP_TEMP_KEY_FILE in the remote file: " + out);
//            assertTrue(copiedFileContent.contains("myKeyData"), "no SSH_TEMP_KEY_FILE content in stdout: " + out);

        } catch (SshException e) {
            if (!e.toString().contains("failed to connect")) throw e;
        }
    }
}
