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
package org.apache.brooklyn.entity;

import static org.testng.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * How to use:
 * <p>
 * Define immediate abstract subclasses with @Test methods for the platforms supported by the
 * cloud with provider returned by {@link #getProvider()}.
 * <p>
 * Then those subclasses can get concrete subclasses of their own that define {@code {@link #doTest(Location)}}
 * in order to run a test on all those platforms.
 * </p>
 * At a minimum the {@code doTest} is going to have to create an application with something
 * of interest to test in it, and start the app:
 * <pre>
 * final XYZEntity xyz = app.createAndManageChild(EntitySpec.create(XYZEntity.class));
 * app.start(ImmutableList.of(loc));
 * </pre>
 */
public abstract class AbstractMultiDistroLiveTest extends BrooklynAppLiveTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMultiDistroLiveTest.class);
    
    protected BrooklynProperties brooklynProperties;
    protected Location jcloudsLocation;

    public abstract String getProvider();

    public abstract String getLocationSpec();

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        // Don't let any defaults from brooklyn.properties (except credentials) interfere with test
        List<String> propsToRemove = ImmutableList.of("imageId", "imageDescriptionRegex", "imageNameRegex", "inboundPorts", "hardwareId", "minRam");

        // Don't let any defaults from brooklyn.properties (except credentials) interfere with test
        brooklynProperties = BrooklynProperties.Factory.newDefault();
        for (String propToRemove : propsToRemove) {
            for (String propVariant : ImmutableList.of(propToRemove,
                CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, propToRemove))) {

                brooklynProperties.remove("brooklyn.locations.jclouds."+getProvider()+"."+propVariant);
                brooklynProperties.remove("brooklyn.locations."+propVariant);
                brooklynProperties.remove("brooklyn.jclouds."+getProvider()+"."+propVariant);
                brooklynProperties.remove("brooklyn.jclouds."+propVariant);
            }
        }

        // Also removes scriptHeader (e.g. if doing `. ~/.bashrc` and `. ~/.profile`, then that can cause "stdin: is not a tty")
        brooklynProperties.remove("brooklyn.ssh.config.scriptHeader");
        
        LocalManagementContextForTests localManagementContextForTests = new LocalManagementContextForTests(brooklynProperties);
        localManagementContextForTests.generateManagementPlaneId();

        mgmt = localManagementContextForTests;
        mgmt.getHighAvailabilityManager().disabled(false);
        
        super.setUp();
    }


    protected void runTest(Map<String,?> flags) throws Exception {
        Map<String,?> allFlags = MutableMap.<String,Object>builder()
                .put("tags", ImmutableList.of(getClass().getName()))
                .put(JcloudsLocationConfig.MACHINE_CREATE_ATTEMPTS.getName(), 1)
                .putAll(flags)
                .build();
        jcloudsLocation = mgmt.getLocationRegistry().getLocationManaged(getLocationSpec(), allFlags);

        doTest(jcloudsLocation);
    }

    protected abstract void doTest(Location loc) throws Exception;

    protected void assertExecSsh(SoftwareProcess entity, List<String> commands) {
        SshMachineLocation machine = Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class).get();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        int result = machine.execScript(ImmutableMap.of("out", outStream, "err", errStream), "url-reachable", commands);
        String out = new String(outStream.toByteArray());
        String err = new String(errStream.toByteArray());
        if (result == 0) {
            LOG.debug("Successfully executed: cmds="+commands+"; stderr="+err+"; out="+out);
        } else {
            fail("Failed to execute: result="+result+"; cmds="+commands+"; stderr="+err+"; out="+out);
        }
    }

    protected void assertViaSshLocalPortListeningEventually(final SoftwareProcess server, final int port) {
        Asserts.succeedsEventually(ImmutableMap.of("timeout", Duration.FIVE_MINUTES), new Runnable() {
            @Override
            public void run() {
                assertExecSsh(server, ImmutableList.of("netstat -antp", "netstat -antp | grep LISTEN | grep "+port));
            }});
    }

    protected void assertViaSshLocalUrlListeningEventually(final SoftwareProcess server, final String url) {
        Asserts.succeedsEventually(ImmutableMap.of("timeout", Duration.FIVE_MINUTES), new Runnable() {
            @Override
            public void run() {
                assertExecSsh(server, ImmutableList.of(BashCommands.installPackage("curl"), "netstat -antp", "curl -k --retry 3 "+url));
            }});
    }
}
