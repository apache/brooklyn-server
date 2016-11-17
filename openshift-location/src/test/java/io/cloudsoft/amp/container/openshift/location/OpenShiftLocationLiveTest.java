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
package io.cloudsoft.amp.container.openshift.location;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.location.BasicMachineDetails;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.network.OnPublicNetworkEnricher;
import org.apache.brooklyn.core.objs.BasicSpecParameter;
import org.apache.brooklyn.core.sensor.PortAttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.os.Os;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

import io.cloudsoft.amp.container.kubernetes.location.KubernetesLocationConfig;

/**
 * Assumes that a pre-existing OpenShift endpoint is available. See system properties and the defaults below.
 */
public class OpenShiftLocationLiveTest extends BrooklynAppLiveTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(OpenShiftLocationLiveTest.class);

    private static final String OPENSHIFT_ENDPOINT = System.getProperty("test.amp.openshift.endpoint", "https://192.168.99.100:8443/");
    private static final String CERTS_BASE_DIR = System.getProperty("test.amp.openshift.certsBaseDir", Os.mergePaths(System.getProperty("user.home"), "openshift-certs"));
    private static final String CA_CERT_FILE = System.getProperty("test.amp.openshift.caCert", Os.mergePaths(CERTS_BASE_DIR, "ca.crt"));
    private static final String CLIENT_CERT_FILE = System.getProperty("test.amp.openshift.clientCert", Os.mergePaths(CERTS_BASE_DIR, "admin.crt"));
    private static final String CLIENT_KEY_FILE = System.getProperty("test.amp.openshift.clientKey", Os.mergePaths(CERTS_BASE_DIR, "admin.key"));
    private static final String NAMESPACE = System.getProperty("test.amp.openshift.namespace", "");

    protected OpenShiftLocation loc;
    protected List<MachineLocation> machines;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        machines = Lists.newCopyOnWriteArrayList();
    }

    // FIXME: Clear up properly: Test leaves deployment, replicas and pods behind if obtain fails.
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        for (MachineLocation machine : machines) {
            try {
                loc.release(machine);
            } catch (Exception e) {
                LOG.error("Error releasing machine "+machine+" in location "+loc, e);
            }
        }
        super.tearDown();
    }

    protected OpenShiftLocation newOpenShiftLocation(Map<String, ?> flags) throws Exception {
        Map<String,?> allFlags = MutableMap.<String,Object>builder()
                .put("endpoint", OPENSHIFT_ENDPOINT)
                .put("kubernetes.caCert", CA_CERT_FILE)
                .put("kubernetes.clientCert", CLIENT_CERT_FILE)
                .put("kubernetes.clientKey", CLIENT_KEY_FILE)
                .put("kubernetes.namespace", NAMESPACE)
                .put("kubernetes.privileged", "true")
                .putAll(flags)
                .build();
        return (OpenShiftLocation) mgmt.getLocationRegistry().getLocationManaged("openshift", allFlags);
    }

    // TODO Fails because can't ssh to container
    @Test(groups={"Live", "Broken"}, enabled=false)
    public void testDefaultImage() throws Exception {
        loc = newOpenShiftLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());

        assertOsNameContains(machine, "ubuntu");
    }

    @Test(groups={"Live"})
    public void testCloudsoftCentosImage() throws Exception {
        loc = newOpenShiftLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .put(KubernetesLocationConfig.IMAGE.getName(), "cloudsoft/centos:7")
                .put(KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName(), "p4ssw0rd")
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());

        assertTrue(machine.isSshable(), "not sshable machine="+machine);
        assertOsNameContains(machine, "centos");
    }

    @Test(groups={"Live"})
    public void testOpenPorts() throws Exception {
        List<Integer> inboundPorts = ImmutableList.of(22, 443, 8000, 8081);
        loc = newOpenShiftLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .put(KubernetesLocationConfig.IMAGE.getName(), "cloudsoft/centos:7")
                .put(KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName(), "p4ssw0rd")
                .put(KubernetesLocationConfig.INBOUND_PORTS.getName(), inboundPorts)
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());
        assertTrue(machine.isSshable());
        
        String publicHostText = machine.getSshHostAndPort().getHostText();
        PortForwardManager pfm = (PortForwardManager) mgmt.getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        for (int targetPort : inboundPorts) {
            HostAndPort mappedPort = pfm.lookup(machine, targetPort);
            assertNotNull(mappedPort, "no mapping for targetPort "+targetPort);
            assertEquals(mappedPort.getHostText(), publicHostText);
            assertTrue(mappedPort.hasPort(), "no port-part in "+mappedPort+" for targetPort "+targetPort);
        }
    }

    @Test(groups={"Live"})
    public void testDeployNetcatApp() throws Exception {
        loc = newOpenShiftLocation(ImmutableMap.of(
                KubernetesLocationConfig.IMAGE.getName(), "cloudsoft/centos:7",
                KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName(), "p4ssw0rd"));
        
        PortAttributeSensorAndConfigKey portKey = ConfigKeys.newPortSensorAndConfigKey("netcat.port", "my port");
        AttributeSensor<String> publicPortSensor = Sensors.newStringSensor("netcat.endpoint.mapped.public");
        
        // Setting port with BasicSpecParameter so it is picked up as a port that must be opened
        // Not using `sudo`, because already running as root
        VanillaSoftwareProcess entity = app.addChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .parametersAdd(ImmutableList.of(new BasicSpecParameter<PortRange>(portKey.getName(), true, portKey.getConfigKey(), portKey)))
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND,
                        "yum install -y nc")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND,
                        "echo $MESSAGE | nc -l $NETCAT_PORT &" + "\n" +
                        "echo $! > $PID_FILE")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, 
                        "test -f $PID_FILE && ps -p $(cat $PID_FILE)") 
                .configure(VanillaSoftwareProcess.SHELL_ENVIRONMENT.getName(), ImmutableMap.of( 
                        "MESSAGE", "mymessage",
                        "NETCAT_PORT", "8081"))
                .configure(portKey.getName(), 8081)
                .enricher(EnricherSpec.create(OnPublicNetworkEnricher.class)
                        .configure(OnPublicNetworkEnricher.SENSORS, ImmutableList.of(portKey)))
                );
        
        app.start(ImmutableList.of(loc));
        
        String publicMapped = EntityAsserts.assertAttributeEventuallyNonNull(entity, publicPortSensor);
        HostAndPort publicPort = HostAndPort.fromString(publicMapped);
        
        assertTrue(Networking.isReachable(publicPort), "publicPort="+publicPort);
    }

    protected void assertOsNameContains(SshMachineLocation machine, String expectedNamePart) {
        MachineDetails machineDetails = app.getExecutionContext()
                .submit(BasicMachineDetails.taskForSshMachineLocation(machine))
                .getUnchecked();
        OsDetails osDetails = machineDetails.getOsDetails();
        String osName = osDetails.getName();
        assertTrue(osName != null && osName.toLowerCase().contains(expectedNamePart), "osDetails="+osDetails);
    }
    
    protected SshMachineLocation newContainerMachine(OpenShiftLocation loc, Map<?, ?> flags) throws Exception {
        MachineLocation result = loc.obtain(flags);
        machines.add(result);
        return (SshMachineLocation) result;
    }
}
