package io.cloudsoft.amp.container.kubernetes.location;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.core.location.BasicMachineDetails;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

/**
 * Assumes that a pre-existing kubernetes endpoint is available. See system properties and the defaults below.
 */
public class KubernetesLocationLiveTest extends BrooklynAppLiveTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLocationLiveTest.class);
    
    private static final String KUBERNETES_ENDPOINT = System.getProperty("test.amp.kubernetes.endpoint", "https://192.168.99.100:8443/");
    private static final String IDENTITY = System.getProperty("test.amp.kubernetes.identity", "");
    private static final String CREDENTIAL = System.getProperty("test.amp.kubernetes.credential", "");

    protected KubernetesLocation loc;
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
    
    protected KubernetesLocation newKubernetesLocation(Map<String, ?> flags) throws Exception {
        Map<String,?> allFlags = MutableMap.<String,Object>builder()
                .put("identity", IDENTITY)
                .put("credential", CREDENTIAL)
                .put("endpoint", KUBERNETES_ENDPOINT)
                .putAll(flags)
                .build();
        return (KubernetesLocation) mgmt.getLocationRegistry().getLocationManaged("kubernetes", allFlags);
    }

    @Test(groups={"Live"})
    public void testDefaultImage() throws Exception {
        // Default is "cloudsoft/centos:7"
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .put(KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName(), "p4ssw0rd")
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());

        assertTrue(machine.isSshable());
        assertOsNameContains(machine, "centos");
    }

    @Test(groups={"Live"})
    public void testCloudsoftCentosImage() throws Exception {
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
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
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
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

    protected void assertOsNameContains(SshMachineLocation machine, String expectedNamePart) {
        MachineDetails machineDetails = app.getExecutionContext()
                .submit(BasicMachineDetails.taskForSshMachineLocation(machine))
                .getUnchecked();
        OsDetails osDetails = machineDetails.getOsDetails();
        String osName = osDetails.getName();
        assertTrue(osName != null && osName.toLowerCase().contains(expectedNamePart), "osDetails="+osDetails);
    }
    
    protected SshMachineLocation newContainerMachine(KubernetesLocation loc, Map<?, ?> flags) throws Exception {
        MachineLocation result = loc.obtain(flags);
        machines.add(result);
        return (SshMachineLocation) result;
    }
}
