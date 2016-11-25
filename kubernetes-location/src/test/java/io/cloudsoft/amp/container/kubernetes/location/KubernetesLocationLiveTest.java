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
/**
 * Live tests for deploying simple containers. Particularly useful during dev, but not so useful
 * after that (because assumes the existence of a kubernetes endpoint). It needs configured with 
 * something like:
 * 
 *   {@code -Dtest.amp.kubernetes.endpoint=http://10.104.2.206:8080}).
 * 
 * The QA Framework is more important for that - hence these tests (trying to be) kept simple 
 * and focused.
 */
public class KubernetesLocationLiveTest extends BrooklynAppLiveTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLocationLiveTest.class);
    
    public static final String KUBERNETES_ENDPOINT = System.getProperty("test.amp.kubernetes.endpoint", "https://192.168.99.100:8443/");
    public static final String IDENTITY = System.getProperty("test.amp.kubernetes.identity", "");
    public static final String CREDENTIAL = System.getProperty("test.amp.kubernetes.credential", "");

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
    public void testDefault() throws Exception {
        // Default is "cloudsoft/centos:7"
        runImage(ImmutableMap.<String, Object>of(), "centos", "7");
    }

    @Test(groups={"Live"})
    public void testCloudsoftCentos7() throws Exception {
        runImage(ImmutableMap.of(KubernetesLocationConfig.IMAGE.getName(), "cloudsoft/centos:7"), "centos", "7");
    }

    @Test(groups={"Live"})
    public void testCloudsoftUbuntu14() throws Exception {
        runImage(ImmutableMap.of(KubernetesLocationConfig.IMAGE.getName(), "cloudsoft/ubuntu:14.04"), "ubuntu", "14.04");
    }

    @Test(groups={"Live"})
    public void testCloudsoftUbuntu16() throws Exception {
        runImage(ImmutableMap.of(KubernetesLocationConfig.IMAGE.getName(), "cloudsoft/ubuntu:16.04"), "ubuntu", "16.04");
    }

    protected void runImage(Map<String, ?> config, String expectedOs, String expectedVersion) throws Exception {
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .putAll(config)
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());

        assertTrue(machine.isSshable(), "not sshable machine="+machine);
        assertOsNameContains(machine, expectedOs, expectedVersion);
        assertMachinePasswordSecure(machine);
    }

    @Test(groups={"Live"})
    protected void testUsesSuppliedLoginPassword() throws Exception {
        // Because defaulting to "cloudsoft/centos:7", it knows to set the loginUserPassword
        // on container creation.
        String password = "myCustomP4ssword";
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .put(KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName(), password)
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());

        assertTrue(machine.isSshable(), "not sshable machine="+machine);
        assertEquals(machine.config().get(SshMachineLocation.PASSWORD), password);
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

    protected void assertOsNameContains(SshMachineLocation machine, String expectedNamePart, String expectedVersionPart) {
        MachineDetails machineDetails = app.getExecutionContext()
                .submit(BasicMachineDetails.taskForSshMachineLocation(machine))
                .getUnchecked();
        OsDetails osDetails = machineDetails.getOsDetails();
        String osName = osDetails.getName();
        String osVersion = osDetails.getVersion();
        assertTrue(osName != null && osName.toLowerCase().contains(expectedNamePart), "osDetails="+osDetails);
        assertTrue(osVersion != null && osVersion.toLowerCase().contains(expectedVersionPart), "osDetails="+osDetails);
    }
    
    protected SshMachineLocation newContainerMachine(KubernetesLocation loc, Map<?, ?> flags) throws Exception {
        MachineLocation result = loc.obtain(flags);
        machines.add(result);
        return (SshMachineLocation) result;
    }
    
    protected void assertMachinePasswordSecure(SshMachineLocation machine) {
        String password = machine.config().get(SshMachineLocation.PASSWORD);
        assertTrue(password.length() > 10, "password="+password);
        boolean hasUpper = false;
        boolean hasLower = false;
        boolean hasNonAlphabetic = false;
        for (char c : password.toCharArray()) {
            if (Character.isUpperCase(c)) hasUpper = true;
            if (Character.isLowerCase(c)) hasLower = true;
            if (!Character.isAlphabetic(c)) hasNonAlphabetic = true;
        }
        assertTrue(hasUpper && hasLower && hasNonAlphabetic, "password="+password);
    }
}
