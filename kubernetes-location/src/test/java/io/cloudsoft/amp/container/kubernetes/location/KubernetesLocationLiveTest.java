package io.cloudsoft.amp.container.kubernetes.location;

import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.location.BasicMachineDetails;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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
    
    private SshMachineLocation newContainerMachine(KubernetesLocation loc, Map<?, ?> flags) throws Exception {
        MachineLocation result = loc.obtain(flags);
        machines.add(result);
        return (SshMachineLocation) result;
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testDefaultImage() throws Exception {
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>of());

        MachineDetails machineDetails = app.getExecutionContext()
                .submit(BasicMachineDetails.taskForSshMachineLocation(machine))
                .getUnchecked();
        String imageName = machineDetails.getOsDetails().getName();
        assertTrue("ubuntu".equalsIgnoreCase(imageName));
    }
    
}
