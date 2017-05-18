package org.apache.brooklyn.container.location.docker;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.location.jclouds.BasicJcloudsLocationCustomizer;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.jclouds.JcloudsLocationCustomizer;
import org.apache.brooklyn.location.jclouds.JcloudsSshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.os.Os;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.docker.compute.options.DockerTemplateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * TODO For these tests to pass, they need on the classpath the patch file(s) from AMP.
 * 
 * Assumes that a pre-existing swarm endpoint is available. See system properties and the defaults
 * below.
 */
public class DockerJcloudsLocationLiveTest extends BrooklynAppLiveTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(DockerJcloudsLocationLiveTest.class);
    
    private static final String SWARM_ENDPOINT = System.getProperty("test.amp.docker.swarmEndpoint", "https://10.104.0.162:3376/");
    private static final String IDENTITY_FILE_PATH = System.getProperty("test.amp.docker.identity", Os.tidyPath("~/.docker/.certs/cert.pem"));
    private static final String CREDENTIAL_FILE_PATH = System.getProperty("test.amp.docker.credential", Os.tidyPath("~/.docker/.certs/key.pem"));
    private static final String SWARM_NETWORK_NAME = System.getProperty("test.amp.docker.networkName", Os.tidyPath("brooklyn"));
    
    protected DockerJcloudsLocation loc;
    protected List<MachineLocation> machines;
    protected DockerTemplateOptions templateOptions;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        machines = Lists.newCopyOnWriteArrayList();
    }
    
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
    
    protected DockerJcloudsLocation newDockerLocation(Map<String, ?> flags) throws Exception {
        JcloudsLocationCustomizer locationCustomizer = new BasicJcloudsLocationCustomizer() {
            @Override
            public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
                DockerJcloudsLocationLiveTest.this.templateOptions = (DockerTemplateOptions) templateOptions;
            }
        };
        Map<String, ?> templateOptionsOverrides = (Map<String, ?>) flags.get(JcloudsLocation.TEMPLATE_OPTIONS.getName());
        Map<String,?> templateOptions = MutableMap.<String, Object>builder()
                .put("networkMode", SWARM_NETWORK_NAME)
                .putAll(templateOptionsOverrides != null ? templateOptionsOverrides : ImmutableMap.<String, Object>of())
                .build();
        Map<String,?> allFlags = MutableMap.<String,Object>builder()
                .put("identity", IDENTITY_FILE_PATH)
                .put("credential", CREDENTIAL_FILE_PATH)
                .put("endpoint", SWARM_ENDPOINT)
                .put("tags", ImmutableList.of(getClass().getName()))
                .put(JcloudsLocation.WAIT_FOR_SSHABLE.getName(), false)
                .put(JcloudsLocation.JCLOUDS_LOCATION_CUSTOMIZERS.getName(), ImmutableList.of(locationCustomizer))
                .putAll(flags)
                .put(JcloudsLocation.TEMPLATE_OPTIONS.getName(), templateOptions)
                .build();
        return (DockerJcloudsLocation) mgmt.getLocationRegistry().getLocationManaged("docker", allFlags);
    }
    
    private JcloudsSshMachineLocation newDockerMachine(DockerJcloudsLocation loc, Map<?, ?> flags) throws Exception {
        MachineLocation result = loc.obtain(flags);
        machines.add(result);
        return (JcloudsSshMachineLocation) result;
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testDefaultImageHasAutoGeneratedCredentials() throws Exception {
        loc = newDockerLocation(ImmutableMap.<String, Object>of());
        JcloudsSshMachineLocation machine = newDockerMachine(loc, ImmutableMap.<String, Object>of(
                JcloudsLocation.WAIT_FOR_SSHABLE.getName(), "1m"));
        
        assertMachineSshableSecureAndFromImage(machine, "cloudsoft/centos:7");
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testExplicitCredentialsNotOverwritten() throws Exception {
        loc = newDockerLocation(ImmutableMap.<String, Object>of());
        JcloudsSshMachineLocation machine = newDockerMachine(loc, MutableMap.of(
                JcloudsLocationConfig.LOGIN_USER, "myuser",
                JcloudsLocationConfig.LOGIN_USER_PASSWORD, "mypassword"));
        Image image = getOptionalImage(machine).get();
        assertEquals(image.getDescription(), "cloudsoft/centos:7");
        assertEquals(templateOptions.getLoginUser(), "myuser");
        assertEquals(templateOptions.getLoginPassword(), "mypassword");
        assertEquals(templateOptions.getLoginPassword(), "mypassword");
        assertEnvNotContainsKey(templateOptions, "CLOUDSOFT_ROOT_PASSWORD");
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testExplicitImageIdNotOverwritten() throws Exception {
        // TODO This id will likely change sometimes; once CCS-29 is done, then use an image name.
        // Assumes we have executed:
        //     docker run ubuntu /bin/echo 'Hello world'
        // which will have downloaded the ubuntu image with the given id.
        String imageId = "sha256:2fa927b5cdd31cdec0027ff4f45ef4343795c7a2d19a9af4f32425132a222330";
        loc = newDockerLocation(ImmutableMap.<String, Object>of());
        JcloudsSshMachineLocation machine = newDockerMachine(loc, MutableMap.of(
                JcloudsLocation.IMAGE_ID, imageId,
                JcloudsLocation.TEMPLATE_OPTIONS, ImmutableMap.of(
                        "entrypoint", ImmutableList.of("/bin/sleep", "1000"))));
        Image image = getOptionalImage(machine).get();
        assertEquals(image.getId(), imageId);
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testMatchingImageDescriptionHasAutoGeneratedCredentials() throws Exception {
        loc = newDockerLocation(ImmutableMap.<String, Object>of());
        JcloudsSshMachineLocation machine = newDockerMachine(loc, ImmutableMap.<String, Object>of(
                JcloudsLocation.IMAGE_DESCRIPTION_REGEX.getName(), "cloudsoft/centos:7",
                JcloudsLocation.WAIT_FOR_SSHABLE.getName(), "1m"));
        
        assertTrue(machine.isSshable(), "machine="+machine);
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testMatchingOsFamilyCentosHasAutoGeneratedCredentials() throws Exception {
        loc = newDockerLocation(ImmutableMap.<String, Object>of());
        JcloudsSshMachineLocation machine = newDockerMachine(loc, ImmutableMap.<String, Object>of(
                JcloudsLocation.OS_FAMILY.getName(), OsFamily.CENTOS,
                JcloudsLocation.OS_VERSION_REGEX.getName(), "7.*",
                JcloudsLocation.WAIT_FOR_SSHABLE.getName(), "1m"));
        
        assertMachineSshableSecureAndFromImage(machine, "cloudsoft/centos:7");
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testMatchingOsFamilyUbuntu14HasAutoGeneratedCredentials() throws Exception {
        loc = newDockerLocation(ImmutableMap.<String, Object>of());
        JcloudsSshMachineLocation machine = newDockerMachine(loc, ImmutableMap.<String, Object>of(
                JcloudsLocation.OS_FAMILY.getName(), OsFamily.UBUNTU,
                JcloudsLocation.OS_VERSION_REGEX.getName(), "14.04.*",
                JcloudsLocation.WAIT_FOR_SSHABLE.getName(), "1m"));
        
        assertMachineSshableSecureAndFromImage(machine, "cloudsoft/ubuntu:14.04");
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testMatchingOsFamilyUbuntu16HasAutoGeneratedCredentials() throws Exception {
        loc = newDockerLocation(ImmutableMap.<String, Object>of());
        JcloudsSshMachineLocation machine = newDockerMachine(loc, ImmutableMap.<String, Object>of(
                JcloudsLocation.OS_FAMILY.getName(), OsFamily.UBUNTU,
                JcloudsLocation.OS_VERSION_REGEX.getName(), "16.04.*",
                JcloudsLocation.WAIT_FOR_SSHABLE.getName(), "1m"));
        
        assertMachineSshableSecureAndFromImage(machine, "cloudsoft/ubuntu:16.04");
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testMatchingOsFamilyConfiguredOnLocationHasAutoGeneratedCredentials() throws Exception {
        loc = newDockerLocation(ImmutableMap.<String, Object>of(
                JcloudsLocation.OS_FAMILY.getName(), OsFamily.UBUNTU,
                JcloudsLocation.OS_VERSION_REGEX.getName(), "16.04.*",
                JcloudsLocation.WAIT_FOR_SSHABLE.getName(), "1m"));
        JcloudsSshMachineLocation machine = newDockerMachine(loc, ImmutableMap.<String, Object>of());
        
        assertMachineSshableSecureAndFromImage(machine, "cloudsoft/ubuntu:16.04");
    }
    
    protected void assertMachineSshableSecureAndFromImage(JcloudsSshMachineLocation machine, String expectedImageDescription) throws Exception {
        Image image = getOptionalImage(machine).get();
        assertEquals(image.getDescription(), expectedImageDescription);
        assertEquals(templateOptions.getLoginUser(), "root");
        assertEnvContainsKeyValue(templateOptions, "CLOUDSOFT_ROOT_PASSWORD", templateOptions.getLoginPassword());
        assertPasswordIsSecure(templateOptions.getLoginPassword());
        
        assertTrue(machine.isSshable(), "machine="+machine);
    }
    
    protected void assertEnvNotContainsKey(DockerTemplateOptions templateOptions, String key) {
        List<String> env = templateOptions.getEnv();
        if (env == null) return;
        for (String keyval : env) {
            if (keyval.startsWith(key+"=")) {
                fail("has key "+key+"; env="+env);
            }
        }
    }
    
    protected void assertEnvContainsKeyValue(DockerTemplateOptions templateOptions, String key, String value) {
        String keyval = key+"="+value;
        List<String> env = templateOptions.getEnv();
        if (env == null) {
            fail("env is null; does not contain "+keyval);
        }
        if (!env.contains(keyval)) {
            fail("env does not contain "+keyval+"; env="+env);
        }
    }
    
    protected void assertPasswordIsSecure(String val) {
        if (!val.matches(".*[0-9].*")) {
            fail("Password '"+val+"' does not contain a digit");
        }
        if (!val.matches(".*[A-Z].*")) {
            fail("Password '"+val+"' does not contain an upper-case letter");
        }
        if (val.trim().length() < 7) {
            fail("Password '"+val+"' is too short");
        }
        
        LOG.debug("Password '"+val+"' passes basic security check");
    }
    
    @SuppressWarnings("unchecked")
    protected Optional<Image> getOptionalImage(JcloudsSshMachineLocation machine) throws Exception {
        Method method = machine.getClass().getDeclaredMethod("getOptionalImage");
        method.setAccessible(true);
        Optional<Image> result = (Optional<Image>) method.invoke(machine);
        return checkNotNull(result, "null must not be returned by getOptionalImage, for %s", machine);
    }
}
