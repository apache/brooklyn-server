package io.cloudsoft.amp.containerservice.openshift.location;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.cloudsoft.amp.containerservice.openshift.location.OpenShiftLocation;

public class OpenShiftLocationResolverTest extends BrooklynMgmtUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(OpenShiftLocationResolverTest.class);

    private BrooklynProperties brooklynProperties;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        brooklynProperties = mgmt.getBrooklynProperties();

        brooklynProperties.put("brooklyn.location.openshift.identity", "openshift-id");
        brooklynProperties.put("brooklyn.location.openshift.credential", "openshift-cred");
    }

    @Test
    public void testGivesCorrectLocationType() {
        LocationSpec<?> spec = getLocationSpec("openshift");
        assertEquals(spec.getType(), OpenShiftLocation.class);

        OpenShiftLocation loc = resolve("openshift");
        assertTrue(loc instanceof OpenShiftLocation, "loc="+loc);
    }

    @Test
    public void testParametersInSpecString() {
        OpenShiftLocation loc = resolve("openshift(endpoint=myMasterUrl)");
        assertEquals(loc.getConfig(OpenShiftLocation.MASTER_URL), "myMasterUrl");
    }

    @Test
    public void testTakesDotSeparateProperty() {
        brooklynProperties.put("brooklyn.location.openshift.endpoint", "myMasterUrl");
        OpenShiftLocation loc = resolve("openshift");
        assertEquals(loc.getConfig(OpenShiftLocation.MASTER_URL), "myMasterUrl");
    }

    @Test
    public void testPropertiesPrecedence() {
        // prefer those in "spec" over everything else
        brooklynProperties.put("brooklyn.location.named.myopenshift", "openshift:(loginUser=\"loginUser-inSpec\")");

        brooklynProperties.put("brooklyn.location.named.myopenshift.loginUser", "loginUser-inNamed");
        brooklynProperties.put("brooklyn.location.openshift.loginUser", "loginUser-inDocker");

        // prefer those in "named" over everything else
        brooklynProperties.put("brooklyn.location.named.myopenshift.privateKeyFile", "privateKeyFile-inNamed");
        brooklynProperties.put("brooklyn.location.openshift.privateKeyFile", "privateKeyFile-inDocker");

        // prefer those in openshift-specific
        brooklynProperties.put("brooklyn.location.openshift.publicKeyFile", "publicKeyFile-inDocker");

        Map<String, Object> conf = resolve("named:myopenshift").config().getBag().getAllConfig();

        assertEquals(conf.get("loginUser"), "loginUser-inSpec");
        assertEquals(conf.get("privateKeyFile"), "privateKeyFile-inNamed");
        assertEquals(conf.get("publicKeyFile"), "publicKeyFile-inDocker");
    }

    private LocationSpec<?> getLocationSpec(String spec) {
        LOG.debug("Obtaining location spec '{}'", spec);
        return mgmt.getLocationRegistry().getLocationSpec(spec).get();
    }

    private OpenShiftLocation resolve(String spec) {
        LOG.debug("Resolving location spec '{}'", spec);
        return (OpenShiftLocation) mgmt.getLocationRegistry().getLocationManaged(spec);
    }
}
