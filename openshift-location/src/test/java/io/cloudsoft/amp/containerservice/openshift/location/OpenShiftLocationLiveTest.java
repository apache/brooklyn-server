package io.cloudsoft.amp.containerservice.openshift.location;

import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.os.Os;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesLocationLiveTest;

/**
 * Tests deploying containers via the {@code openshift"} location, to an OpenShift endpoint. 
 * By extending {@link KubernetesLocationLiveTest}, we get all the k8s tests.
 * 
 * It needs configured with something like:
 * 
 * <pre>
 * {@code
 * -Dtest.amp.openshift.endpoint=https://192.168.99.100:8443/
 * -Dtest.amp.openshift.certsBaseDir=~/repos/grkvlt/40bdf09b09d5896e19a9d287f41d39bb
 * }
 * </pre>
 */
public class OpenShiftLocationLiveTest extends KubernetesLocationLiveTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(OpenShiftLocationLiveTest.class);

    public static final String OPENSHIFT_ENDPOINT = System.getProperty("test.amp.openshift.endpoint", "");
    public static final String CERTS_BASE_DIR = System.getProperty("test.amp.openshift.certsBaseDir", Os.mergePaths(System.getProperty("user.home"), "openshift-certs"));
    public static final String CA_CERT_FILE = System.getProperty("test.amp.openshift.caCert", Os.mergePaths(CERTS_BASE_DIR, "ca.crt"));
    public static final String CLIENT_CERT_FILE = System.getProperty("test.amp.openshift.clientCert", Os.mergePaths(CERTS_BASE_DIR, "admin.crt"));
    public static final String CLIENT_KEY_FILE = System.getProperty("test.amp.openshift.clientKey", Os.mergePaths(CERTS_BASE_DIR, "admin.key"));
    public static final String NAMESPACE = System.getProperty("test.amp.openshift.namespace", "");

    @Override
    protected OpenShiftLocation newKubernetesLocation(Map<String, ?> flags) throws Exception {
        Map<String,?> allFlags = MutableMap.<String,Object>builder()
                .put("endpoint", OPENSHIFT_ENDPOINT)
                .put("caCert", CA_CERT_FILE)
                .put("clientCert", CLIENT_CERT_FILE)
                .put("clientKey", CLIENT_KEY_FILE)
                .put("kubernetes.namespace", NAMESPACE)
                .put("kubernetes.privileged", "true")
                .putAll(flags)
                .build();
        return (OpenShiftLocation) mgmt.getLocationRegistry().getLocationManaged("openshift", allFlags);
    }
}
