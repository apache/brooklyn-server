package org.apache.brooklyn.container.location.openshift;

import org.apache.brooklyn.util.core.config.ConfigBag;

import org.apache.brooklyn.container.location.kubernetes.KubernetesClientRegistryImpl;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;

public class OpenShiftClientRegistryImpl extends KubernetesClientRegistryImpl {

    public static final OpenShiftClientRegistryImpl INSTANCE = new OpenShiftClientRegistryImpl();

    /**
     * The default OpenShift URL is set using the Kubernetes
     * {@code KubernetesLocationConfig#MASTER_URL master URL} as follows:
     * <pre>
     * openShiftUrl = URLUtils.join(getMasterUrl(), "oapi", oapiVersion);
     * </pre>
     */
    @Override
    public KubernetesClient getKubernetesClient(ConfigBag conf) {
        KubernetesClient client = super.getKubernetesClient(conf);
        OpenShiftClient oClient = client.adapt(OpenShiftClient.class);
        return oClient;
    }

}
