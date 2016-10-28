package io.cloudsoft.amp.container.kubernetes.location;

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;

public class OpenShiftLocation extends KubernetesLocation implements OpenShiftLocationConfig {

    public static final Logger log = LoggerFactory.getLogger(OpenShiftLocation.class);

    private KubernetesClient client;

    public OpenShiftLocation() {
        super();
    }

    public OpenShiftLocation(Map<?, ?> properties) {
        super(properties);
    }

    protected KubernetesClient getClient(ConfigBag config) {
        if (client == null) {
            KubernetesClientRegistry registry = getConfig(OPENSHIFT_CLIENT_REGISTRY);
            client = registry.getKubernetesClient(ResolvingConfigBag.newInstanceExtending(getManagementContext(), config));
        }
        return client;
    }

}
