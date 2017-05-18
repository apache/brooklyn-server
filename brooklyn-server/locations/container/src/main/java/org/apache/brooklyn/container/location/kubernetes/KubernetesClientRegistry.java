package org.apache.brooklyn.container.location.kubernetes;

import org.apache.brooklyn.util.core.config.ConfigBag;

import io.fabric8.kubernetes.client.KubernetesClient;

public interface KubernetesClientRegistry {

    KubernetesClient getKubernetesClient(ConfigBag conf);

}
