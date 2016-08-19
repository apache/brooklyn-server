package io.cloudsoft.amp.container.kubernetes.location;

import org.apache.brooklyn.util.core.config.ConfigBag;

import io.fabric8.kubernetes.client.KubernetesClient;

public interface KubernetesClientRegistry {

    KubernetesClient getKubernetesClient(ConfigBag conf);

}
