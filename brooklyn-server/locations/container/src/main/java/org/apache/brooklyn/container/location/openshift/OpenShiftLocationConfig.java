package io.cloudsoft.amp.containerservice.openshift.location;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesClientRegistry;

public interface OpenShiftLocationConfig {

    ConfigKey<KubernetesClientRegistry> OPENSHIFT_CLIENT_REGISTRY = ConfigKeys.newConfigKey(
            KubernetesClientRegistry.class, "openShiftClientRegistry",
            "Registry/Factory for creating OpenShift client; default is almost always fine, " +
                    "except where tests want to customize behaviour", OpenShiftClientRegistryImpl.INSTANCE);
}

