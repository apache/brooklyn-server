package org.apache.brooklyn.container.location.openshift;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

import org.apache.brooklyn.container.location.kubernetes.KubernetesClientRegistry;

public interface OpenShiftLocationConfig {

    ConfigKey<KubernetesClientRegistry> OPENSHIFT_CLIENT_REGISTRY = ConfigKeys.newConfigKey(
            KubernetesClientRegistry.class, "openShiftClientRegistry",
            "Registry/Factory for creating OpenShift client; default is almost always fine, " +
                    "except where tests want to customize behaviour", OpenShiftClientRegistryImpl.INSTANCE);
}

