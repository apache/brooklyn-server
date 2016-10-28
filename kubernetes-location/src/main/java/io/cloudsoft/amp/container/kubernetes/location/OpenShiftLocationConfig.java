package io.cloudsoft.amp.container.kubernetes.location;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

public interface OpenShiftLocationConfig {

    @SetFromFlag("openShiftUrl")
    ConfigKey<String> OPENSHIFT_URL = ConfigKeys.newStringConfigKey("openShiftUrl");

    ConfigKey<KubernetesClientRegistry> OPENSHIFT_CLIENT_REGISTRY = ConfigKeys.newConfigKey(
            KubernetesClientRegistry.class, "openShiftClientRegistry",
            "Registry/Factory for creating OpenShift client; default is almost always fine, " +
                    "except where tests want to customize behaviour", OpenShiftClientRegistryImpl.INSTANCE);
}

