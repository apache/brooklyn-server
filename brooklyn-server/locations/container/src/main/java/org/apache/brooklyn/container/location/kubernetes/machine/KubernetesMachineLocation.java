package org.apache.brooklyn.container.location.kubernetes.machine;

import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

public interface KubernetesMachineLocation extends MachineLocation {

    ConfigKey<String> KUBERNETES_NAMESPACE = ConfigKeys.builder(String.class, "kubernetes.namespace")
            .description("Namespace for the KubernetesMachineLocation")
            .build();

    ConfigKey<String> KUBERNETES_RESOURCE_NAME = ConfigKeys.builder(String.class, "kubernetes.name")
            .description("Name of the resource represented by the KubernetesMachineLocation")
            .build();

    ConfigKey<String> KUBERNETES_RESOURCE_TYPE = ConfigKeys.builder(String.class, "kubernetes.type")
            .description("Type of the resource represented by the KubernetesMachineLocation")
            .build();

    public String getResourceName();

    public String getResourceType();

    public String getNamespace();

}
