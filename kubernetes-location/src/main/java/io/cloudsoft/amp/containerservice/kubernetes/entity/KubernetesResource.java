package io.cloudsoft.amp.containerservice.kubernetes.entity;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.util.core.ResourcePredicates;

@ImplementedBy(KubernetesResourceImpl.class)
public interface KubernetesResource extends SoftwareProcess {

    ConfigKey<String> RESOURCE_FILE = ConfigKeys.builder(String.class)
                                             .name("resource")
                                             .description("Kubernetes resource YAML file URI")
                                             .constraint(ResourcePredicates.urlExists())
                                             .build();

    AttributeSensor<String> RESOURCE_TYPE = Sensors.builder(String.class, "kubernetes.resource.type")
                                             .description("Kubernetes resource type")
                                             .build();

    AttributeSensor<String> RESOURCE_NAME = Sensors.builder(String.class, "kubernetes.resource.name")
                                             .description("Kubernetes resource name")
                                             .build();

}
