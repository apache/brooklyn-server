package io.cloudsoft.amp.containerservice.kubernetes.entity;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.util.core.ResourcePredicates;

@ImplementedBy(KubernetesResourceImpl.class)
public interface KubernetesResource extends BasicStartable {

    ConfigKey<String> RESOURCE_FILE = ConfigKeys.builder(String.class)
                                             .name("resource")
                                             .description("Kubernetes resource YAML file URI")
                                             .constraint(ResourcePredicates.urlExists())
                                             .build();

}
