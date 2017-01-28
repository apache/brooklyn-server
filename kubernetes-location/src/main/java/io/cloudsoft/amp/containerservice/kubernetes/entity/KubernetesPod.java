package io.cloudsoft.amp.containerservice.kubernetes.entity;

import org.apache.brooklyn.api.entity.ImplementedBy;

import io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer;
import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesLocationConfig;

@ImplementedBy(KubernetesPodImpl.class)
public interface KubernetesPod extends DockerContainer, KubernetesLocationConfig {
}
