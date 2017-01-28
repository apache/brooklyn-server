package io.cloudsoft.amp.containerservice.openshift.entity;

import org.apache.brooklyn.api.entity.ImplementedBy;

import io.cloudsoft.amp.containerservice.kubernetes.entity.KubernetesPod;

@ImplementedBy(OpenShiftPodImpl.class)
public interface OpenShiftPod extends KubernetesPod {

}
