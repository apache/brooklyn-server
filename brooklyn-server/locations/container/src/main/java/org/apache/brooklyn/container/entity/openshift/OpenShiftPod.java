package org.apache.brooklyn.container.entity.openshift;

import org.apache.brooklyn.api.entity.ImplementedBy;

import org.apache.brooklyn.container.entity.kubernetes.KubernetesPod;

@ImplementedBy(OpenShiftPodImpl.class)
public interface OpenShiftPod extends KubernetesPod {

}
