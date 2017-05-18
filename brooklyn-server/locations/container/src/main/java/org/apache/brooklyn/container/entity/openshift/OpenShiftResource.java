package io.cloudsoft.amp.containerservice.openshift.entity;

import org.apache.brooklyn.api.entity.ImplementedBy;

import io.cloudsoft.amp.containerservice.kubernetes.entity.KubernetesResource;

@ImplementedBy(OpenShiftResourceImpl.class)
public interface OpenShiftResource extends KubernetesResource {

    String DEPLOYMENT_CONFIG = "DeploymentConfig";
    String PROJECT = "Project";
    String TEMPLATE = "Template";
    String BUILD_CONFIG = "BuildConfig";

}
