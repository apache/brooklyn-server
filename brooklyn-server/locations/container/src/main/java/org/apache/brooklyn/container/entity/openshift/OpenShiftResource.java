package org.apache.brooklyn.container.entity.openshift;

import org.apache.brooklyn.api.entity.ImplementedBy;

import org.apache.brooklyn.container.entity.kubernetes.KubernetesResource;

@ImplementedBy(OpenShiftResourceImpl.class)
public interface OpenShiftResource extends KubernetesResource {

    String DEPLOYMENT_CONFIG = "DeploymentConfig";
    String PROJECT = "Project";
    String TEMPLATE = "Template";
    String BUILD_CONFIG = "BuildConfig";

}
