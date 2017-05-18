/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.container.location.openshift;

import com.google.common.collect.ImmutableSet;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigBuilder;
import io.fabric8.openshift.api.model.DeploymentConfigStatus;
import io.fabric8.openshift.api.model.Project;
import io.fabric8.openshift.api.model.ProjectBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.container.entity.openshift.OpenShiftPod;
import org.apache.brooklyn.container.entity.openshift.OpenShiftResource;
import org.apache.brooklyn.container.location.kubernetes.KubernetesClientRegistry;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocation;
import org.apache.brooklyn.container.location.kubernetes.machine.KubernetesMachineLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.apache.brooklyn.util.net.Networking;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;

public class OpenShiftLocation extends KubernetesLocation implements OpenShiftLocationConfig {

    public static final String OPENSHIFT_GENERATED_BY = "openshift.io/generated-by";
    private static final Logger LOG = LoggerFactory.getLogger(OpenShiftLocation.class);
    private OpenShiftClient client;

    public OpenShiftLocation() {
        super();
    }

    public OpenShiftLocation(Map<?, ?> properties) {
        super(properties);
    }

    @Override
    protected KubernetesClient getClient(ConfigBag config) {
        if (client == null) {
            KubernetesClientRegistry registry = getConfig(OPENSHIFT_CLIENT_REGISTRY);
            client = (OpenShiftClient) registry.getKubernetesClient(ResolvingConfigBag.newInstanceExtending(getManagementContext(), config));
        }
        return client;
    }

    @Override
    protected boolean handleResourceDelete(String resourceType, String resourceName, String namespace) {
        if (super.handleResourceDelete(resourceType, resourceName, namespace)) {
            return true;
        }

        try {
            switch (resourceType) {
                case OpenShiftResource.DEPLOYMENT_CONFIG:
                    return client.deploymentConfigs().inNamespace(namespace).withName(resourceName).delete();
                case OpenShiftResource.PROJECT:
                    return client.projects().withName(resourceName).delete();
                case OpenShiftResource.TEMPLATE:
                    return client.templates().inNamespace(namespace).withName(resourceName).delete();
                case OpenShiftResource.BUILD_CONFIG:
                    return client.buildConfigs().inNamespace(namespace).withName(resourceName).delete();
            }
        } catch (KubernetesClientException kce) {
            LOG.warn("Error deleting resource {}: {}", resourceName, kce);
        }
        return false;
    }

    @Override
    protected boolean findResourceAddress(LocationSpec<? extends KubernetesMachineLocation> locationSpec, Entity entity, HasMetadata metadata, String resourceType, String resourceName, String namespace) {
        if (super.findResourceAddress(locationSpec, entity, metadata, resourceType, resourceName, namespace)) {
            return true;
        }

        if (resourceType.equals(OpenShiftResource.DEPLOYMENT_CONFIG)) {
            DeploymentConfig deploymentConfig = (DeploymentConfig) metadata;
            Map<String, String> labels = deploymentConfig.getSpec().getTemplate().getMetadata().getLabels();
            Pod pod = getPod(namespace, labels);
            entity.sensors().set(OpenShiftPod.KUBERNETES_POD, pod.getMetadata().getName());

            InetAddress node = Networking.getInetAddressWithFixedName(pod.getSpec().getNodeName());
            String podAddress = pod.getStatus().getPodIP();

            locationSpec.configure("address", node);
            locationSpec.configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.of(podAddress));

            return true;
        } else {
            return false;
        }
    }

    @Override
    protected synchronized Namespace createOrGetNamespace(final String name, Boolean create) {
        Project project = client.projects().withName(name).get();
        ExitCondition projectReady = new ExitCondition() {
            @Override
            public Boolean call() {
                Project actualProject = client.projects().withName(name).get();
                return actualProject != null && actualProject.getStatus().getPhase().equals(PHASE_ACTIVE);
            }

            @Override
            public String getFailureMessage() {
                Project actualProject = client.projects().withName(name).get();
                return "Project for " + name + " " + (actualProject == null ? "absent" : " status " + actualProject.getStatus());
            }
        };
        if (project != null) {
            LOG.debug("Found project {}, returning it.", project);
        } else if (create) {
            project = client.projects().create(new ProjectBuilder().withNewMetadata().withName(name).endMetadata().build());
            LOG.debug("Created project {}.", project);
        } else {
            throw new IllegalStateException("Project " + name + " does not exist and namespace.create is not set");
        }
        waitForExitCondition(projectReady);
        return client.namespaces().withName(name).get();
    }

    @Override
    protected synchronized void deleteEmptyNamespace(final String name) {
        if (!name.equals("default") && isNamespaceEmpty(name)) {
            if (client.projects().withName(name).get() != null &&
                    !client.projects().withName(name).get().getStatus().getPhase().equals(PHASE_TERMINATING)) {
                client.projects().withName(name).delete();
                ExitCondition exitCondition = new ExitCondition() {
                    @Override
                    public Boolean call() {
                        return client.projects().withName(name).get() == null;
                    }

                    @Override
                    public String getFailureMessage() {
                        return "Project " + name + " still present";
                    }
                };
                waitForExitCondition(exitCondition);
            }
        }
    }

    @Override
    protected boolean isNamespaceEmpty(String namespace) {
        return client.deploymentConfigs().inNamespace(namespace).list().getItems().isEmpty() &&
                client.services().inNamespace(namespace).list().getItems().isEmpty() &&
                client.secrets().inNamespace(namespace).list().getItems().isEmpty();
    }

    @Override
    protected void deploy(final String namespace, Entity entity, Map<String, String> metadata, final String deploymentName, Container container, final Integer replicas, Map<String, String> secrets) {
        PodTemplateSpecBuilder podTemplateSpecBuilder = new PodTemplateSpecBuilder()
                .withNewMetadata()
                .addToLabels("name", deploymentName)
                .addToLabels(metadata)
                .endMetadata()
                .withNewSpec()
                .addToContainers(container)
                .endSpec();
        if (secrets != null) {
            for (String secretName : secrets.keySet()) {
                podTemplateSpecBuilder.withNewSpec()
                        .addToContainers(container)
                        .addNewImagePullSecret(secretName)
                        .endSpec();
            }
        }
        PodTemplateSpec template = podTemplateSpecBuilder.build();
        DeploymentConfig deployment = new DeploymentConfigBuilder()
                .withNewMetadata()
                .withName(deploymentName)
                .addToAnnotations(OPENSHIFT_GENERATED_BY, "AMP")
                .addToAnnotations(CLOUDSOFT_ENTITY_ID, entity.getId())
                .addToAnnotations(CLOUDSOFT_APPLICATION_ID, entity.getApplicationId())
                .endMetadata()
                .withNewSpec()
                .withNewStrategy()
                .withType("Recreate")
                .endStrategy()
                .addNewTrigger()
                .withType("ConfigChange")
                .endTrigger()
                .withReplicas(replicas)
                .addToSelector("name", deploymentName)
                .withTemplate(template)
                .endSpec()
                .build();
        client.deploymentConfigs().inNamespace(namespace).create(deployment);
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                DeploymentConfig dc = client.deploymentConfigs().inNamespace(namespace).withName(deploymentName).get();
                DeploymentConfigStatus status = (dc == null) ? null : dc.getStatus();
                Integer replicas = (status == null) ? null : status.getAvailableReplicas();
                return replicas != null && replicas.intValue() == replicas;
            }

            @Override
            public String getFailureMessage() {
                DeploymentConfig dc = client.deploymentConfigs().inNamespace(namespace).withName(deploymentName).get();
                DeploymentConfigStatus status = (dc == null) ? null : dc.getStatus();
                return "Namespace=" + namespace + "; deploymentName= " + deploymentName + "; Deployment=" + dc + "; status=" + status;
            }
        };
        waitForExitCondition(exitCondition);
        LOG.debug("Deployed {} to namespace {}.", deployment, namespace);
    }

    @Override
    protected String getContainerResourceType() {
        return OpenShiftResource.DEPLOYMENT_CONFIG;
    }

    @Override
    protected void undeploy(final String namespace, final String deployment, final String pod) {
        client.deploymentConfigs().inNamespace(namespace).withName(deployment).delete();
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                return client.deploymentConfigs().inNamespace(namespace).withName(deployment).get() == null;
            }

            @Override
            public String getFailureMessage() {
                return "No deployment with namespace=" + namespace + ", deployment=" + deployment;
            }
        };
        waitForExitCondition(exitCondition);
    }

}
