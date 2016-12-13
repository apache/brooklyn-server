package io.cloudsoft.amp.containerservice.openshift.location;

import java.util.Map;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesClientRegistry;
import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesLocation;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.ReplicationControllerList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigBuilder;
import io.fabric8.openshift.api.model.DeploymentConfigStatus;
import io.fabric8.openshift.api.model.Project;
import io.fabric8.openshift.api.model.ProjectBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

public class OpenShiftLocation extends KubernetesLocation implements OpenShiftLocationConfig {

    public static final Logger log = LoggerFactory.getLogger(OpenShiftLocation.class);

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
    protected synchronized Namespace createOrGetNamespace(final String name, Boolean create) {
        Project project = client.projects().withName(name).get();
        ExitCondition projectReady = new ExitCondition() {
            @Override
            public Boolean call() {
                Project actualProject = client.projects().withName(name).get();
                return actualProject != null && actualProject.getStatus().getPhase().equals("Active");
            }
            @Override
            public String getFailureMessage() {
                Project actualProject = client.projects().withName(name).get();
                return "Project for " + name+ " " + (actualProject == null ? "absent" : " status " + actualProject.getStatus());
            }
        };
        if (project != null) {
            log.debug("Found project {}, returning it.", project);
        } else if (create) {
            project = client.projects().create(new ProjectBuilder().withNewMetadata().withName(name).endMetadata().build());
            log.debug("Created project {}.", project);
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
                    !client.projects().withName(name).get().getStatus().getPhase().equals("Terminating")) {
                client.projects().withName(name).delete();
                ExitCondition exitCondition = new ExitCondition() {
                    @Override
                    public Boolean call() {
                        return client.projects().withName(name).get() == null;
                    }
                    @Override
                    public String getFailureMessage() {
                        return "Project " + name+ " still present";
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
    protected void deploy(final String namespace, Map<String, String> metadata, final String deploymentName, Container container, final Integer replicas, Map<String, String> secrets) {
        PodTemplateSpecBuilder podTemplateSpecBuilder = new PodTemplateSpecBuilder()
                .withNewMetadata()
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
                    .addToLabels(metadata)
                    .addToAnnotations("openshift.io/generated-by", "CloudsoftAMP")
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                    .addToSelector("name", deploymentName)
                    .withTemplate(template)
                .endSpec()
                .withNewStatus()
                    .withLatestVersion(1L)
                    .withObservedGeneration(1L)
                .endStatus()
                .build();
        client.deploymentConfigs().inNamespace(namespace).create(deployment);
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                DeploymentConfig dc = client.deploymentConfigs().inNamespace(namespace).withName(deploymentName).get();
                DeploymentConfigStatus status = (dc == null) ? null : dc.getStatus();
                return status != null;
            }
            @Override
            public String getFailureMessage() {
                DeploymentConfig dc = client.deploymentConfigs().inNamespace(namespace).withName(deploymentName).get();
                DeploymentConfigStatus status = (dc == null) ? null : dc.getStatus();
                return "Namespace=" + namespace + "; deploymentName= " + deploymentName + "; Deployment=" + dc + "; status=" + status;
            }
        };
        waitForExitCondition(exitCondition);
        log.debug("Created deployment config {} in namespace {}.", deployment, namespace);

        client.deploymentConfigs().inNamespace(namespace).withName(deploymentName).deployLatest();
        exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                DeploymentConfig dc = client.deploymentConfigs().inNamespace(namespace).withName(deploymentName).get();
                DeploymentConfigStatus status = (dc == null) ? null : dc.getStatus();
                Integer replicaStatus = (status == null) ? 0 : (Integer) status.getReplicas();
                return replicaStatus != null && replicaStatus.intValue() == replicas;
            }
            @Override
            public String getFailureMessage() {
                DeploymentConfig dc = client.deploymentConfigs().inNamespace(namespace).withName(deploymentName).get();
                DeploymentConfigStatus status = (dc == null) ? null : dc.getStatus();
                return "Namespace=" + namespace + "; deploymentName= " + deploymentName + "; Deployment=" + dc + "; status=" + status;
            }
        };
        waitForExitCondition(exitCondition);
        log.debug("Deployed {} to namespace {}.", deployment, namespace);
    }

    protected void undeploy(final String namespace, final String deployment, final String pod) {
        ReplicationControllerList controllers = client.replicationControllers().inNamespace(namespace)
                .withLabel("name", deployment)
                .list();

        client.replicationControllers().delete(controllers.getItems());

        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                return client.replicationControllers().inNamespace(namespace)
                        .withLabel("name", deployment)
                        .list().getItems().isEmpty();
            }
            @Override
            public String getFailureMessage() {
                return "No deployment with namespace=" + namespace + ", deployment=" + deployment;
            }
        };
        waitForExitCondition(exitCondition);
    }

}
