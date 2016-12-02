package io.cloudsoft.amp.container.openshift.location;

import java.util.Map;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesClientRegistry;
import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesLocation;
import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesLocation.ExitCondition;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
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
    protected synchronized Namespace createOrGetNamespace(final String namespace) {
        Project project = client.projects().withName(namespace).get();
        ExitCondition projectReady = new ExitCondition() {
            @Override
            public Boolean call() {
                Project actualProject = client.projects().withName(namespace).get();
                return actualProject != null && actualProject.getStatus().getPhase().equals("Active");
            }
            @Override
            public String getFailureMessage() {
                Project actualProject = client.projects().withName(namespace).get();
                return "Project for " + namespace + " " + (actualProject == null ? "absent" : " status " + actualProject.getStatus());
            }
        };
        if (project != null) {
            log.debug("Found project {}, returning it.", project);
        } else {
            project = client.projects().create(new ProjectBuilder().withNewMetadata().withName(namespace).endMetadata().build());
            log.debug("Created project {}.", project);
        }
        waitForExitCondition(projectReady);
        return client.namespaces().withName(namespace).get();
    }

    @Override
    protected synchronized void deleteNamespace(final String namespace) {
        if (!namespace.equals("default") && isNamespaceEmpty(namespace)) {
            if (client.projects().withName(namespace).get() != null && !client.projects().withName(namespace).get().getStatus().getPhase().equals("Terminating")) {
                client.projects().withName(namespace).delete();
                ExitCondition exitCondition = new ExitCondition() {
                    @Override
                    public Boolean call() {
                        return client.projects().withName(namespace).get() == null;
                    }
                    @Override
                    public String getFailureMessage() {
                        return "Project " + namespace + " still present";
                    }
                };
                waitForExitCondition(exitCondition);
            }
        }
    }

    @Override
    protected boolean isNamespaceEmpty(String namespace) {
        return client.extensions().deployments().inNamespace(namespace).list().getItems().isEmpty() &&
               client.services().inNamespace(namespace).list().getItems().isEmpty() &&
               client.secrets().inNamespace(namespace).list().getItems().isEmpty();
    }

}
