package io.cloudsoft.amp.containerservice.kubernetes.entity;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer;
import io.cloudsoft.amp.containerservice.kubernetes.location.KubernetesLocationConfig;

@ImplementedBy(KubernetesPodImpl.class)
public interface KubernetesPod extends DockerContainer {

    ConfigKey<String> NAMESPACE = KubernetesLocationConfig.NAMESPACE;

    ConfigKey<String> POD = ConfigKeys.builder(String.class)
            .name("pod")
            .description("The name of the pod")
            .constraint(Predicates.<String>notNull())
            .build();

    @SuppressWarnings("serial")
    ConfigKey<List<String>> PERSISTENT_VOLUMES = ConfigKeys.builder(new TypeToken<List<String>>() {})
            .name("persistentVolumes")
            .description("Persistent volumes used by the pod")
            .constraint(Predicates.<List<String>>notNull())
            .build();

    ConfigKey<String> DEPLOYMENT = ConfigKeys.builder(String.class)
            .name("deployment")
            .description("The name of the service the deployed pod will use")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<Integer> REPLICAS = ConfigKeys.builder(Integer.class)
            .name("replicas")
            .description("Number of replicas in the pod")
            .constraint(Predicates.notNull())
            .defaultValue(1)
            .build();

    @SuppressWarnings("serial")
    ConfigKey<Map<String, String>> SECRETS = ConfigKeys.builder(new TypeToken<Map<String, String>>() {})
            .name("secrets")
            .description("Secrets to be added to the pod")
            .build();

    @SuppressWarnings("serial")
    ConfigKey<Map<String, String>> LIMITS = ConfigKeys.builder(new TypeToken<Map<String, String>>() {})
            .name("limits")
            .description("Container resource limits for the pod")
            .build();

    ConfigKey<Boolean> PRIVILEGED = ConfigKeys.builder(Boolean.class)
            .name("privileged")
            .description("Whether the container is privileged")
            .defaultValue(false)
            .build();

    MapConfigKey<Object> METADATA = new MapConfigKey.Builder<Object>(Object.class, "metadata")
            .description("Metadata to set on the pod")
            .defaultValue(ImmutableMap.<String, Object>of())
            .typeInheritance(BasicConfigInheritance.DEEP_MERGE)
            .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED_ELSE_DEEP_MERGE)
            .build();

    AttributeSensor<String> KUBERNETES_DEPLOYMENT = Sensors.builder(String.class, "kubernetes.deployment")
            .description("Deployment resources run in")
            .build();

    AttributeSensor<String> KUBERNETES_NAMESPACE = Sensors.builder(String.class, "kubernetes.namespace")
            .description("Namespace that resources run in")
            .build();

    AttributeSensor<String> KUBERNETES_SERVICE = Sensors.builder(String.class, "kubernetes.service")
            .description("Service that exposes the deployment")
            .build();

    AttributeSensor<String> KUBERNETES_POD = Sensors.builder(String.class, "kubernetes.pod")
            .description("Pod running the deployment")
            .build();
}
