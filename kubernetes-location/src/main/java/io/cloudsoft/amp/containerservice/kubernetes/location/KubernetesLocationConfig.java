package io.cloudsoft.amp.containerservice.kubernetes.location;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

public interface KubernetesLocationConfig extends CloudLocationConfig {

    @SetFromFlag("endpoint")
    ConfigKey<String> MASTER_URL = LocationConfigKeys.CLOUD_ENDPOINT;

    ConfigKey<String> CA_CERT_DATA = ConfigKeys.builder(String.class)
            .name("caCertData")
            .description("Data for CA certificate")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CA_CERT_FILE = ConfigKeys.builder(String.class)
            .name("caCertFile")
            .description("URL of resource containing CA certificate data")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_CERT_DATA = ConfigKeys.builder(String.class)
            .name("clientCertData")
            .description("Data for client certificate")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_CERT_FILE = ConfigKeys.builder(String.class)
            .name("clientCertFile")
            .description("URL of resource containing client certificate data")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_KEY_DATA = ConfigKeys.builder(String.class)
            .name("clientKeyData")
            .description("Data for client key")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_KEY_FILE = ConfigKeys.builder(String.class)
            .name("clientKeyFile")
            .description("URL of resource containing client key data")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_KEY_ALGO = ConfigKeys.builder(String.class)
            .name("clientKeyAlgo")
            .description("Algorithm used for the client key")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_KEY_PASSPHRASE = ConfigKeys.builder(String.class)
            .name("clientKeyPassphrase")
            .description("Passphrase used for the client key")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> OAUTH_TOKEN = ConfigKeys.builder(String.class)
            .name("oauthToken")
            .description("The OAuth token data for the current user")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> NAMESPACE = ConfigKeys.builder(String.class)
            .name("namespace")
            .description("Namespace where resources will live; the default is 'amp'")
            .defaultValue("amp")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<Boolean> CREATE_NAMESPACE = ConfigKeys.builder(Boolean.class)
            .name("namespace.create")
            .description("Whether to create the namespace if it does not exist")
            .defaultValue(true)
            .constraint(Predicates.<Boolean>notNull())
            .build();

    ConfigKey<Boolean> DELETE_EMPTY_NAMESPACE = ConfigKeys.builder(Boolean.class)
            .name("namespace.deleteEmpty")
            .description("Whether to delete an empty namespace when releasing resources")
            .defaultValue(false)
            .constraint(Predicates.<Boolean>notNull())
            .build();

    @SuppressWarnings("serial")
    ConfigKey<List<String>> PERSISTENT_VOLUMES = ConfigKeys.builder(new TypeToken<List<String>>() {})
            .name("persistentVolumes")
            .description("Set up persistent volumes.")
            .constraint(Predicates.<List<String>>notNull())
            .build();

    ConfigKey<String> DEPLOYMENT = ConfigKeys.builder(String.class)
            .name("deployment")
            .description("Deployment where resources will live.")
            .constraint(Predicates.<String>notNull())
            .build();

    // TODO Move to a KubernetesSshMachineLocation?
    ConfigKey<String> SERVICE = ConfigKeys.builder(String.class)
            .name("service")
            .description("Service (auto-set on the container location) that exposes the deployment.")
            .constraint(Predicates.<String>notNull())
            .build();

    // TODO Move to a KubernetesSshMachineLocation?
    ConfigKey<String> POD = ConfigKeys.builder(String.class)
            .name("pod")
            .description("Pod (auto-set on the container location) running the deployment.")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> IMAGE = ConfigKeys.builder(String.class)
            .name("image")
            .description("Docker image to be deployed into the pod")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> OS_FAMILY = ConfigKeys.builder(String.class)
            .name("osFamily")
            .description("OS family, e.g. CentOS, Ubuntu")
            .build();
    
    ConfigKey<String> OS_VERSION_REGEX = ConfigKeys.builder(String.class)
            .name("osVersionRegex")
            .description("Regular expression for the OS version to load")
            .build();

    @SuppressWarnings("serial")
    ConfigKey<Map<String, ?>> ENV = ConfigKeys.newConfigKey(
            new TypeToken<Map<String, ?>>() {},
            "env", 
            "Environment variables to inject when starting the container", 
            ImmutableMap.<String, Object>of());

    ConfigKey<Integer> REPLICAS = ConfigKeys.builder(Integer.class)
            .name("replicas")
            .description("Number of replicas into the pod")
            .constraint(Predicates.notNull())
            .defaultValue(1)
            .build();

    @SuppressWarnings("serial")
    ConfigKey<Map<String, String>> SECRETS = ConfigKeys.builder(
            new TypeToken<Map<String, String>>() {})
            .name("secrets")
            .description("Kubernetes secrets to be added to the pod")
            .build();

    @SuppressWarnings("serial")
    ConfigKey<Map<String, String>> LIMITS = ConfigKeys.builder(
            new TypeToken<Map<String, String>>() {})
            .name("limits")
            .description("Kubernetes resource limits")
            .build();

    ConfigKey<Boolean> PRIVILEGED = ConfigKeys.builder(Boolean.class)
            .name("privileged")
            .description("Whether Kubernetes should allow privileged containers")
            .defaultValue(false)
            .build();

    ConfigKey<KubernetesClientRegistry> KUBERNETES_CLIENT_REGISTRY = ConfigKeys.builder(KubernetesClientRegistry.class) 
            .name("kubernetesClientRegistry")
            .description("Registry/Factory for creating Kubernetes client; default is almost always fine, "
                    + "except where tests want to customize behaviour") 
            .defaultValue(KubernetesClientRegistryImpl.INSTANCE)
            .build();

    ConfigKey<String> LOGIN_USER = ConfigKeys.builder(String.class)
            .name("loginUser")
            .description("Override the user who logs in initially to perform setup")
            .defaultValue("root")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> LOGIN_USER_PASSWORD = ConfigKeys.builder(String.class)
            .name("loginUser.password")
            .description("Custom password for the user who logs in initially")
            .constraint(Predicates.<String>notNull())
            .build();
    
    ConfigKey<Boolean> INJECT_LOGIN_CREDENTIAL = ConfigKeys.builder(Boolean.class)
            .name("injectLoginCredential") 
            .description("Whether to inject login credentials (if null, will infer from image choice); ignored if explicit 'loginUser.password' supplied")
            .build();
}

