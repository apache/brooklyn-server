package io.cloudsoft.amp.containerservice.kubernetes.location;

import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;

import io.cloudsoft.amp.containerservice.ThreadedRepeater;
import io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer;
import io.cloudsoft.amp.containerservice.dockerlocation.DockerJcloudsLocation;
import io.cloudsoft.amp.containerservice.kubernetes.entity.KubernetesResource;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.QuantityBuilder;
import io.fabric8.kubernetes.api.model.ReplicationController;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;

public class KubernetesLocation extends AbstractLocation implements MachineProvisioningLocation<MachineLocation>, KubernetesLocationConfig {

    /*
     * TODO
     *
     *  - Ignores config such as 'user' and 'password', just uses 'loginUser'
     *    and 'loginUser.password' for connecting to the container.
     *  - Does not create a user, so behaves differently from things that use
     *    JcloudsLocation.
     *  - Does not use ssh keys only passwords.
     *  - The 'cloudsoft/*' images use root which is discouraged.
     *  - Error handling needs revisited. For example, if provisioning fails then
     *    it waits for five minutes and then fails without a reason why.
     *    e.g. try launching a container with an incorrect image name.
     */

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLocation.class);

    public static final String SERVER_TYPE = "NodePort";
    public static final String IMMUTABLE_CONTAINER_KEY = "immutable-container";
    public static final String SSHABLE_CONTAINER = "sshable-container";
    public static final String CLOUDSOFT_ENTITY_ID = "cloudsoft.io/entity-id";
    public static final String CLOUDSOFT_APPLICATION_ID = "cloudsoft.io/application-id";

    /**
     * The regex for the image descriptions that support us injecting login credentials.
     */
    private static final List<String> IMAGE_DESCRIPTION_REGEXES_REQUIRING_INJECTED_LOGIN_CREDS = ImmutableList.of(
            "cloudsoft/centos.*",
            "cloudsoft/ubuntu.*");

    private KubernetesClient client;

    public KubernetesLocation() {
        super();
    }

    public KubernetesLocation(Map<?, ?> properties) {
        super(properties);
    }

    @Override
    public void init() {
        super.init();
    }

    protected KubernetesClient getClient() {
        return getClient(MutableMap.of());
    }

    protected KubernetesClient getClient(Map<?, ?> flags) {
        ConfigBag conf = (flags == null || flags.isEmpty())
                ? config().getBag()
                : ConfigBag.newInstanceExtending(config().getBag(), flags);
        return getClient(conf);
    }

    protected KubernetesClient getClient(ConfigBag config) {
        if (client == null) {
            KubernetesClientRegistry registry = getConfig(KUBERNETES_CLIENT_REGISTRY);
            client = registry.getKubernetesClient(ResolvingConfigBag.newInstanceExtending(getManagementContext(), config));
        }
        return client;
    }

    @Override
    public MachineLocation obtain(Map<?, ?> flags) throws NoMachinesAvailableException {
        ConfigBag setupRaw = ConfigBag.newInstanceExtending(config().getBag(), flags);
        ConfigBag setup = ResolvingConfigBag.newInstanceExtending(getManagementContext(), setupRaw);

        client = getClient(setup);
        Entity entity = validateCallerContext(setup);
        if (isKubernetesResource(entity)) {
            return createKubernetesResourceLocation(entity, setup);
        } else {
            return createKubernetesContainerLocation(entity, setup);
        }
    }

    @Override
    public void release(MachineLocation machine) {
        Entity entity = validateCallerContext(machine);
        if (isKubernetesResource(entity)) {
            deleteKubernetesResourceLocation(entity);
        } else {
            deleteKubernetesContainerLocation(entity, machine);
        }
    }

    protected void deleteKubernetesContainerLocation(Entity entity, MachineLocation machine) {
        final String namespace = entity.sensors().get(KUBERNETES_NAMESPACE);
        final String deployment = entity.sensors().get(KUBERNETES_DEPLOYMENT);
        final String pod = entity.sensors().get(KUBERNETES_POD);
        final String service = entity.sensors().get(KUBERNETES_SERVICE);

        undeploy(namespace, deployment, pod);

        client.services().inNamespace(namespace).withName(service).delete();
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                return client.services().inNamespace(namespace).withName(service).get() == null;
            }
            @Override
            public String getFailureMessage() {
                return "No service with namespace=" + namespace + ", serviceName=" + service;
            }
        };
        waitForExitCondition(exitCondition);

        Boolean delete = machine.config().get(DELETE_EMPTY_NAMESPACE);
        if (delete) {
            deleteEmptyNamespace(namespace);
        }
    }

    protected void deleteKubernetesResourceLocation(Entity entity) {
        final String namespace = entity.sensors().get(KUBERNETES_NAMESPACE);
        final String resourceType = entity.sensors().get(KubernetesResource.RESOURCE_TYPE);
        final String resourceName = entity.sensors().get(KubernetesResource.RESOURCE_NAME);

        if (!handleResourceDelete(resourceType, resourceName, namespace)) {
            LOG.warn("Resource {}: {} not deleted", resourceName, resourceType);
        }
    }

    protected boolean handleResourceDelete(String resourceType, String resourceName, String namespace) {
        try {
            switch (resourceType) {
                case "Deployment":
                    return client.extensions().deployments().inNamespace(namespace).withName(resourceName).delete();
                case "ReplicaSet":
                    return client.extensions().replicaSets().inNamespace(namespace).withName(resourceName).delete();
                case "ConfigMap":
                    return client.configMaps().inNamespace(namespace).withName(resourceName).delete();
                case "PersistentVolume":
                    return client.persistentVolumes().withName(resourceName).delete();
                case "Secret":
                    return client.secrets().inNamespace(namespace).withName(resourceName).delete();
                case "Service":
                    return client.services().inNamespace(namespace).withName(resourceName).delete();
                case "ReplicationController":
                    return client.replicationControllers().inNamespace(namespace).withName(resourceName).delete();
                case "Namespace":
                    return client.namespaces().withName(resourceName).delete();
            }
        } catch (KubernetesClientException kce) {
            LOG.warn("Error deleting resource {}: {}", resourceName, kce);
        }
        return false;
    }

    protected void undeploy(final String namespace, final String deployment, final String pod) {
        client.extensions().deployments().inNamespace(namespace).withName(deployment).delete();
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                return client.extensions().deployments().inNamespace(namespace).withName(deployment).get() == null;
            }
            @Override
            public String getFailureMessage() {
                return "No deployment with namespace=" + namespace + ", deployment=" + deployment;
            }
        };
        waitForExitCondition(exitCondition);
    }

    protected synchronized void deleteEmptyNamespace(final String name) {
        if (!name.equals("default") && isNamespaceEmpty(name)) {
            if (client.namespaces().withName(name).get() != null &&
                    !client.namespaces().withName(name).get().getStatus().getPhase().equals("Terminating")) {
                client.namespaces().withName(name).delete();
                ExitCondition exitCondition = new ExitCondition() {
                    @Override
                    public Boolean call() {
                        return client.namespaces().withName(name).get() == null;
                    }
                    @Override
                    public String getFailureMessage() {
                        return "Namespace " + name + " still present";
                    }
                };
                waitForExitCondition(exitCondition);
            }
        }
    }

    protected boolean isNamespaceEmpty(String name) {
        return client.extensions().deployments().inNamespace(name).list().getItems().isEmpty() &&
               client.services().inNamespace(name).list().getItems().isEmpty() &&
               client.secrets().inNamespace(name).list().getItems().isEmpty();
    }

    @Override
    public Map<String, Object> getProvisioningFlags(Collection<String> tags) {
        return null;
    }

    protected MachineLocation createKubernetesResourceLocation(Entity entity, ConfigBag setup) {
        String resourceUri = entity.config().get(KubernetesResource.RESOURCE_FILE);
        InputStream resource = ResourceUtils.create(entity).getResourceFromUrl(resourceUri);
        String templateContents = Streams.readFullyString(resource);
        String processedContents = TemplateProcessor.processTemplateContents(templateContents, (EntityInternal) entity, setup.getAllConfig());
        InputStream processedResource = Streams.newInputStreamWithContents(processedContents);

        final List<HasMetadata> result = getClient().load(processedResource).createOrReplace();

        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                if (result.isEmpty()) {
                    return false;
                }
                List<HasMetadata> check = client.resource(result.get(0)).inNamespace(result.get(0).getMetadata().getNamespace()).get();
                if (result.size() > 1 || check.size() != 1 || check.get(0).getMetadata() == null) {
                    return false;
                }
                return true;
            }
            @Override
            public String getFailureMessage() {
                return "Cannot find created resources";
            }
        };
        waitForExitCondition(exitCondition);

        HasMetadata metadata = result.get(0);
        String resourceType = metadata.getKind();
        String resourceName = metadata.getMetadata().getName();
        String namespace = metadata.getMetadata().getNamespace();
        LOG.debug("Resource {} (type {}) deployed to {}", resourceName, resourceType, namespace);

        entity.sensors().set(KUBERNETES_NAMESPACE, namespace);
        entity.sensors().set(KubernetesResource.RESOURCE_NAME, resourceName);
        entity.sensors().set(KubernetesResource.RESOURCE_TYPE, resourceType);

        LocationSpec<SshMachineLocation> locationSpec = LocationSpec.create(SshMachineLocation.class)
                        .configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT));
        if (!findResourceAddress(locationSpec, entity, metadata, resourceType, resourceName, namespace)) {
            LOG.info("Resource {} with type {} has no associated address", resourceName, resourceType);
            locationSpec.configure("address", "0.0.0.0");
        }

        SshMachineLocation machine = getManagementContext().getLocationManager().createLocation(locationSpec);

        if (resourceType.equals("Service")) {
            Service service = getService(namespace, resourceName);
            registerPortMappings(machine, service);

            List<ServicePort> ports = service.getSpec().getPorts();
            for (ServicePort port : ports) {
                String protocol = port.getProtocol();
                if ("TCP".equalsIgnoreCase(protocol)) {
                    Integer targetPort = port.getTargetPort().getIntVal();
                    AttributeSensor<Integer> sensor = Sensors.newIntegerSensor("kubernetes.port." + port.getName());
                    entity.sensors().set(sensor, targetPort);
                }
            }
        }

        return machine;
    }

    protected boolean findResourceAddress(LocationSpec<SshMachineLocation> locationSpec, Entity entity, HasMetadata metadata, String resourceType, String resourceName, String namespace) {
        if (resourceType.equals("Deployment") || resourceType.equals("ReplicationController") || resourceType.equals("Pod")) {
            Map<String, String> labels = MutableMap.of();
            if (resourceType.equals("Deployment")) {
                Deployment deployment = (Deployment) metadata;
                labels = deployment.getSpec().getTemplate().getMetadata().getLabels();
            } else if (resourceType.equals("ReplicationController")) {
                ReplicationController replicationController = (ReplicationController) metadata;
                labels = replicationController.getSpec().getTemplate().getMetadata().getLabels();
            }
            Pod pod = resourceType.equals("Pod") ? getPod(namespace, resourceName) : getPod(namespace, labels);
            entity.sensors().set(KubernetesLocationConfig.KUBERNETES_POD, pod.getMetadata().getName());

            InetAddress node = Networking.getInetAddressWithFixedName(pod.getSpec().getNodeName());
            String podAddress = pod.getStatus().getPodIP();

            locationSpec.configure("address", node);
            locationSpec.configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.of(podAddress));

            return true;
        } else if (resourceType.equals("Service")) {
            getService(namespace, resourceName);

            Endpoints endpoints = client.endpoints().inNamespace(namespace).withName(resourceName).get();
            EndpointSubset subset = endpoints.getSubsets().get(0);
            EndpointAddress address = subset.getAddresses().get(0);
            String podName = address.getTargetRef().getName();
            String privateIp = address.getIp();

            locationSpec.configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.of(privateIp));

            try {
                Pod pod = getPod(namespace, podName);
                entity.sensors().set(KubernetesLocationConfig.KUBERNETES_POD, podName);

                InetAddress node = Networking.getInetAddressWithFixedName(pod.getSpec().getNodeName());
                locationSpec.configure("address", node);
            } catch (KubernetesClientException kce) {
                LOG.warn("Cannot find pod {} in namespace {} for service {}", new Object[] { podName, namespace, resourceName });
            }

            return true;
        } else {
            return false;
        }
    }

    protected MachineLocation createKubernetesContainerLocation(Entity entity, ConfigBag setup) {
        String deploymentName = findDeploymentName(entity, setup);
        Integer replicas = setup.get(REPLICAS);
        List<String> volumes = setup.get(KubernetesLocationConfig.PERSISTENT_VOLUMES);
        Map<String, String> secrets = setup.get(KubernetesLocationConfig.SECRETS);
        Map<String, String> limits = setup.get(KubernetesLocationConfig.LIMITS);
        Boolean privileged = setup.get(KubernetesLocationConfig.PRIVILEGED);
        String imageName = findImageName(entity, setup);
        Iterable<Integer> inboundPorts = findInboundPorts(entity, setup);
        Map<String, String> env = findEnvironmentVariables(entity, setup, imageName);
        Map<String, String> metadata = findMetadata(entity, deploymentName);

        if (volumes != null) {
            createPersistentVolumes(volumes);
        }

        Namespace namespace = createOrGetNamespace(setup.get(NAMESPACE), setup.get(CREATE_NAMESPACE));

        if (secrets != null) {
            createSecrets(namespace.getMetadata().getName(), secrets);
        }

        Container container = buildContainer(namespace.getMetadata().getName(), metadata, deploymentName, imageName, inboundPorts, env, limits, privileged);
        deploy(namespace.getMetadata().getName(), entity, metadata, deploymentName, container, replicas, secrets);
        Service service = exposeService(namespace.getMetadata().getName(), metadata, deploymentName, inboundPorts);
        Pod pod = getPod(namespace.getMetadata().getName(), metadata);

        entity.sensors().set(KubernetesLocationConfig.KUBERNETES_NAMESPACE, namespace.getMetadata().getName());
        entity.sensors().set(KubernetesLocationConfig.KUBERNETES_DEPLOYMENT, deploymentName);
        entity.sensors().set(KubernetesLocationConfig.KUBERNETES_POD, pod.getMetadata().getName());
        entity.sensors().set(KubernetesLocationConfig.KUBERNETES_SERVICE, service.getMetadata().getName());

        LocationSpec<SshMachineLocation> locationSpec = prepareLocationSpec(entity, setup, namespace, deploymentName, service, pod);
        SshMachineLocation machine = getManagementContext().getLocationManager().createLocation(locationSpec);
        registerPortMappings(machine, service);
        if (!isDockerContainer(entity)) {
            waitForSshable(machine, Duration.FIVE_MINUTES);
        }

        return machine;
    }

    protected void waitForSshable(final SshMachineLocation machine, Duration timeout) {
        Callable<Boolean> checker = new Callable<Boolean>() {
            public Boolean call() {
                int exitstatus = machine.execScript(
                        ImmutableMap.of( // TODO investigate why SSH connection does not time out with this config
                                SshTool.PROP_CONNECT_TIMEOUT.getName(), Duration.TEN_SECONDS.toMilliseconds(),
                                SshTool.PROP_SESSION_TIMEOUT.getName(), Duration.TEN_SECONDS.toMilliseconds(),
                                SshTool.PROP_SSH_TRIES_TIMEOUT.getName(), Duration.TEN_SECONDS.toMilliseconds(),
                                SshTool.PROP_SSH_TRIES.getName(), 1),
                        "check-sshable",
                        ImmutableList.of("true"));
                boolean success = (exitstatus == 0);
                return success;
            }};

        Stopwatch stopwatch = Stopwatch.createStarted();
        ReferenceWithError<Boolean> reachable = new ThreadedRepeater("reachable")
                .backoff(Duration.FIVE_SECONDS, 2, Duration.TEN_SECONDS) // Exponential backoff, to 10 seconds
                .until(checker)
                .limitTimeTo(timeout)
                .runKeepingError();

        if (!reachable.getWithoutError()) {
            throw new IllegalStateException("Connection failed for "+machine.getSshHostAndPort()+" after waiting "+stopwatch.elapsed(TimeUnit.SECONDS), reachable.getError());
        } else {
            LOG.debug("Connection succeeded for {} after {}", machine.getSshHostAndPort(), stopwatch.elapsed(TimeUnit.SECONDS));
        }
    }

    protected void registerPortMappings(SshMachineLocation machine, Service service) {
        PortForwardManager portForwardManager = (PortForwardManager) getManagementContext().getLocationRegistry()
                .getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        List<ServicePort> ports = service.getSpec().getPorts();
        String publicHostText = machine.getSshHostAndPort().getHostText();
        LOG.debug("Recording port-mappings for container {} of {}: {}", new Object[] { machine, this, ports });

        for (ServicePort port : ports) {
            String protocol = port.getProtocol();
            Integer targetPort = port.getTargetPort().getIntVal();

            if (!"TCP".equalsIgnoreCase(protocol)) {
                LOG.debug("Ignoring port mapping {} for {} because only TCP is currently supported", port, machine);
            } else if (targetPort == null) {
                LOG.debug("Ignoring port mapping {} for {} because targetPort.intValue is null", port, machine);
            } else if (port.getNodePort() == null) {
                LOG.debug("Ignoring port mapping {} to {} because port.getNodePort() is null", targetPort, machine);
            } else {
                portForwardManager.associate(publicHostText, HostAndPort.fromParts(publicHostText, port.getNodePort()), machine, targetPort);
            }
        }
    }

    protected String findDeploymentName(Entity entity, ConfigBag setup) {
        return Optional.fromNullable(setup.get(KubernetesLocationConfig.DEPLOYMENT)).or(entity.getId());
    }

    protected synchronized Namespace createOrGetNamespace(final String name, Boolean create) {
        Namespace namespace = client.namespaces().withName(name).get();
        ExitCondition namespaceReady = new ExitCondition() {
            @Override
            public Boolean call() {
                Namespace actualNamespace = client.namespaces().withName(name).get();
                return actualNamespace != null && actualNamespace.getStatus().getPhase().equals("Active");
            }
            @Override
            public String getFailureMessage() {
                Namespace actualNamespace = client.namespaces().withName(name).get();
                return "Namespace for " + name + " " + (actualNamespace == null ? "absent" : " status " + actualNamespace.getStatus()); 
            }
        };
        if (namespace != null) {
            LOG.debug("Found namespace {}, returning it.", namespace);
        } else if (create) {
            namespace = client.namespaces().create(new NamespaceBuilder().withNewMetadata().withName(name).endMetadata().build());
            LOG.debug("Created namespace {}.", namespace);
        } else {
            throw new IllegalStateException("Namespace " + name + " does not exist and namespace.create is not set");
        }
        waitForExitCondition(namespaceReady);
        return client.namespaces().withName(name).get();
    }

    protected Pod getPod(final String namespace, final String name) {
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                Pod result = client.pods().inNamespace(namespace).withName(name).get();
                return result != null && result.getStatus().getPodIP() != null;
            }
            @Override
            public String getFailureMessage() {
                return "Cannot find pod with name: " + name;
            }
        };
        waitForExitCondition(exitCondition);
        Pod result = client.pods().inNamespace(namespace).withName(name).get();
        return result;
    }

    protected Pod getPod(final String namespace, final Map<String, String> metadata) {
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                PodList result = client.pods().inNamespace(namespace).withLabels(metadata).list();
                return result.getItems().size() >= 1 && result.getItems().get(0).getStatus().getPodIP() != null;
            }
            @Override
            public String getFailureMessage() {
                return "Cannot find pod with metadata: " + Joiner.on(" ").withKeyValueSeparator("=").join(metadata);
            }
        };
        waitForExitCondition(exitCondition);
        PodList result = client.pods().inNamespace(namespace).withLabels(metadata).list();
        return result.getItems().get(0);
    }

    protected void createSecrets(String namespace, Map<String, String> secrets) {
        for (Map.Entry<String, String> nameAuthEntry : secrets.entrySet()) {
            createSecret(namespace, nameAuthEntry.getKey(), nameAuthEntry.getValue());
        }
    }

    protected Secret createSecret(final String namespace, final String secretName, String auth) {
        Secret secret = client.secrets().inNamespace(namespace).withName(secretName).get();
        if (secret != null) return secret;

        String json = String.format("{\"https://index.docker.io/v1/\":{\"auth\":\"%s\"}}", auth);
        String base64encoded = BaseEncoding.base64().encode(json.getBytes(Charset.defaultCharset()));
        secret = client.secrets().inNamespace(namespace)
                .create(new SecretBuilder()
                        .withNewMetadata()
                            .withName(secretName)
                        .endMetadata()
                        .withType("kubernetes.io/dockercfg")
                        .withData(ImmutableMap.of(".dockercfg", base64encoded))
                        .build());
        try {
            client.secrets().inNamespace(namespace).create(secret);
        } catch (KubernetesClientException e) {
            if (e.getCode() == 500 && e.getMessage().contains("Message: resourceVersion may not be set on objects to be created")) {
                // ignore exception as per https://github.com/fabric8io/kubernetes-client/issues/451
            } else {
                throw Throwables.propagate(e);
            }
        }
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                return client.secrets().inNamespace(namespace).withName(secretName).get() != null;
            }
            @Override
            public String getFailureMessage() {
                return "Absent namespace=" + namespace + ", secretName=" + secretName;
            }
        };
        waitForExitCondition(exitCondition);
        return client.secrets().inNamespace(namespace).withName(secretName).get();
    }

    protected Container buildContainer(String namespace, Map<String, String> metadata, String deploymentName, String imageName, Iterable<Integer> inboundPorts, Map<String, ?> env, Map<String, String> limits, boolean privileged) {
        List<ContainerPort> containerPorts = Lists.newArrayList();
        for (Integer inboundPort : inboundPorts) {
            containerPorts.add(new ContainerPortBuilder().withContainerPort(inboundPort).build());
        }

        List<EnvVar> envVars = Lists.newArrayList();
        for (Map.Entry<String, ?> envVarEntry : env.entrySet()) {
            envVars.add(new EnvVarBuilder().withName(envVarEntry.getKey()).withValue(envVarEntry.getValue().toString()).build());
        }

        ContainerBuilder containerBuilder = new ContainerBuilder()
                .withName(deploymentName)
                .withImage(imageName)
                .addToPorts(Iterables.toArray(containerPorts, ContainerPort.class))
                .addToEnv(Iterables.toArray(envVars, EnvVar.class))
                .withNewSecurityContext()
                     .withPrivileged(privileged)
                .endSecurityContext();

        if (limits != null) {
            for (Map.Entry<String, String> nameValueEntry : limits.entrySet()) {
                ResourceRequirements resourceRequirements = new ResourceRequirementsBuilder().addToLimits(nameValueEntry.getKey(), new QuantityBuilder().withAmount(nameValueEntry.getValue()).build()).build();
                containerBuilder.withResources(resourceRequirements);
            }
        }
        LOG.debug("Built container {} to be deployed in namespace {} with metadata {}.", containerBuilder.build(), namespace, metadata);
        return containerBuilder.build();
    }

    protected void deploy(final String namespace, Entity entity, Map<String, String> metadata, final String deploymentName, Container container, final Integer replicas, Map<String, String> secrets) {
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
        Deployment deployment = new DeploymentBuilder()
                .withNewMetadata()
                    .withName(deploymentName)
                    .addToAnnotations(CLOUDSOFT_ENTITY_ID, entity.getId())
                    .addToAnnotations(CLOUDSOFT_APPLICATION_ID, entity.getApplicationId())
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                    .withTemplate(template)
                .endSpec()
                .build();
        client.extensions().deployments().inNamespace(namespace).create(deployment);
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                Deployment dep = client.extensions().deployments().inNamespace(namespace).withName(deploymentName).get();
                DeploymentStatus status = (dep == null) ? null : dep.getStatus();
                Integer replicas = (status == null) ? null : status.getAvailableReplicas();
                return replicas != null && replicas.intValue() == replicas;
            }
            @Override
            public String getFailureMessage() {
                Deployment dep = client.extensions().deployments().inNamespace(namespace).withName(deploymentName).get();
                DeploymentStatus status = (dep == null) ? null : dep.getStatus();
                return "Namespace=" + namespace + "; deploymentName= " + deploymentName + "; Deployment=" + dep
                        + "; status=" + status
                        + "; availableReplicas=" + (status == null ? "null" : status.getAvailableReplicas());
            }
        };
        waitForExitCondition(exitCondition);
        LOG.debug("Deployed deployment {} in namespace {}.", deployment, namespace);
    }

    protected Service exposeService(String namespace, Map<String, String> metadata, String serviceName, Iterable<Integer> inboundPorts) {
        List<ServicePort> servicePorts = Lists.newArrayList();
        for (Integer inboundPort : inboundPorts) {
            servicePorts.add(new ServicePortBuilder().withName(Integer.toString(inboundPort)).withPort(inboundPort).build());
        }
        Service service = new ServiceBuilder().withNewMetadata().withName(serviceName).endMetadata()
                .withNewSpec()
                    .addToSelector(metadata)
                    .addToPorts(Iterables.toArray(servicePorts, ServicePort.class))
                    .withType(SERVER_TYPE)
                .endSpec()
                .build();
        client.services().inNamespace(namespace).create(service);

        service = getService(namespace, serviceName);
        LOG.debug("Exposed service {} in namespace {}.", service, namespace);
        return service;
    }

    protected Service getService(final String namespace, final String serviceName) {
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                Service svc = client.services().inNamespace(namespace).withName(serviceName).get();
                if (svc == null || svc.getStatus() == null) {
                    return false;
                }
                Endpoints endpoints = client.endpoints().inNamespace(namespace).withName(serviceName).get();
                if (endpoints == null || endpoints.getSubsets().isEmpty()) {
                    return false;
                }
                for (EndpointSubset subset : endpoints.getSubsets()) {
                    if (subset.getNotReadyAddresses().size() > 0) {
                        return false;
                    }
                }
                return true;
            }
            @Override
            public String getFailureMessage() {
                Endpoints endpoints = client.endpoints().inNamespace(namespace).withName(serviceName).get();
                return "Service endpoints in " + namespace + " for serviceName= " + serviceName + " not ready: " + endpoints;
            }
        };
        waitForExitCondition(exitCondition);

        return client.services().inNamespace(namespace).withName(serviceName).get();
    }

    protected LocationSpec<SshMachineLocation> prepareLocationSpec(Entity entity, ConfigBag setup, Namespace namespace, String deploymentName, Service service, Pod pod) {
        InetAddress node = Networking.getInetAddressWithFixedName(pod.getSpec().getNodeName());
        String podAddress = pod.getStatus().getPodIP();
        LocationSpec<SshMachineLocation> locationSpec = LocationSpec.create(SshMachineLocation.class)
                .configure("address", node)
                .configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.of(podAddress))
                .configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT));
        if (!isDockerContainer(entity)) {
            Optional<ServicePort> sshPort = Iterables.tryFind(service.getSpec().getPorts(), new Predicate<ServicePort>() {
                    @Override
                    public boolean apply(ServicePort input) {
                        return input.getProtocol().equalsIgnoreCase("TCP") && input.getPort().intValue() == 22;
                    }
                });
            Optional<Integer> sshPortNumber;
            if (sshPort.isPresent()) {
                sshPortNumber = Optional.of(sshPort.get().getNodePort());
            } else {
                LOG.warn("No port-mapping found to ssh port 22, for container {}", service);
                sshPortNumber = Optional.absent();
            }
            locationSpec.configure(CloudLocationConfig.USER, setup.get(KubernetesLocationConfig.LOGIN_USER))
                    .configure(SshMachineLocation.PASSWORD, setup.get(KubernetesLocationConfig.LOGIN_USER_PASSWORD))
                    .configureIfNotNull(SshMachineLocation.SSH_PORT, sshPortNumber.orNull())
                    .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
                    .configure(BrooklynConfigKeys.ONBOX_BASE_DIR, "/tmp");
        }
        return locationSpec;
    }

    protected void createPersistentVolumes(List<String> volumes) {
        for (final String persistentVolume : volumes) {
            PersistentVolume volume = new PersistentVolumeBuilder()
                    .withNewMetadata()
                    .withName(persistentVolume)
                    .withLabels(ImmutableMap.of("type", "local")) // TODO make it configurable
                    .endMetadata()
                    .withNewSpec()
                    .addToCapacity("storage", new QuantityBuilder().withAmount("20").build()) // TODO make it configurable
                    .addToAccessModes("ReadWriteOnce") // TODO make it configurable
                    .withNewHostPath().withPath("/tmp/pv-1").endHostPath() // TODO make it configurable
                    .endSpec()
                    .build();
            client.persistentVolumes().create(volume);
            ExitCondition exitCondition = new ExitCondition() {
                @Override
                public Boolean call() {
                    PersistentVolume pv = client.persistentVolumes().withName(persistentVolume).get();
                    return pv != null && pv.getStatus() != null
                            && pv.getStatus().getPhase().equals("Available");
                }
                @Override
                public String getFailureMessage() {
                    PersistentVolume pv = client.persistentVolumes().withName(persistentVolume).get();
                    return "PersistentVolume for " + persistentVolume + " " + (pv == null ? "absent" : "pv=" + pv);
                }
            };
            waitForExitCondition(exitCondition);
        }
    }

    protected Entity validateCallerContext(ConfigBag setup) {
        // Lookup entity flags
        Object callerContext = setup.get(LocationConfigKeys.CALLER_CONTEXT);
        if (callerContext == null || !(callerContext instanceof Entity)) {
            throw new IllegalStateException("Invalid caller context: " + callerContext);
        }
        return (Entity) callerContext;
    }

    protected Entity validateCallerContext(MachineLocation machine) {
        // Lookup entity flags
        Object callerContext = machine.config().get(LocationConfigKeys.CALLER_CONTEXT);
        if (callerContext == null || !(callerContext instanceof Entity)) {
            throw new IllegalStateException("Invalid caller context: " + callerContext);
        }
        return (Entity) callerContext;
    }


    protected Map<String, String> findMetadata(Entity entity, String value) {
        Map<String, String> metadata = Maps.newHashMap();
        if (!isDockerContainer(entity)) {
            metadata.put(SSHABLE_CONTAINER, value);
        } else {
            metadata.put(IMMUTABLE_CONTAINER_KEY, value);
        }
        return metadata;
    }

    /**
     * Appends the config's "env" with the {@code CLOUDSOFT_ROOT_PASSWORD} env if appropriate.
     * This is (approximately) the same behaviour as the {@link DockerJcloudsLocation} used for 
     * Swarm.
     * 
     * Side-effects the {@code config}, to set the loginPassword if one is auto-generated.
     * 
     * TODO Longer term, we'll add a config key to the `DockerContainer` entity for env variables.
     * We'll inject those when launching the container (and will do the same in 
     * `DockerJcloudsLocation` for Swarm). What would the precedence be? Would we prefer the
     * entity's config over the location's (or vice versa)? I think the entity's would take
     * precedence, as a location often applies to a whole app so we might well want to override
     * or augment it for specific entities.
     */
    protected Map<String, String> findEnvironmentVariables(Entity entity, ConfigBag config, String imageName) {
        String loginUser = config.get(LOGIN_USER);
        String loginPassword = config.get(LOGIN_USER_PASSWORD);
        Map<String, String> injections = Maps.newLinkedHashMap();

        // Check if login credentials should be injected
        Boolean injectLoginCredentials = config.get(INJECT_LOGIN_CREDENTIAL);
        if (injectLoginCredentials == null) {
            for (String regex : IMAGE_DESCRIPTION_REGEXES_REQUIRING_INJECTED_LOGIN_CREDS) {
                if (imageName != null && imageName.matches(regex)) {
                    injectLoginCredentials = true;
                    break;
                }
            }
        }

        if (Boolean.TRUE.equals(injectLoginCredentials)) {
            if ((Strings.isBlank(loginUser) || "root".equals(loginUser))) {
                loginUser = "root";
                config.configure(LOGIN_USER, loginUser);

                if (Strings.isBlank(loginPassword)) {
                    loginPassword = Identifiers.makeRandomPassword(12);
                    config.configure(LOGIN_USER_PASSWORD, loginPassword);
                }

                injections.put("CLOUDSOFT_ROOT_PASSWORD", loginPassword);
            }
        }

        Map<String,Object> rawEnv = MutableMap.<String, Object>builder()
                .putAll(injections)
                .putAll(MutableMap.copyOf(config.get(ENV)))
                .putAll(MutableMap.copyOf(entity.config().get(DockerContainer.CONTAINER_ENVIRONMENT)))
                .build();
        return Maps.transformValues(rawEnv, Functions.toStringFunction());
    }

    protected Iterable<Integer> findInboundPorts(Entity entity, ConfigBag setup) {
        Iterable<String> inboundTcpPorts = entity.config().get(DockerContainer.INBOUND_TCP_PORTS);
        if (inboundTcpPorts != null) {
            List<Integer> inboundPorts = Lists.newArrayList();
            List<String> portRanges = MutableList.copyOf(entity.config().get(DockerContainer.INBOUND_TCP_PORTS));
            for (String portRange : portRanges) {
                for (Integer port : PortRanges.fromString(portRange)) {
                    inboundPorts.add(port);
                }
            }
            return inboundPorts;
        } else {
            if (setup.containsKey(INBOUND_PORTS)) {
                return toIntPortList(setup.get(INBOUND_PORTS));
            } else {
                return ImmutableList.of(22);
            }
        }
    }

    public static List<Integer> toIntPortList(Object v) {
        if (v == null) return ImmutableList.of();
        PortRange portRange = PortRanges.fromIterable(ImmutableList.of(v));
        return ImmutableList.copyOf(portRange);
    }

    protected String findImageName(Entity entity, ConfigBag setup) {
        String result = entity.config().get(DockerContainer.IMAGE_NAME);
        if (Strings.isNonBlank(result)) return result;

        result = setup.get(IMAGE);
        if (Strings.isNonBlank(result)) return result;

        String osFamily = setup.get(OS_FAMILY);
        String osVersion = setup.get(OS_VERSION_REGEX);
        Optional<String> imageName = new ImageChooser().chooseImage(osFamily, osVersion);
        if (imageName.isPresent()) return imageName.get();

        throw new IllegalStateException("No matching image found for " + entity 
                + " (no explicit image name, osFamily=" + osFamily + "; osVersion=" + osVersion + ")");
    }

    protected boolean isDockerContainer(Entity entity) {
        return entity.getEntityType().getName().equalsIgnoreCase(DockerContainer.class.getName());
    }

    protected boolean isKubernetesResource(Entity entity) {
        return entity.getEntityType().getName().equalsIgnoreCase(KubernetesResource.class.getName());
    }

    @Override
    public MachineProvisioningLocation<MachineLocation> newSubLocation(Map<?, ?> newFlags) {
        return null;
    }

    protected void waitForExitCondition(ExitCondition exitCondition) {
        waitForExitCondition(exitCondition, Duration.ONE_SECOND, Duration.FIVE_MINUTES);
    }

    protected void waitForExitCondition(ExitCondition exitCondition, Duration finalDelay, Duration duration) {
        ReferenceWithError<Boolean> result = Repeater.create()
                .backoffTo(finalDelay)
                .limitTimeTo(duration)
                .until(exitCondition)
                .runKeepingError();
        if (!result.get()) {
            String err = "Exit condition unsatisfied after " + duration + ": " + exitCondition.getFailureMessage();
            LOG.info(err + " (rethrowing)");
            throw new IllegalStateException(err);
        }
    }

    public static interface ExitCondition extends Callable<Boolean> {
        public String getFailureMessage();
    }
}
