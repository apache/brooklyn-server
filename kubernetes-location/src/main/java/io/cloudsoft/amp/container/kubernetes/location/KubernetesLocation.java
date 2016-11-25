package io.cloudsoft.amp.container.kubernetes.location;

import static com.google.common.base.Objects.firstNonNull;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;

import io.cloudsoft.amp.containerservice.dockercontainer.DockerContainer;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolume;
import io.fabric8.kubernetes.api.model.PersistentVolumeBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.QuantityBuilder;
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
     * TODOs:
     *  - Ignores config such as "user" and "password"; just uses "loginUser" for connecting to the container.
     *    Does not create a user (so behaves differently from things that use JcloudsLocation).
     *  - Does not use ssh keys (only passwords).
     *  - "cloudsoft/*" containers use "root" (which is discouraged).
     *  - Error handling needs revisited.
     *    For example, if provisioning fails then it waits for five minutes and then fails without a reason why.
     *    e.g. try launching a container with an incorrect image name.
     *  - 
     */

    public static final Logger log = LoggerFactory.getLogger(KubernetesLocation.class);

    public static final String SERVER_TYPE = "NodePort";
    public static final String IMMUTABLE_CONTAINER_KEY = "immutable-container";
    public static final String SSHABLE_CONTAINER = "sshable-container";

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
        // Fail-fast if deployments extension is not available
        if (client.extensions().deployments().list() == null) {
            log.debug("Cannot find the deployments extension!");
            throw new IllegalStateException("Cannot find the deployments extension!");
        }
        return createKubernetesContainerLocation(setup);
    }

    @Override
    public void release(MachineLocation machine) {
        client = getClient();

        final String namespace = machine.config().get(NAMESPACE);
        final String deployment = machine.config().get(DEPLOYMENT);
        final String service = machine.config().get(SERVICE);

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

        client.services().inNamespace(namespace).withName(service).delete();
        exitCondition = new ExitCondition() {
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

        deleteNamespace(namespace);
    }

    private synchronized void deleteNamespace(final String namespace) {
        if (!namespace.equals("default") && isNamespaceEmpty(namespace)) {
            if (client.namespaces().withName(namespace).get() != null && !client.namespaces().withName(namespace).get().getStatus().getPhase().equals("Terminating")) {
                client.namespaces().withName(namespace).delete();
                ExitCondition exitCondition = new ExitCondition() {
                    @Override
                    public Boolean call() {
                        return client.namespaces().withName(namespace).get() == null;
                    }
                    @Override
                    public String getFailureMessage() {
                        return "Namespace " + namespace + " still present";
                    }
                };
                waitForExitCondition(exitCondition);
            }
        }
    }

    private boolean isNamespaceEmpty(String namespace) {
        return client.extensions().deployments().inNamespace(namespace).list().getItems().isEmpty() &&
               client.services().inNamespace(namespace).list().getItems().isEmpty() &&
               client.secrets().inNamespace(namespace).list().getItems().isEmpty();
    }

    @Override
    public Map<String, Object> getProvisioningFlags(Collection<String> tags) {
        return null;
    }

    protected MachineLocation createKubernetesContainerLocation(ConfigBag setup) {
        Entity entity = validateCallerContext(setup);
        String deploymentName = findDeploymentName(entity, setup);
        Integer replicas = setup.get(REPLICAS);
        List<String> volumes = setup.get(KubernetesLocationConfig.PERSISTENT_VOLUMES);
        Map<String, String> secrets = setup.get(KubernetesLocationConfig.SECRETS);
        Map<String, String> limits = setup.get(KubernetesLocationConfig.LIMITS);
        Boolean privileged = setup.get(KubernetesLocationConfig.PRIVILEGED);
        String imageName = findImageName(entity, setup);
        Iterable<Integer> inboundPorts = findInboundPorts(entity, setup);
        Map<String, Object> env = findEnvironmentVariables(entity);
        Map<String, String> metadata = findMetadata(entity, deploymentName);

        if (volumes != null) {
          createPersistentVolumes(volumes);
        }

        Namespace namespace = createOrGetNamespace(setup.get(NAMESPACE));

        if (secrets != null) {
          createSecrets(namespace.getMetadata().getName(), secrets);
        }

        Container container = buildContainer(namespace.getMetadata().getName(), metadata, deploymentName, imageName, inboundPorts, env, limits, privileged);
        Deployment deployment = deploy(namespace.getMetadata().getName(), metadata, deploymentName, container, replicas, secrets);
        Service service = exposeService(namespace.getMetadata().getName(), metadata, deploymentName, inboundPorts);
        Pod pod = getPod(namespace.getMetadata().getName(), metadata);
        LocationSpec<SshMachineLocation> locationSpec = prepareLocationSpec(entity, setup, namespace, deployment, service, pod);
        SshMachineLocation machine = getManagementContext().getLocationManager().createLocation(locationSpec);
        registerPortMappings(machine, service);
        return machine;
    }

    protected void registerPortMappings(SshMachineLocation machine, Service service) {
        PortForwardManager portForwardManager = (PortForwardManager) getManagementContext().getLocationRegistry()
                .getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        List<ServicePort> ports = service.getSpec().getPorts();
        String publicHostText = machine.getSshHostAndPort().getHostText();
        log.info("Recording port-mappings for container {} of {}: {}", new Object[] {machine, this, ports});
        
        for (ServicePort port : ports) {
            String protocol = port.getProtocol();
            IntOrString targetPortObj = port.getTargetPort();
            Integer targetPort = targetPortObj.getIntVal();
            if (!"TCP".equalsIgnoreCase(protocol)) {
                log.debug("Ignoring port mapping {} for {} because only TCP is currently supported", port, machine);
            } else if (targetPort == null) {
                log.debug("Ignoring port mapping {} for {} because targetPort.intValue is null", port, machine);
            } else {
                portForwardManager.associate(publicHostText, HostAndPort.fromParts(publicHostText, port.getNodePort()), machine, targetPort);
            }
        }
    }
    
    private String findDeploymentName(Entity entity, ConfigBag setup) {
        return firstNonNull(setup.get(KubernetesLocationConfig.DEPLOYMENT), entity.getId());
    }

    private synchronized Namespace createOrGetNamespace(final String ns) {
        Namespace namespace = client.namespaces().withName(ns).get();
        ExitCondition namespaceReady = new ExitCondition() {
            @Override
            public Boolean call() {
                Namespace actualNamespace = client.namespaces().withName(ns).get();
                return actualNamespace != null && actualNamespace.getStatus().getPhase().equals("Active");
            }
            @Override
            public String getFailureMessage() {
                Namespace actualNamespace = client.namespaces().withName(ns).get();
                return "Namespace for " + ns + " " + (actualNamespace == null ? "absent" : " status " + actualNamespace.getStatus()); 
            }
        };
        if (namespace != null) {
            log.debug("Found namespace {}, returning it.", namespace);
        } else {
            namespace = client.namespaces().create(new NamespaceBuilder().withNewMetadata().withName(ns).endMetadata().build());
            log.debug("Created namespace {}.", namespace);
        }
        waitForExitCondition(namespaceReady);
        return client.namespaces().withName(ns).get();
    }

    private Pod getPod(String namespace, Map<String, String> metadata) {
        PodList result = client.pods().inNamespace(namespace).withLabels(metadata).list();
        if (result.getItems().isEmpty() || result.getItems().size() > 1) {
            throw new IllegalStateException("Cannot find pod  Pod with metadata: " + Joiner.on(" ").withKeyValueSeparator("=").join(metadata));
        }
        return result.getItems().get(0);
    }

    private void createSecrets(String namespace, Map<String, String> secrets) {
        for (Map.Entry<String, String> nameAuthEntry : secrets.entrySet()) {
            createSecret(namespace, nameAuthEntry.getKey(), nameAuthEntry.getValue());
        }
    }

    private Secret createSecret(final String namespace, final String secretName, String auth) {
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

    private Container buildContainer(String namespace, Map<String, String> metadata, String deploymentName, String imageName, Iterable<Integer> inboundPorts, Map<String, Object> env, Map<String, String> limits, boolean privileged) {
        List<ContainerPort> containerPorts = Lists.newArrayList();
        for (Integer inboundPort : inboundPorts) {
            containerPorts.add(new ContainerPortBuilder().withContainerPort(inboundPort).build());
        }

        List<EnvVar> envVars = Lists.newArrayList();
        for (Map.Entry<String, Object> envVarEntry : env.entrySet()) {
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
        log.debug("Built container {} to be deployed in namespace {} with metadata {}.", containerBuilder.build(), namespace, metadata);
        return containerBuilder.build();
    }

    private Deployment deploy(final String namespace, Map<String, String> metadata, final String deploymentName, Container container, final Integer replicas, Map<String, String> secrets) {
        PodTemplateSpecBuilder podTemplateSpecBuilder = new PodTemplateSpecBuilder()
                .withNewMetadata().addToLabels(metadata).endMetadata()
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
        Deployment deployment = new DeploymentBuilder().withNewMetadata()
                .withName(deploymentName)
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
                return status != null && status.getAvailableReplicas() == replicas;
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
        log.debug("Deployed deployment {} in namespace {}.", deployment, namespace);
        return client.extensions().deployments().inNamespace(namespace).withName(deploymentName).get();
    }

    private Service exposeService(final String namespace, Map<String, String> metadata, final String serviceName, Iterable<Integer> inboundPorts) {
        List<ServicePort> servicePorts = Lists.newArrayList();
        for (Integer inboundPort : inboundPorts) {
            servicePorts.add(new ServicePortBuilder().withName(inboundPort+"").withPort(inboundPort).build());
        }
        Service service = new ServiceBuilder().withNewMetadata().withName(serviceName).endMetadata()
                .withNewSpec()
                .addToSelector(metadata)
                .addToPorts(Iterables.toArray(servicePorts, ServicePort.class))
                .withType(SERVER_TYPE)
                .endSpec()
                .build();
        client.services().inNamespace(namespace).create(service);
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                Service actualService = client.services().inNamespace(namespace).withName(serviceName).get();
                return actualService != null && actualService.getStatus() != null;
            }
            @Override
            public String getFailureMessage() {
                Service s = client.services().inNamespace(namespace).withName(serviceName).get();
                return "Namespace=" + namespace + "; serviceName= " + serviceName + "; service=" + s
                        + "; status=" + (s == null ? "null" : s.getStatus());
            }
        };
        waitForExitCondition(exitCondition);
        log.debug("Exposed service {} in namespace {}.", service, namespace);

        return client.services().inNamespace(namespace).withName(serviceName).get();
    }

    private LocationSpec<SshMachineLocation> prepareLocationSpec(Entity entity, ConfigBag setup, Namespace namespace, Deployment deployment, Service service, Pod pod) {
        try {
            InetAddress node = InetAddress.getByName(pod.getSpec().getNodeName());
            String podAddress = pod.getStatus().getPodIP();
            LocationSpec<SshMachineLocation> locationSpec = LocationSpec.create(SshMachineLocation.class)
                    .configure("address", node)
                    .configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.of(podAddress))
                    .configure(KubernetesLocationConfig.NAMESPACE, namespace.getMetadata().getName())
                    .configure(KubernetesLocationConfig.DEPLOYMENT, deployment.getMetadata().getName())
                    .configure(KubernetesLocationConfig.SERVICE, service.getMetadata().getName())
                    .configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT));
            if (!isDockerContainer(entity)) {
                Optional<ServicePort> sshPort = Iterables.tryFind(service.getSpec().getPorts(), new Predicate<ServicePort>() {
                        @Override
                        public boolean apply(ServicePort input) {
                            return input.getProtocol().equalsIgnoreCase("TCP") && input.getPort() == 22;
                        }
                    });
                Optional<Integer> sshPortNumber;
                if (sshPort.isPresent()) {
                    sshPortNumber = Optional.of(sshPort.get().getNodePort());
                } else {
                    log.warn("No port-mapping found to ssh port 22, for container {}", service);
                    sshPortNumber = Optional.absent();
                }
                
                locationSpec.configure(CloudLocationConfig.USER, setup.get(KubernetesLocationConfig.LOGIN_USER))
                        .configure(SshMachineLocation.PASSWORD, setup.get(KubernetesLocationConfig.LOGIN_USER_PASSWORD))
                        .configureIfNotNull(SshMachineLocation.SSH_PORT, sshPortNumber.orNull())
                        .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
                        .configure(BrooklynConfigKeys.ONBOX_BASE_DIR, "/tmp")
                        .configure(SshMachineLocation.UNIQUE_ID, entity.getId());
            }
            return locationSpec;
        } catch (UnknownHostException uhe) {
            throw Throwables.propagate(uhe);
        }
    }

    private void createPersistentVolumes(List<String> volumes) {
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

    private Entity validateCallerContext(ConfigBag setup) {
        // Lookup entity flags
        Object callerContext = setup.get(LocationConfigKeys.CALLER_CONTEXT);
        if (callerContext == null || !(callerContext instanceof Entity)) {
            throw new IllegalStateException("Invalid caller context: " + callerContext);
        }
        return (Entity) callerContext;
    }


    private Map<String, String> findMetadata(Entity entity, String value) {
        Map<String, String> metadata = Maps.newHashMap();
        if (!isDockerContainer(entity)) {
            metadata.put(SSHABLE_CONTAINER, value);
        } else {
            metadata.put(IMMUTABLE_CONTAINER_KEY, value);
        }
        return metadata;
    }

    private Map<String, Object> findEnvironmentVariables(Entity entity) {
        return firstNonNull(entity.getConfig(SoftwareProcess.SHELL_ENVIRONMENT), ImmutableMap.<String, Object> of());
    }

    private Iterable<Integer> findInboundPorts(Entity entity, ConfigBag setup) {
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
        }
        else {
            if (setup.containsKey(INBOUND_PORTS)) {
                return toIntPortList(setup.get(INBOUND_PORTS));
            } else {
                return ImmutableList.of(22);
            }
        }
    }

    static List<Integer> toIntPortList(Object v) {
        if (v == null) return ImmutableList.of();
        PortRange portRange = PortRanges.fromIterable(ImmutableList.of(v));
        return ImmutableList.copyOf(portRange);
    }

    private String findImageName(Entity entity, ConfigBag setup) {
        return firstNonNull(entity.config().get(DockerContainer.IMAGE_NAME), setup.get(IMAGE));
    }

    private boolean isDockerContainer(Entity entity) {
        return entity.getEntityType().getName().equalsIgnoreCase(DockerContainer.class.getName());
    }

    @Override
    public MachineProvisioningLocation<MachineLocation> newSubLocation(Map<?, ?> newFlags) {
        return null;
    }

    private void waitForExitCondition(ExitCondition exitCondition) {
        waitForExitCondition(exitCondition, Duration.ONE_SECOND, Duration.FIVE_MINUTES);
    }

    private void waitForExitCondition(ExitCondition exitCondition, Duration finalDelay, Duration duration) {
        ReferenceWithError<Boolean> result = Repeater.create()
                .backoffTo(finalDelay)
                .limitTimeTo(duration)
                .until(exitCondition)
                .runKeepingError();
        if (!result.get()) {
            String err = "Exit condition unsatisfied after " + duration + ": " + exitCondition.getFailureMessage();
            log.info(err + " (rethrowing)");
            throw new IllegalStateException(err);
        }
    }

    private static interface ExitCondition extends Callable<Boolean> {
        public String getFailureMessage();
    }
}
