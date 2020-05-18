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
package org.apache.brooklyn.container.location.kubernetes;

import com.google.common.base.Optional;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import hapi.chart.ChartOuterClass.Chart;
import hapi.release.ReleaseOuterClass.Release;
import hapi.services.tiller.Tiller.InstallReleaseRequest;
import hapi.services.tiller.Tiller.InstallReleaseResponse;
import hapi.services.tiller.Tiller.UninstallReleaseRequest;
import hapi.services.tiller.Tiller.UninstallReleaseResponse;
import org.microbean.helm.ReleaseManager;
import org.microbean.helm.Tiller;
import org.microbean.helm.TillerInstaller;
import org.microbean.helm.chart.repository.ChartRepository;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.container.entity.docker.DockerContainer;
import org.apache.brooklyn.container.entity.kubernetes.KubernetesHelmChart;
import org.apache.brooklyn.container.entity.kubernetes.KubernetesPod;
import org.apache.brooklyn.container.entity.kubernetes.KubernetesResource;
import org.apache.brooklyn.container.location.docker.DockerJcloudsLocation;
import org.apache.brooklyn.container.location.kubernetes.machine.KubernetesEmptyMachineLocation;
import org.apache.brooklyn.container.location.kubernetes.machine.KubernetesMachineLocation;
import org.apache.brooklyn.container.location.kubernetes.machine.KubernetesSshMachineLocation;
import org.apache.brooklyn.container.supplier.TillerSupplier;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.network.AbstractOnNetworkEnricher;
import org.apache.brooklyn.core.network.OnPublicNetworkEnricher;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KubernetesLocation extends AbstractLocation implements MachineProvisioningLocation<KubernetesMachineLocation>, KubernetesLocationConfig {

    /*
     * TODO
     *
     *  - Ignores config such as 'user' and 'password', just uses 'loginUser'
     *    and 'loginUser.password' for connecting to the container.
     *  - Does not create a user, so behaves differently from things that use
     *    JcloudsLocation.
     *  - Does not use ssh keys only passwords.
     *  - The 'brooklyncentral/*' images use root which is discouraged.
     *  - Error handling needs revisited. For example, if provisioning fails then
     *    it waits for five minutes and then fails without a reason why.
     *    e.g. try launching a container with an incorrect image name.
     */

    public static final String NODE_PORT = "NodePort";
    public static final String IMMUTABLE_CONTAINER_KEY = "immutable-container";
    public static final String SSHABLE_CONTAINER = "sshable-container";
    public static final String BROOKLYN_ENTITY_ID = "brooklyn.apache.org/entity-id";
    public static final String BROOKLYN_APPLICATION_ID = "brooklyn.apache.org/application-id";
    public static final String KUBERNETES_DOCKERCFG = "kubernetes.io/dockercfg";
    public static final String PHASE_AVAILABLE = "Available";
    public static final String PHASE_TERMINATING = "Terminating";
    public static final String PHASE_ACTIVE = "Active";
    /**
     * The regex for the image descriptions that support us injecting login credentials.
     */
    public static final List<String> IMAGE_DESCRIPTION_REGEXES_REQUIRING_INJECTED_LOGIN_CREDS = ImmutableList.of(
            "brooklyncentral/centos.*",
            "brooklyncentral/ubuntu.*");
    /**
     * The environment variable for injecting login credentials.
     */
    public static final String BROOKLYN_ROOT_PASSWORD = "BROOKLYN_ROOT_PASSWORD";
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLocation.class);
    private Tiller tiller;
    public static final String ADDRESS_KEY = "address";
    private ConfigBag currentConfig;

    public KubernetesLocation() {
        super();
    }

    public KubernetesLocation(Map<?, ?> properties) {
        super(properties);
    }

    protected KubernetesClient getClient() {
        if(currentConfig != null) {
            return getClient(currentConfig);
        }
        return getClient(MutableMap.of());
    }

    protected KubernetesClient getClient(Map<?, ?> flags) {
        ConfigBag conf = (flags == null || flags.isEmpty())
                ? config().getBag()
                : ConfigBag.newInstanceExtending(config().getBag(), flags);
        return getClient(conf);
    }

    protected KubernetesClient getClient(ConfigBag config) {
        currentConfig = config;
        KubernetesClientRegistry registry = getConfig(KUBERNETES_CLIENT_REGISTRY);
        KubernetesClient client = registry.getKubernetesClient(ResolvingConfigBag.newInstanceExtending(getManagementContext(), config));
        return client;
    }

    @Override
    public KubernetesMachineLocation obtain(Map<?, ?> flags) {
        ConfigBag setupRaw = ConfigBag.newInstanceExtending(config().getBag(), flags);
        ConfigBag setup = ResolvingConfigBag.newInstanceExtending(getManagementContext(), setupRaw);

        Entity entity = validateCallerContext(setup);
        if (isKubernetesResource(entity)) {
            return createKubernetesResourceLocation(entity, setup);
        } else if (isKubernetesHelmChart(entity)) {
            return createKubernetesHelmChartLocation(entity, setup);
        } else {
            return createKubernetesContainerLocation(entity, setup);
        }
    }

    @Override
    public void release(KubernetesMachineLocation machine) {
        Entity entity = validateCallerContext(machine);
        if (isKubernetesResource(entity)) {
            deleteKubernetesResourceLocation(entity);
        } else if (isKubernetesHelmChart(entity)) {
            deleteKubernetesHelmChartLocation(entity);
        } else {
            deleteKubernetesContainerLocation(entity, machine);
        }
    }

    protected void deleteKubernetesContainerLocation(Entity entity, MachineLocation machine) {
        final String namespace = entity.sensors().get(KubernetesPod.KUBERNETES_NAMESPACE);
        final String deployment = entity.sensors().get(KubernetesPod.KUBERNETES_DEPLOYMENT);
        final String service = entity.sensors().get(KubernetesPod.KUBERNETES_SERVICE);

        undeploy(namespace, deployment);
        try(KubernetesClient client = getClient()) {
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
            if (Boolean.TRUE.equals(delete)) {
                deleteEmptyNamespace(namespace);
            }
        }
    }

    protected void deleteKubernetesResourceLocation(Entity entity) {
        final String namespace = entity.sensors().get(KubernetesPod.KUBERNETES_NAMESPACE);
        final String resourceType = entity.sensors().get(KubernetesResource.RESOURCE_TYPE);
        final String resourceName = entity.sensors().get(KubernetesResource.RESOURCE_NAME);

        if (!handleResourceDelete(resourceType, resourceName, namespace)) {
            LOG.warn("Resource {}: {} not deleted", resourceName, resourceType);
        }
    }

    protected boolean handleResourceDelete(String resourceType, String resourceName, String namespace) {
        try (KubernetesClient client = getClient()){
            switch (resourceType) {
                case KubernetesResource.DEPLOYMENT:
                    return client.apps().deployments().inNamespace(namespace).withName(resourceName).delete();
                case KubernetesResource.REPLICA_SET:
                    return client.apps().replicaSets().inNamespace(namespace).withName(resourceName).delete();
                case KubernetesResource.CONFIG_MAP:
                    return client.configMaps().inNamespace(namespace).withName(resourceName).delete();
                case KubernetesResource.PERSISTENT_VOLUME:
                    return client.persistentVolumes().withName(resourceName).delete();
                case KubernetesResource.SECRET:
                    return client.secrets().inNamespace(namespace).withName(resourceName).delete();
                case KubernetesResource.SERVICE:
                    return client.services().inNamespace(namespace).withName(resourceName).delete();
                case KubernetesResource.REPLICATION_CONTROLLER:
                    return client.replicationControllers().inNamespace(namespace).withName(resourceName).delete();
                case KubernetesResource.NAMESPACE:
                    return client.namespaces().withName(resourceName).delete();
            }
        } catch (KubernetesClientException kce) {
            LOG.warn("Error deleting resource {}: {}", resourceName, kce);
        }
        return false;
    }

    protected void undeploy(final String namespace, final String deployment) {
        try (KubernetesClient client = getClient()) {
            client.apps().deployments().inNamespace(namespace).withName(deployment).delete();
            ExitCondition exitCondition = new ExitCondition() {
                @Override
                public Boolean call() {
                    return client.apps().deployments().inNamespace(namespace).withName(deployment).get() == null;
                }

                @Override
                public String getFailureMessage() {
                    return "No deployment with namespace=" + namespace + ", deployment=" + deployment;
                }
            };
            waitForExitCondition(exitCondition);
        }
    }
    protected void deleteKubernetesHelmChartLocation(Entity entity) {
        final String releaseName = entity.sensors().get(KubernetesResource.RESOURCE_NAME);
        ReleaseManager chartManager = new ReleaseManager(tiller);
        try {
            Future<UninstallReleaseResponse> uninstallReleaseResponseFuture = chartManager.uninstall(UninstallReleaseRequest.newBuilder()
                    .setTimeout(300L)
                    .setName(releaseName)
                    .setPurge(true)
                    .build());
            UninstallReleaseResponse response = uninstallReleaseResponseFuture.get();
            LOG.debug("Release {} uninstalled", response);
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }


    protected synchronized void deleteEmptyNamespace(final String name) {
        if (!name.equals("default") && isNamespaceEmpty(name)) {
            try (KubernetesClient client = getClient()) {
                if (client.namespaces().withName(name).get() != null &&
                        !client.namespaces().withName(name).get().getStatus().getPhase().equals(PHASE_TERMINATING)) {
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
    }

    protected boolean isNamespaceEmpty(String name) {
        try (KubernetesClient client = getClient()) {
            return client.apps().deployments().inNamespace(name).list().getItems().isEmpty() &&
                    client.services().inNamespace(name).list().getItems().isEmpty() &&
                    client.secrets().inNamespace(name).list().getItems().isEmpty();
        }
    }

    @Override
    public Map<String, Object> getProvisioningFlags(Collection<String> tags) {
        return null;
    }

    protected KubernetesMachineLocation createKubernetesResourceLocation(Entity entity, ConfigBag setup) {
        String resourceUri = entity.config().get(KubernetesResource.RESOURCE_FILE);
        InputStream resource = ResourceUtils.create(entity).getResourceFromUrl(resourceUri);
        String templateContents = Streams.readFullyString(resource);
        String processedContents = TemplateProcessor.processTemplateContents(templateContents, (EntityInternal) entity, setup.getAllConfig());
        InputStream processedResource = Streams.newInputStreamWithContents(processedContents);


        try (KubernetesClient client = getClient()) {
            final List<HasMetadata> result = client.load(processedResource).createOrReplace();

            ExitCondition exitCondition = new ExitCondition() {
                @Override
                public Boolean call() {
                    if (result.isEmpty()) {
                        return false;
                    }
                    HasMetadata check = client.resource(result.get(0)).inNamespace(result.get(0).getMetadata().getNamespace()).get();
                    if (result.size() > 1 || check != null || check.getMetadata() == null) {
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

            entity.sensors().set(KubernetesPod.KUBERNETES_NAMESPACE, namespace);
            entity.sensors().set(KubernetesResource.RESOURCE_NAME, resourceName);
            entity.sensors().set(KubernetesResource.RESOURCE_TYPE, resourceType);

            LocationSpec<? extends KubernetesMachineLocation> locationSpec = LocationSpec.create(KubernetesSshMachineLocation.class);
            if (!findResourceAddress(locationSpec, entity, metadata, resourceType, resourceName, namespace)) {
                LOG.info("Resource {} with type {} has no associated address", resourceName, resourceType);
                locationSpec = LocationSpec.create(KubernetesEmptyMachineLocation.class);
            }
            locationSpec.configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT))
                    .configure(KubernetesMachineLocation.KUBERNETES_NAMESPACE, namespace)
                    .configure(KubernetesMachineLocation.KUBERNETES_RESOURCE_NAME, resourceName)
                    .configure(KubernetesMachineLocation.KUBERNETES_RESOURCE_TYPE, resourceType);

            KubernetesMachineLocation machine = getManagementContext().getLocationManager().createLocation(locationSpec);

            if (resourceType.equals(KubernetesResource.SERVICE) && machine instanceof KubernetesSshMachineLocation) {
                Service service = getService(namespace, resourceName, client);
                registerPortMappings((KubernetesSshMachineLocation) machine, entity, service);
            }

            return machine;
        }
    }

    protected boolean findResourceAddress(LocationSpec<? extends KubernetesMachineLocation> locationSpec, Entity entity,
                                          HasMetadata metadata, String resourceType, String resourceName, String namespace) {
        if (resourceType.equals(KubernetesResource.DEPLOYMENT) || resourceType.equals(KubernetesResource.REPLICATION_CONTROLLER)
                || resourceType.equals(KubernetesResource.POD)) {
            Map<String, String> labels = MutableMap.of();
            if (resourceType.equals(KubernetesResource.DEPLOYMENT)) {
                Deployment deployment = (Deployment) metadata;
                labels = deployment.getSpec().getTemplate().getMetadata().getLabels();
            } else if (resourceType.equals(KubernetesResource.REPLICATION_CONTROLLER)) {
                ReplicationController replicationController = (ReplicationController) metadata;
                labels = replicationController.getSpec().getTemplate().getMetadata().getLabels();
            }
            Pod pod;
            try(KubernetesClient client = getClient()) {
                pod = resourceType.equals(KubernetesResource.POD) ? getPod(namespace, resourceName, client) : getPod(namespace, labels, client);
            }
            entity.sensors().set(KubernetesPod.KUBERNETES_POD, pod.getMetadata().getName());

            InetAddress node = Networking.getInetAddressWithFixedName(pod.getSpec().getNodeName());
            String podAddress = pod.getStatus().getPodIP();

            locationSpec.configure(ADDRESS_KEY, node);
            locationSpec.configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.of(podAddress));

            return true;
        } else if (resourceType.equals(KubernetesResource.SERVICE)) {
            Endpoints endpoints;
            try (KubernetesClient client = getClient()) {
                getService(namespace, resourceName, client);
                endpoints = client.endpoints().inNamespace(namespace).withName(resourceName).get();
            }
            Set<String> privateIps = Sets.newLinkedHashSet();
            Set<String> podNames = Sets.newLinkedHashSet();
            for (EndpointSubset subset : endpoints.getSubsets()) {
                for (EndpointAddress address : subset.getAddresses()) {
                    String podName = address.getTargetRef().getName();
                    podNames.add(podName);
                    String privateIp = address.getIp();
                    privateIps.add(privateIp);
                }
            }
            locationSpec.configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.copyOf(privateIps));

            if (!podNames.isEmpty()) {
                // Use the first pod name from the list; warn when multiple pods are referenced
                String podName = Iterables.get(podNames, 0);
                if (podNames.size() > 1) {
                    LOG.warn("Multiple pods referenced by service {} in namespace {}, using {}: {}",
                            resourceName, namespace, podName, Iterables.toString(podNames));
                }
                try(KubernetesClient client = getClient()) {
                    Pod pod = getPod(namespace, podName, client);
                    entity.sensors().set(KubernetesPod.KUBERNETES_POD, podName);

                    InetAddress node = Networking.getInetAddressWithFixedName(pod.getSpec().getNodeName());
                    locationSpec.configure(ADDRESS_KEY, node);
                } catch (KubernetesClientException kce) {
                    LOG.warn("Cannot find pod {} in namespace {} for service {}", podName, namespace, resourceName);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    protected KubernetesMachineLocation createKubernetesContainerLocation(Entity entity, ConfigBag setup) {
        String deploymentName = lookup(KubernetesPod.DEPLOYMENT, entity, setup, entity.getId());
        Integer replicas = lookup(KubernetesPod.REPLICAS, entity, setup);
        List<String> volumes = lookup(KubernetesPod.PERSISTENT_VOLUMES, entity, setup);
        Map<String, String> secrets = lookup(KubernetesPod.SECRETS, entity, setup);
        Map<String, String> limits = lookup(KubernetesPod.LIMITS, entity, setup);
        Boolean privileged = lookup(KubernetesPod.PRIVILEGED, entity, setup);
        String imageName = findImageName(entity, setup);
        Iterable<Integer> inboundPorts = findInboundPorts(entity, setup);
        Map<String, String> env = findEnvironmentVariables(entity, setup, imageName);
        Map<String, String> metadata = findMetadata(entity, setup, deploymentName);
        KubernetesSshMachineLocation machine;
        try(KubernetesClient client = getClient())  {
            if (volumes != null) {
                createPersistentVolumes(volumes, client);
            }

            Namespace namespace = createOrGetNamespace(lookup(NAMESPACE, entity, setup), setup.get(CREATE_NAMESPACE));

            if (secrets != null) {
                createSecrets(namespace.getMetadata().getName(), secrets);
            }

            Container container = buildContainer(namespace.getMetadata().getName(), metadata, deploymentName, imageName, inboundPorts, env, limits, privileged);
            deploy(namespace.getMetadata().getName(), entity, metadata, deploymentName, container, replicas, secrets);
            Service service = exposeService(namespace.getMetadata().getName(), metadata, deploymentName, inboundPorts);
            Pod pod = getPod(namespace.getMetadata().getName(), metadata, client);

            entity.sensors().set(KubernetesPod.KUBERNETES_NAMESPACE, namespace.getMetadata().getName());
            entity.sensors().set(KubernetesPod.KUBERNETES_DEPLOYMENT, deploymentName);
            entity.sensors().set(KubernetesPod.KUBERNETES_POD, pod.getMetadata().getName());
            entity.sensors().set(KubernetesPod.KUBERNETES_SERVICE, service.getMetadata().getName());

            LocationSpec<KubernetesSshMachineLocation> locationSpec = prepareSshableLocationSpec(entity, setup, service, pod)
                    .configure(KubernetesMachineLocation.KUBERNETES_NAMESPACE, namespace.getMetadata().getName())
                    .configure(KubernetesMachineLocation.KUBERNETES_RESOURCE_NAME, deploymentName)
                    .configure(KubernetesMachineLocation.KUBERNETES_RESOURCE_TYPE, getContainerResourceType());

            machine = getManagementContext().getLocationManager().createLocation(locationSpec);
            registerPortMappings(machine, entity, service);
            if (!isDockerContainer(entity)) {
                waitForSshable(machine, Duration.FIVE_MINUTES);
            }
        }
        return machine;
    }

    protected String getContainerResourceType() {
        return KubernetesResource.DEPLOYMENT;
    }

    protected KubernetesMachineLocation createKubernetesHelmChartLocation(Entity entity, ConfigBag setup) {
        try (KubernetesClient client = getClient()){
            Map<String, String> podLabels = ImmutableMap.of("name", "tiller", "app", "helm");
            if (!isTillerInstalled("kube-system", podLabels, client)) {
                installTillerPodAndService("kube-system", "tiller-deploy", podLabels, client);
            }
            // Create ${HOME}/.helm/{cache/archive,repository/cache}
            try {
                Files.createDirectories(Paths.get(System.getProperty("user.home"),".helm", "cache", "archive"));
                Files.createDirectories(Paths.get(System.getProperty("user.home"),".helm", "repository", "cache"));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }

            tiller = Suppliers.memoize(new TillerSupplier((DefaultKubernetesClient) client)).get();
            String chartName = entity.config().get(KubernetesHelmChart.CHART_NAME);
            String chartVersion = entity.config().get(KubernetesHelmChart.CHART_VERSION);

            ReleaseManager chartManager = new ReleaseManager(tiller);
            InstallReleaseRequest.Builder requestBuilder = InstallReleaseRequest.newBuilder();
            requestBuilder.setTimeout(300L);
            requestBuilder.setWait(true);

            ChartRepository chartRepository = new ChartRepository("stable", new URI("https://kubernetes-charts.storage.googleapis.com/"));
            if (chartVersion == null) {
                ChartRepository.Index.Entry latest = chartRepository.getIndex().getEntries().get(chartName).first();
                chartVersion = latest.getVersion();
            }

            Chart.Builder chartBuilder = chartRepository.resolve(chartName, chartVersion);

            Future<InstallReleaseResponse> releaseFuture = chartManager.install(requestBuilder, chartBuilder);
            Release release = releaseFuture.get().getRelease();

            String resourceName = release.getName();
            String namespace = release.getNamespace();
            LOG.debug("Resource {} (from chart {}) deployed to {}", new Object[]{resourceName, chartName, namespace});

            Node node = Iterables.getFirst(client.nodes().list().getItems(), null); // null should never happen here
            String nodeAddress = node.getStatus().getAddresses().get(0).getAddress();
            InetAddress inetAddress = Networking.getInetAddressWithFixedName(nodeAddress);

            entity.sensors().set(KubernetesPod.KUBERNETES_NAMESPACE, namespace);
            entity.sensors().set(KubernetesResource.RESOURCE_NAME, resourceName);

            LocationSpec<? extends KubernetesMachineLocation> locationSpec = LocationSpec.create(KubernetesEmptyMachineLocation.class);
            locationSpec.configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT))
                    .configure(KubernetesMachineLocation.KUBERNETES_NAMESPACE, namespace)
                    .configure(KubernetesMachineLocation.KUBERNETES_RESOURCE_NAME, resourceName)
                    .configure("address", inetAddress)
                    .configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.of(nodeAddress));

            KubernetesMachineLocation machine = getManagementContext().getLocationManager().createLocation(locationSpec);

            String serviceName = String.format("%s-%s", resourceName, chartName);
            Service service = getService(namespace, serviceName, client);
            registerPortMappings(machine, entity, service);
            return machine;

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private void installTillerPodAndService(String namespace, String serviceName, Map<String, String> podLabels, KubernetesClient client) {
        TillerInstaller installer = new TillerInstaller();
        installer.init(true);
        getPod(namespace, podLabels, client);
        getService(namespace, serviceName, client);
    }

    private boolean isTillerInstalled(String namespace, Map<String, String> podLabels, KubernetesClient client) {
        return !Iterables.isEmpty(client.pods().inNamespace(namespace).withLabels(podLabels).list().getItems());
    }

    protected void waitForSshable(final SshMachineLocation machine, Duration timeout) {
        Callable<Boolean> checker = () -> {
            int exitstatus = machine.execScript(
                    ImmutableMap.of( // TODO investigate why SSH connection does not time out with this config
                            SshTool.PROP_CONNECT_TIMEOUT.getName(), Duration.TEN_SECONDS.toMilliseconds(),
                            SshTool.PROP_SESSION_TIMEOUT.getName(), Duration.TEN_SECONDS.toMilliseconds(),
                            SshTool.PROP_SSH_TRIES_TIMEOUT.getName(), Duration.TEN_SECONDS.toMilliseconds(),
                            SshTool.PROP_SSH_TRIES.getName(), 1),
                    "check-sshable",
                    ImmutableList.of("true"));
            return (exitstatus == 0);
        };

        Stopwatch stopwatch = Stopwatch.createStarted();
        ReferenceWithError<Boolean> reachable = Repeater.create("reachable")
                .threaded()
                .backoff(Duration.FIVE_SECONDS, 2, Duration.TEN_SECONDS) // Exponential backoff, to 10 seconds
                .until(checker)
                .limitTimeTo(timeout)
                .runKeepingError();
        if (!reachable.getWithoutError()) {
            throw new IllegalStateException("Connection failed for " + machine.getSshHostAndPort() + " after waiting " + stopwatch.elapsed(TimeUnit.SECONDS), reachable.getError());
        } else {
            LOG.debug("Connection succeeded for {} after {}", machine.getSshHostAndPort(), stopwatch.elapsed(TimeUnit.SECONDS));
        }
    }

    protected void registerPortMappings(KubernetesMachineLocation machine, Entity entity, Service service) {
        PortForwardManager portForwardManager = (PortForwardManager) getManagementContext().getLocationRegistry()
                .getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        List<ServicePort> ports = service.getSpec().getPorts();
        String publicHostText;
        if (machine instanceof SshMachineLocation) {
            publicHostText = ((SshMachineLocation) machine).getSshHostAndPort().getHostText();
        } else {
            publicHostText = Iterables.getFirst(machine.config().get(SshMachineLocation.PRIVATE_ADDRESSES), null);
        }
        LOG.debug("Recording port-mappings for container {} of {}: {}", new Object[]{machine, this, ports});

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
                AttributeSensor<Integer> sensor = Sensors.newIntegerSensor("kubernetes." + Strings.maybeNonBlank(port.getName()).or(targetPort.toString()) + ".port");
                entity.sensors().set(sensor, targetPort);
            }
        }

        entity.enrichers().add(EnricherSpec.create(OnPublicNetworkEnricher.class)
                .configure(AbstractOnNetworkEnricher.MAP_MATCHING, "kubernetes.[a-zA-Z0-9][a-zA-Z0-9-_]*.port"));
    }

    protected synchronized Namespace createOrGetNamespace(final String name, Boolean create) {
        try (KubernetesClient client = getClient()) {
            Namespace namespace = client.namespaces().withName(name).get();
            ExitCondition namespaceReady = new ExitCondition() {
                @Override
                public Boolean call() {
                    Namespace actualNamespace = client.namespaces().withName(name).get();
                    return actualNamespace != null && actualNamespace.getStatus().getPhase().equals(PHASE_ACTIVE);
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
                namespace = client.namespaces().create(new NamespaceBuilder().withNewMetadata().withName(name).addToLabels("name", name).endMetadata().build());
                LOG.debug("Created namespace {}.", namespace);
            } else {
                throw new IllegalStateException("Namespace " + name + " does not exist and namespace.create is not set");
            }
            waitForExitCondition(namespaceReady);
            return client.namespaces().withName(name).get();
        }
    }

    protected Pod getPod(final String namespace, final String name, KubernetesClient client) {
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
        return client.pods().inNamespace(namespace).withName(name).get();
    }

    protected Pod getPod(final String namespace, final Map<String, String> metadata, KubernetesClient client) {
        ExitCondition exitCondition = new ExitCondition() {
            @Override
            public Boolean call() {
                PodList result = client.pods().inNamespace(namespace).withLabels(metadata).list();
                return result.getItems().size() >= 1 &&
                        Iterables.any(result.getItems().get(0).getStatus().getConditions(), new Predicate<PodCondition>() {
                            @Override
                            public boolean apply(@Nullable PodCondition input) {
                                return input.getStatus().equals("True");
                            }
                        });
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
        try (KubernetesClient client = getClient()) {
            Secret secret = client.secrets().inNamespace(namespace).withName(secretName).get();
            if (secret != null) return secret;

            String json = String.format("{\"https://index.docker.io/v1/\":{\"auth\":\"%s\"}}", auth);
            String base64encoded = BaseEncoding.base64().encode(json.getBytes(Charset.defaultCharset()));
            secret = new SecretBuilder()
                    .withNewMetadata()
                    .withName(secretName)
                    .endMetadata()
                    .withType(KUBERNETES_DOCKERCFG)
                    .withData(ImmutableMap.of(".dockercfg", base64encoded))
                    .build();
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
    }

    protected Container buildContainer(String namespace, Map<String, String> metadata, String deploymentName, String imageName, Iterable<Integer> inboundPorts, Map<String, ?> env, Map<String, String> limits, boolean privileged) {
        List<ContainerPort> containerPorts = Lists.newArrayList();
        inboundPorts.forEach(inboundPort -> containerPorts.add(new ContainerPortBuilder().withContainerPort(inboundPort).build()));

        List<EnvVar> envVars = Lists.newArrayList();
        env.forEach((key,value) -> envVars.add(new EnvVarBuilder().withName(key).withValue(value.toString()).build()));

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
                ResourceRequirements resourceRequirements = new ResourceRequirementsBuilder().addToLimits(nameValueEntry.getKey(),
                        new QuantityBuilder().withAmount(nameValueEntry.getValue()).build()).build();
                containerBuilder.withResources(resourceRequirements);
            }
        }
        LOG.debug("Built container {} to be deployed in namespace {} with metadata {}.", containerBuilder.build(), namespace, metadata);
        return containerBuilder.build();
    }

    protected void deploy(final String namespace, Entity entity, Map<String, String> metadata, final String deploymentName,
                          Container container, final Integer replicas, Map<String, String> secrets) {
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

        LabelSelectorBuilder labelSelectorBuilder = new LabelSelectorBuilder();
        labelSelectorBuilder.addToMatchLabels(metadata);
        LabelSelector labelSelector = labelSelectorBuilder.build();

        Deployment deployment = new DeploymentBuilder()
                .withNewMetadata()
                .withName(deploymentName)
                .addToAnnotations(BROOKLYN_ENTITY_ID, entity.getId())
                .addToAnnotations(BROOKLYN_APPLICATION_ID, entity.getApplicationId())
                .endMetadata()
                .withNewSpec()
                .withSelector(labelSelector)
                .withReplicas(replicas)
                .withTemplate(template)
                .endSpec()
                .build();
        try (KubernetesClient client = getClient()) {
            client.apps().deployments().inNamespace(namespace).create(deployment);
            ExitCondition exitCondition = new ExitCondition() {
                @Override
                public Boolean call() {
                    Deployment dep = client.apps().deployments().inNamespace(namespace).withName(deploymentName).get();
                    DeploymentStatus status = (dep == null) ? null : dep.getStatus();
                    Integer replicas = (status == null) ? null : status.getAvailableReplicas();
                    return replicas != null;
                }

                @Override
                public String getFailureMessage() {
                    Deployment dep = client.apps().deployments().inNamespace(namespace).withName(deploymentName).get();
                    DeploymentStatus status = (dep == null) ? null : dep.getStatus();
                    return "Namespace=" + namespace + "; deploymentName= " + deploymentName + "; Deployment=" + dep
                            + "; status=" + status
                            + "; availableReplicas=" + (status == null ? "null" : status.getAvailableReplicas());
                }
            };
            waitForExitCondition(exitCondition);
        }
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
                .withType(NODE_PORT)
                .endSpec()
                .build();
        try (KubernetesClient client = getClient()) {
            client.services().inNamespace(namespace).create(service);

            service = getService(namespace, serviceName, client);
            LOG.debug("Exposed service {} in namespace {}.", service, namespace);
            return service;
        }
    }

    protected Service getService(final String namespace, final String serviceName, KubernetesClient client) {
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
                    if (!subset.getNotReadyAddresses().isEmpty() &&
                            subset.getAddresses().isEmpty() &&
                            subset.getNotReadyAddresses().size() > 0) {
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

    protected LocationSpec<KubernetesSshMachineLocation> prepareSshableLocationSpec(Entity entity, ConfigBag setup, Service service, Pod pod) {
        InetAddress node = Networking.getInetAddressWithFixedName(pod.getSpec().getNodeName());
        String podAddress = pod.getStatus().getPodIP();
        LocationSpec<KubernetesSshMachineLocation> locationSpec = LocationSpec.create(KubernetesSshMachineLocation.class)
                .configure(ADDRESS_KEY, node)
                .configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableSet.of(podAddress))
                .configure(CALLER_CONTEXT, setup.get(CALLER_CONTEXT));
        if (!isDockerContainer(entity)) {
            Optional<ServicePort> sshPort = Iterables.tryFind(service.getSpec().getPorts(), input -> "TCP".equalsIgnoreCase(input.getProtocol()) && input.getPort() == 22);
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

    protected void createPersistentVolumes(List<String> volumes, KubernetesClient client) {
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
                            && pv.getStatus().getPhase().equals(PHASE_AVAILABLE);
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
        if (callerContext instanceof Entity) {
            return (Entity) callerContext;
        } else {
            throw new IllegalStateException("Invalid caller context: " + callerContext);
        }
    }

    protected Entity validateCallerContext(MachineLocation machine) {
        // Lookup entity flags
        Object callerContext = machine.config().get(LocationConfigKeys.CALLER_CONTEXT);
        if (callerContext instanceof Entity) {
            return (Entity) callerContext;
        } else {
            throw new IllegalStateException("Invalid caller context: " + callerContext);
        }
    }

    protected Map<String, String> findMetadata(Entity entity, ConfigBag setup, String value) {
        Map<String, String> podMetadata = Maps.newLinkedHashMap();
        podMetadata.put(isDockerContainer(entity) ? IMMUTABLE_CONTAINER_KEY : SSHABLE_CONTAINER, value);

        Map<String, Object> metadata = MutableMap.<String, Object>builder()
                .putAll(MutableMap.copyOf(setup.get(KubernetesPod.METADATA)))
                .putAll(MutableMap.copyOf(entity.config().get(KubernetesPod.METADATA)))
                .putAll(podMetadata)
                .build();
        return Maps.transformValues(metadata, Functions.toStringFunction());
    }

    /**
     * Sets the {@code BROOKLYN_ROOT_PASSWORD} variable in the container environment if appropriate.
     * This is (approximately) the same behaviour as the {@link DockerJcloudsLocation} used for
     * Swarm.
     * <p>
     * Side-effects the location {@code config} to set the {@link KubernetesLocationConfig#LOGIN_USER_PASSWORD loginUser.password}
     * if one is auto-generated. Note that this injected value overrides any other settings configured for the
     * container environment.
     */
    protected Map<String, String> findEnvironmentVariables(Entity entity, ConfigBag setup, String imageName) {
        String loginUser = setup.get(LOGIN_USER);
        String loginPassword = setup.get(LOGIN_USER_PASSWORD);
        Map<String, String> injections = Maps.newLinkedHashMap();

        // Check if login credentials should be injected
        Boolean injectLoginCredentials = setup.get(INJECT_LOGIN_CREDENTIAL);
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
                setup.configure(LOGIN_USER, loginUser);

                if (Strings.isBlank(loginPassword)) {
                    loginPassword = Identifiers.makeRandomPassword(12);
                    setup.configure(LOGIN_USER_PASSWORD, loginPassword);
                }

                injections.put(BROOKLYN_ROOT_PASSWORD, loginPassword);
            }
        }

        Map<String, Object> rawEnv = MutableMap.<String, Object>builder()
                .putAll(MutableMap.copyOf(setup.get(ENV)))
                .putAll(MutableMap.copyOf(entity.config().get(DockerContainer.CONTAINER_ENVIRONMENT)))
                .putAll(injections)
                .build();
        return Maps.transformValues(rawEnv, Functions.toStringFunction());
    }

    protected Iterable<Integer> findInboundPorts(Entity entity, ConfigBag setup) {
        Iterable<String> inboundTcpPorts = entity.config().get(DockerContainer.INBOUND_TCP_PORTS);
        if(inboundTcpPorts == null)
            return setup.containsKey(INBOUND_PORTS) ? toIntPortList(setup.get(INBOUND_PORTS)) : ImmutableList.of(22);

        List<Integer> inboundPorts = Lists.newArrayList();
        List<String> portRanges = MutableList.copyOf(entity.config().get(DockerContainer.INBOUND_TCP_PORTS));
        for (String portRange : portRanges) {
            for (Integer port : PortRanges.fromString(portRange)) {
                inboundPorts.add(port);
            }
        }
        return inboundPorts;
    }

    protected List<Integer> toIntPortList(Object v) {
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
        return implementsInterface(entity, DockerContainer.class);
    }

    protected boolean isKubernetesPod(Entity entity) {
        return implementsInterface(entity, KubernetesPod.class);
    }

    protected boolean isKubernetesResource(Entity entity) {
        return implementsInterface(entity, KubernetesResource.class);
    }

    protected boolean isKubernetesHelmChart(Entity entity) {
        return implementsInterface(entity, KubernetesHelmChart.class);
    }

    public boolean implementsInterface(Entity entity, Class<?> type) {
        return Iterables.tryFind(Arrays.asList(entity.getClass().getInterfaces()), Predicates.assignableFrom(type)).isPresent();
    }

    @Override
    public MachineProvisioningLocation<KubernetesMachineLocation> newSubLocation(Map<?, ?> newFlags) {
        throw new UnsupportedOperationException();
    }

    /** @see {@link #lookup(ConfigKey, Entity, ConfigBag, Object)} */
    public <T> T lookup(ConfigKey<T> config, Entity entity, ConfigBag setup) {
        return lookup(config, entity, setup, config.getDefaultValue());
    }

    /**
     * Looks up {@link ConfigKey configuration} with the entity value taking precedence over the
     * location, and returning a default value (normally {@literal null}) if neither is present.
     */
    public <T> T lookup(final ConfigKey<T> config, Entity entity, ConfigBag setup, T defaultValue) {
        boolean entityConfigPresent = !entity.config().findKeysPresent(config::equals).isEmpty();

        boolean setupBagConfigPresent = setup.containsKey(config);

        if (entityConfigPresent) {
            return entity.config().get(config);
        } else if (setupBagConfigPresent) {
            return setup.get(config);
        }

        return defaultValue;
    }

    public void waitForExitCondition(ExitCondition exitCondition) {
        waitForExitCondition(exitCondition, Duration.ONE_SECOND, Duration.FIVE_MINUTES);
    }

    public void waitForExitCondition(ExitCondition exitCondition, Duration initial, Duration duration) {
        ReferenceWithError<Boolean> result = Repeater.create()
                .backoff(initial, 1.2, duration)
                .limitTimeTo(duration)
                .until(exitCondition)
                .runKeepingError();
        if (!Boolean.TRUE.equals(result.get())) {
            String err = String.format("Exit condition unsatisfied after %s: %s", duration, exitCondition.getFailureMessage());
            LOG.info("{} (rethrowing)", err);
            throw new IllegalStateException(err);
        }
    }

    public interface ExitCondition extends Callable<Boolean> {
        String getFailureMessage();
    }
}
