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

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apiextensions.DoneableCustomResourceDefinition;
import io.fabric8.kubernetes.api.model.coordination.v1.DoneableLease;
import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.VersionInfo;
import io.fabric8.kubernetes.client.dsl.*;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.dsl.internal.RawCustomResourceOperationsImpl;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectorBuilder;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * Wrapper implementation for a KubernetesClient that checks if the authentication token is expired before every client call and
 * re-initializes the client with a new token.
 */
public class SafeKubernetesClient implements KubernetesClient {

    private static final Logger LOG = LoggerFactory.getLogger(SafeKubernetesClient.class);

    private KubernetesClient client;
    private final KubernetesLocation kubernetesLocation;

    public SafeKubernetesClient(KubernetesClient client, KubernetesLocation kubernetesLocation) {
        this.client = client;
        this.kubernetesLocation = kubernetesLocation;
    }

    /**
     * Method that checks before every client method invocation if the token is still valid and if not the client is re-initialized with a new token.
     */
    private void reinitIfTokenExpired(){
        try {
            client.namespaces().list().getItems();
        } catch (Exception e) {
            if (e instanceof KubernetesClientException) {
                KubernetesClientException kce = (KubernetesClientException) e;
                if(kce.getCode() == 401 ) {
                    LOG.debug("Kubernetes Token is being re-initialized for location {}.", kubernetesLocation.getDisplayName());
                    KubernetesClientRegistry registry = kubernetesLocation.getConfig(KubernetesLocationConfig.KUBERNETES_CLIENT_REGISTRY);
                    this.client = registry.getKubernetesClient(ResolvingConfigBag.newInstanceExtending(kubernetesLocation.getManagementContext(), kubernetesLocation.config().getBag()));
                }
            } else {
                LOG.error("Unexpected KubernetesClient Exception: ", e);
            }
        }
    }

    @Override
    public MixedOperation<Service, ServiceList, DoneableService, ServiceResource<Service, DoneableService>> services() {
        reinitIfTokenExpired();
        return client.services();
    }

    @Override
    public AppsAPIGroupDSL apps() {
        reinitIfTokenExpired();
        return client.apps();
    }

    @Override
    public MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> configMaps() {
        reinitIfTokenExpired();
        return client.configMaps();
    }

    @Override
    public NonNamespaceOperation<PersistentVolume, PersistentVolumeList, DoneablePersistentVolume, Resource<PersistentVolume, DoneablePersistentVolume>> persistentVolumes() {
        reinitIfTokenExpired();
        return client.persistentVolumes();
    }

    @Override
    public MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets() {
        reinitIfTokenExpired();
        return client.secrets();
    }

    @Override
    public MixedOperation<ReplicationController, ReplicationControllerList, DoneableReplicationController, RollableScalableResource<ReplicationController, DoneableReplicationController>> replicationControllers() {
        reinitIfTokenExpired();
        return client.replicationControllers();
    }

    @Override
    public NonNamespaceOperation<Namespace, NamespaceList, DoneableNamespace, Resource<Namespace, DoneableNamespace>> namespaces() {
        reinitIfTokenExpired();
        return client.namespaces();
    }

    @Override
    public NamespaceVisitFromServerGetWatchDeleteRecreateWaitApplicable<HasMetadata, Boolean> resource(HasMetadata item) {
        reinitIfTokenExpired();
        return client.resource(item);
    }

    @Override
    public MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> endpoints() {
        reinitIfTokenExpired();
        return client.endpoints();
    }

    @Override
    public MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods() {
        reinitIfTokenExpired();
        return client.pods();
    }

    ///// Methods from the client that we currently do not use

    @Override
    public NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, DoneableCustomResourceDefinition, Resource<CustomResourceDefinition, DoneableCustomResourceDefinition>> customResourceDefinitions() {
        reinitIfTokenExpired();
        return client.customResourceDefinitions();
    }

    @Override
    public <T extends HasMetadata, L extends KubernetesResourceList, D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>> customResources(CustomResourceDefinition crd, Class<T> resourceType, Class<L> listClass, Class<D> doneClass) {
        reinitIfTokenExpired();
        return client.customResources(crd, resourceType, listClass, doneClass);
    }

    @Override
    public <T extends HasMetadata, L extends KubernetesResourceList, D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>> customResource(CustomResourceDefinition crd, Class<T> resourceType, Class<L> listClass, Class<D> doneClass) {
        reinitIfTokenExpired();
        return client.customResource(crd, resourceType, listClass, doneClass);
    }

    @Override
    public ExtensionsAPIGroupDSL extensions() {
        reinitIfTokenExpired();
        return client.extensions();
    }

    @Override
    public VersionInfo getVersion() {
        reinitIfTokenExpired();
        return client.getVersion();
    }

    @Override
    public RawCustomResourceOperationsImpl customResource(CustomResourceDefinitionContext customResourceDefinition) {
        reinitIfTokenExpired();
        return client.customResource(customResourceDefinition);
    }

    @Override
    public AutoscalingAPIGroupDSL autoscaling() {
        reinitIfTokenExpired();
        return client.autoscaling();
    }

    @Override
    public NetworkAPIGroupDSL network() {
        reinitIfTokenExpired();
        return client.network();
    }

    @Override
    public StorageAPIGroupDSL storage() {
        reinitIfTokenExpired();
        return client.storage();
    }

    @Override
    public SettingsAPIGroupDSL settings() {
        reinitIfTokenExpired();
        return client.settings();
    }

    @Override
    public BatchAPIGroupDSL batch() {
        reinitIfTokenExpired();
        return client.batch();
    }

    @Override
    public MetricAPIGroupDSL top() {
        reinitIfTokenExpired();
        return client.top();
    }

    @Override
    public PolicyAPIGroupDSL policy() {
        reinitIfTokenExpired();
        return client.policy();
    }

    @Override
    public RbacAPIGroupDSL rbac() {
        reinitIfTokenExpired();
        return client.rbac();
    }

    @Override
    public SchedulingAPIGroupDSL scheduling() {
        reinitIfTokenExpired();
        return client.scheduling();
    }

    @Override
    public MixedOperation<ComponentStatus, ComponentStatusList, DoneableComponentStatus, Resource<ComponentStatus, DoneableComponentStatus>> componentstatuses() {
        reinitIfTokenExpired();
        return client.componentstatuses();
    }

    @Override
    public ParameterNamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata, Boolean> load(InputStream is) {
        reinitIfTokenExpired();
        return client.load(is);
    }

    @Override
    public ParameterNamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata, Boolean> resourceList(String s) {
        reinitIfTokenExpired();
        return client.resourceList(s);
    }

    @Override
    public NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata, Boolean> resourceList(KubernetesResourceList list) {
        reinitIfTokenExpired();
        return client.resourceList(list);
    }

    @Override
    public NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata, Boolean> resourceList(HasMetadata... items) {
        reinitIfTokenExpired();
        return client.resourceList(items);
    }

    @Override
    public NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata, Boolean> resourceList(Collection<HasMetadata> items) {
        reinitIfTokenExpired();
        return client.resourceList(items);
    }

    @Override
    public NamespaceVisitFromServerGetWatchDeleteRecreateWaitApplicable<HasMetadata, Boolean> resource(String s) {
        reinitIfTokenExpired();
        return client.resource(s);
    }

    @Override
    public MixedOperation<Binding, KubernetesResourceList, DoneableBinding, Resource<Binding, DoneableBinding>> bindings() {
        reinitIfTokenExpired();
        return client.bindings();
    }

    @Override
    public MixedOperation<Event, EventList, DoneableEvent, Resource<Event, DoneableEvent>> events() {
        reinitIfTokenExpired();
        return client.events();
    }

    @Override
    public NonNamespaceOperation<Node, NodeList, DoneableNode, Resource<Node, DoneableNode>> nodes() {
        reinitIfTokenExpired();
        return client.nodes();
    }

    @Override
    public MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> persistentVolumeClaims() {
        reinitIfTokenExpired();
        return client.persistentVolumeClaims();
    }

    @Override
    public MixedOperation<ResourceQuota, ResourceQuotaList, DoneableResourceQuota, Resource<ResourceQuota, DoneableResourceQuota>> resourceQuotas() {
        reinitIfTokenExpired();
        return client.resourceQuotas();
    }

    @Override
    public MixedOperation<ServiceAccount, ServiceAccountList, DoneableServiceAccount, Resource<ServiceAccount, DoneableServiceAccount>> serviceAccounts() {
        reinitIfTokenExpired();
        return client.serviceAccounts();
    }

    @Override
    public KubernetesListMixedOperation lists() {
        reinitIfTokenExpired();
        return client.lists();
    }

    @Override
    public MixedOperation<LimitRange, LimitRangeList, DoneableLimitRange, Resource<LimitRange, DoneableLimitRange>> limitRanges() {
        reinitIfTokenExpired();
        return client.limitRanges();
    }

    @Override
    public SubjectAccessReviewDSL subjectAccessReviewAuth() {
        reinitIfTokenExpired();
        return client.subjectAccessReviewAuth();
    }

    @Override
    public SharedInformerFactory informers() {
        reinitIfTokenExpired();
        return null;
    }

    @Override
    public SharedInformerFactory informers(ExecutorService executorService) {
        reinitIfTokenExpired();
        return client.informers(executorService);
    }

    @Override
    public <C extends Namespaceable<C> & KubernetesClient> LeaderElectorBuilder<C> leaderElector() {
        reinitIfTokenExpired();
        return client.leaderElector();
    }

    @Override
    public MixedOperation<Lease, LeaseList, DoneableLease, Resource<Lease, DoneableLease>> leases() {
        reinitIfTokenExpired();
        return client.leases();
    }

    @Override
    public <C> Boolean isAdaptable(Class<C> type) {
        return client.isAdaptable(type);
    }

    @Override
    public <C> C adapt(Class<C> type) {
        return client.adapt(type);
    }

    @Override
    public URL getMasterUrl() {
        return client.getMasterUrl();
    }

    @Override
    public String getApiVersion() {
        return client.getApiVersion();
    }

    @Override
    public String getNamespace() {
        return client.getNamespace();
    }

    @Override
    public RootPaths rootPaths() {
        return client.rootPaths();
    }

    @Override
    public boolean supportsApiPath(String path) {
        return client.supportsApiPath(path);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public Config getConfiguration() {
        return client.getConfiguration();
    }
}
