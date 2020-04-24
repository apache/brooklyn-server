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
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.*;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SafeKubernetesClient extends DefaultKubernetesClient {

    private static final Logger LOG = LoggerFactory.getLogger(SafeKubernetesClient.class);

    private KubernetesClient client;
    private KubernetesLocation kubernetesLocation;

    public SafeKubernetesClient(KubernetesClient client, KubernetesLocation kubernetesLocation) {
        this.client = client;
        this.kubernetesLocation = kubernetesLocation;
    }

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
}
