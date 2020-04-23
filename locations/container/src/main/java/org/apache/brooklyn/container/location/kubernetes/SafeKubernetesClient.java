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
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.*;
import org.apache.brooklyn.util.core.config.ResolvingConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static org.apache.brooklyn.container.entity.kubernetes.KubernetesResource.*;

/**
 * Wrapper around the Kubernetes client making sure that the token never expires
 */
public class SafeKubernetesClient {
    public static final String ENDPOINTS = "Endpoints";

    private static final Logger LOG = LoggerFactory.getLogger(SafeKubernetesClient.class);

    private KubernetesClient client;
    private KubernetesLocation kubernetesLocation;

    public SafeKubernetesClient(KubernetesClient client, KubernetesLocation kubernetesLocation) {
        this.client = client;
        this.kubernetesLocation = kubernetesLocation;
    }

    public MixedOperation<Service, ServiceList, DoneableService, ServiceResource<Service, DoneableService>> services() {
        reinitIfTokenExpired();
        return client.services();
    }

    public NonNamespaceOperation<Namespace, NamespaceList, DoneableNamespace, Resource<Namespace, DoneableNamespace>> namespaces() {
        reinitIfTokenExpired();
        return client.namespaces();
    }

    public AppsAPIGroupDSL apps() {
        reinitIfTokenExpired();
        return client.apps();
    }

    public ParameterNamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata, Boolean> load(InputStream is) {
        reinitIfTokenExpired();
        return client.load(is);
    }

    public NamespaceVisitFromServerGetWatchDeleteRecreateWaitApplicable<HasMetadata, Boolean> resource(HasMetadata item) {
        reinitIfTokenExpired();
        return client.resource(item);
    }

    public MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets() {
        reinitIfTokenExpired();
        return client.secrets();
    }

    public NonNamespaceOperation<PersistentVolume, PersistentVolumeList, DoneablePersistentVolume, Resource<PersistentVolume, DoneablePersistentVolume>> persistentVolumes() {
        reinitIfTokenExpired();
        return client.persistentVolumes();
    }

    public MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods() {
        reinitIfTokenExpired();
        return client.pods();
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

    protected Namespace getNamespace(String name ){
        reinitIfTokenExpired();
        return client.namespaces().withName(name).get();
    }

    protected Resource<?,?> getResource(String resourceType, String resourceName, String namespaceName) {
        reinitIfTokenExpired();
        try {
            switch (resourceType) {
                case ENDPOINTS:
                    return client.endpoints().inNamespace(namespaceName).withName(resourceName);
                case DEPLOYMENT:
                    return client.apps().deployments().inNamespace(namespaceName).withName(resourceName);
                case REPLICA_SET:
                    return client.apps().replicaSets().inNamespace(namespaceName).withName(resourceName);
                case CONFIG_MAP:
                    return client.configMaps().inNamespace(namespaceName).withName(resourceName);
                case PERSISTENT_VOLUME:
                    return client.persistentVolumes().withName(resourceName);
                case SECRET:
                    return client.secrets().inNamespace(namespaceName).withName(resourceName);
                case SERVICE:
                    return client.services().inNamespace(namespaceName).withName(resourceName);
                case REPLICATION_CONTROLLER:
                    return client.replicationControllers().inNamespace(namespaceName).withName(resourceName);
            }
        }catch (KubernetesClientException kce) {
                LOG.warn("Unable to retrieve resource {}: {}", resourceName, kce);
        }
        return null;
    }

    protected boolean isNamespaceEmpty(String name) {
        reinitIfTokenExpired();
        return client.apps().deployments().inNamespace(name).list().getItems().isEmpty() &&
                client.services().inNamespace(name).list().getItems().isEmpty() &&
                client.secrets().inNamespace(name).list().getItems().isEmpty();
    }

    protected boolean deleteResource(String resourceType, String resourceName, String namespace) {
        reinitIfTokenExpired();
        try {
            switch (resourceType) {
                case DEPLOYMENT:
                    return client.apps().deployments().inNamespace(namespace).withName(resourceName).delete();
                case REPLICA_SET:
                    return client.apps().replicaSets().inNamespace(namespace).withName(resourceName).delete();
                case CONFIG_MAP:
                    return client.configMaps().inNamespace(namespace).withName(resourceName).delete();
                case PERSISTENT_VOLUME:
                    return client.persistentVolumes().withName(resourceName).delete();
                case SECRET:
                    return client.secrets().inNamespace(namespace).withName(resourceName).delete();
                case SERVICE:
                    return client.services().inNamespace(namespace).withName(resourceName).delete();
                case REPLICATION_CONTROLLER:
                    return client.replicationControllers().inNamespace(namespace).withName(resourceName).delete();
                case NAMESPACE:
                    return client.namespaces().withName(resourceName).delete();
            }
        } catch (KubernetesClientException kce) {
            LOG.warn("Error deleting resource {}: {}", resourceName, kce);
        }
        return false;
    }

}
