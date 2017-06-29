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
package org.apache.brooklyn.container.entity.kubernetes;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.util.core.ResourcePredicates;

@ImplementedBy(KubernetesResourceImpl.class)
public interface KubernetesResource extends SoftwareProcess {

    ConfigKey<String> RESOURCE_FILE = ConfigKeys.builder(String.class)
            .name("resource")
            .description("Kubernetes resource YAML file URI")
            .constraint(ResourcePredicates.urlExists())
            .build();

    AttributeSensor<String> RESOURCE_TYPE = Sensors.builder(String.class, "kubernetes.resource.type")
            .description("Kubernetes resource type")
            .build();

    AttributeSensor<String> RESOURCE_NAME = Sensors.builder(String.class, "kubernetes.resource.name")
            .description("Kubernetes resource name")
            .build();

    AttributeSensor<String> KUBERNETES_NAMESPACE = KubernetesPod.KUBERNETES_NAMESPACE;

    String POD = "Pod";
    String DEPLOYMENT = "Deployment";
    String REPLICA_SET = "ReplicaSet";
    String CONFIG_MAP = "ConfigMap";
    String PERSISTENT_VOLUME = "PersistentVolume";
    String SECRET = "Secret";
    String SERVICE = "Service";
    String REPLICATION_CONTROLLER = "ReplicationController";
    String NAMESPACE = "Namespace";

}
