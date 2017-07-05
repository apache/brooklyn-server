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

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.container.entity.docker.DockerContainer;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocationConfig;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.math.MathPredicates;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

@ImplementedBy(KubernetesPodImpl.class)
public interface KubernetesPod extends DockerContainer {

    ConfigKey<String> NAMESPACE = KubernetesLocationConfig.NAMESPACE;

    ConfigKey<Boolean> PRIVILEGED = KubernetesLocationConfig.PRIVILEGED;

    @SuppressWarnings("serial")
    ConfigKey<List<String>> PERSISTENT_VOLUMES = ConfigKeys.builder(new TypeToken<List<String>>() {
    })
            .name("persistentVolumes")
            .description("Persistent volumes used by the pod")
            .build();

    ConfigKey<String> DEPLOYMENT = ConfigKeys.builder(String.class)
            .name("deployment")
            .description("The name of the service the deployed pod will use")
            .build();

    ConfigKey<Integer> REPLICAS = ConfigKeys.builder(Integer.class)
            .name("replicas")
            .description("Number of replicas in the pod")
            .constraint(MathPredicates.greaterThanOrEqual(1d))
            .defaultValue(1)
            .build();

    @SuppressWarnings("serial")
    ConfigKey<Map<String, String>> SECRETS = ConfigKeys.builder(new TypeToken<Map<String, String>>() {
    })
            .name("secrets")
            .description("Secrets to be added to the pod")
            .build();

    @SuppressWarnings("serial")
    ConfigKey<Map<String, String>> LIMITS = ConfigKeys.builder(new TypeToken<Map<String, String>>() {
    })
            .name("limits")
            .description("Container resource limits for the pod")
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

    String EMPTY = "Empty";
}
