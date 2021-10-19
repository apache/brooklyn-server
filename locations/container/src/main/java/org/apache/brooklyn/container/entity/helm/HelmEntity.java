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
package org.apache.brooklyn.container.entity.helm;

import java.util.List;

import com.google.common.reflect.TypeToken;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocation;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.annotation.EffectorParam;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.sensor.Sensors;

/**
 * Deploys a Helm Chart to a {@link KubernetesLocation}.
 *
 * <pre>{@code
 * location: kubernetes-location
 * services:
 * - type: org.apache.brooklyn.container.entity.helm.HelmEntity
 *   brooklyn.config:
 *     repo.name: bitnami
 *     repo.url: https://charts.bitnami.com/bitnami
 *     helm.template: bitnami/nginx
 *     helm.deployment.name: nginx
 * }</pre>
 */
@ImplementedBy(HelmEntityImpl.class)
public interface HelmEntity extends Entity, Startable {

    ConfigKey<String> REPO_NAME = ConfigKeys.newStringConfigKey(
            "repo.name",
            "Name for the Helm repository");

    ConfigKey<String> REPO_URL = ConfigKeys.newStringConfigKey(
            "repo.url",
            "URL of a Helm repository");

    ConfigKey<String> HELM_TEMPLATE = ConfigKeys.newStringConfigKey(
            "helm.template",
            "The name of the Helm template to be deployed");

    ConfigKey<String> HELM_DEPLOYMENT_NAME = ConfigKeys.newStringConfigKey(
            "helm.deployment.name",
            "The name to use for the Helm deployment");

    ConfigKey<String> HELM_INSTALL_VALUES = ConfigKeys.newStringConfigKey(
            "helm.install.values",
            "A file or URL with a set of Helm config values to use at install time");

    AttributeSensor<String> STATUS = Sensors.newStringSensor(
            "helm.status",
            "The status of the Helm deployment");

    AttributeSensor<Boolean> DEPLOYMENT_READY = Sensors.newBooleanSensor(
            "kube.deployment.status",
            "The status of the Kubernetes Deployment resource");

    AttributeSensor<List<String>> DEPLOYMENTS = Sensors.newSensor(
            new TypeToken<List<String>>() { },
            "kube.deployments",
            "List of Kubernetes Deployment resources");

    AttributeSensor<List<String>> SERVICES = Sensors.newSensor(
            new TypeToken<List<String>>() { },
            "kube.services",
            "List of Kubernetes Service resources");

    @Effector(description = "Resize Helm deployment")
    Integer resize(@EffectorParam(name = "deploymentName") String name, @EffectorParam(name = "desiredSize") Integer desiredSize);
}
