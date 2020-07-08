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

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.annotation.Effector;
import org.apache.brooklyn.core.annotation.EffectorParam;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.trait.Resizable;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.w3c.dom.Attr;

import java.util.List;

/**
 * Deploy using a KubernetesLocation
 * e.g.
 * location: kubernetes-location
 * services:
 * - type: org.apache.brooklyn.container.entity.helm.HelmEntity
 *   brooklyn.config:
 *     repo.name: bitnami
 *     repo.url: https://charts.bitnami.com/bitnami
 *     helm.template: bitnami/nginx
 *     helm.deployment.name: nginx
 */

@ImplementedBy(HelmEntityImpl.class)
public interface HelmEntity extends Entity, Startable {

   public static final ConfigKey<String> REPO_NAME = ConfigKeys.newStringConfigKey(
           "repo.name",
           "Name to add repo under");

   public static final ConfigKey<String> REPO_URL = ConfigKeys.newStringConfigKey(
           "repo.url",
           "Repo url");

   public static final ConfigKey<String> HELM_TEMPLATE = ConfigKeys.newStringConfigKey(
           "helm.template",
           "Template name or url");

   public static final ConfigKey<String> HELM_DEPLOYMENT_NAME = ConfigKeys.newStringConfigKey(
           "helm.deployment.name",
           "Deployment name");

   public static final ConfigKey<String> HELM_INSTALL_VALUES = ConfigKeys.newStringConfigKey(
           "helm.install.values",
           "Helm config values to use at install time - can be file or url");

   AttributeSensor<String> STATUS = Sensors.newStringSensor("helm.status",
           "The results of a status call");

   AttributeSensor<Boolean> DEPLOYMENT_READY = Sensors.newBooleanSensor("kube.deployment.status",
           "The status of the deploymeny");

   AttributeSensor<List<String>> DEPLOYMENTS = Sensors.newSensor(new TypeToken<List<String>>() {}, "kube.deployments");

   AttributeSensor<List<String>> SERVICES = Sensors.newSensor(new TypeToken<List<String>>() {}, "kube.services");

   @Effector(description="")
   Integer resize(@EffectorParam(name="deplymentName") String name, @EffectorParam(name="desiredSize") Integer desiredSize);

}
