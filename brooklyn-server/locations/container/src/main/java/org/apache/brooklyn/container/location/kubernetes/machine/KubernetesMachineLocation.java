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
package org.apache.brooklyn.container.location.kubernetes.machine;

import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

public interface KubernetesMachineLocation extends MachineLocation {

    ConfigKey<String> KUBERNETES_NAMESPACE = ConfigKeys.builder(String.class, "kubernetes.namespace")
            .description("Namespace for the KubernetesMachineLocation")
            .build();

    ConfigKey<String> KUBERNETES_RESOURCE_NAME = ConfigKeys.builder(String.class, "kubernetes.name")
            .description("Name of the resource represented by the KubernetesMachineLocation")
            .build();

    ConfigKey<String> KUBERNETES_RESOURCE_TYPE = ConfigKeys.builder(String.class, "kubernetes.type")
            .description("Type of the resource represented by the KubernetesMachineLocation")
            .build();

    public String getResourceName();

    public String getResourceType();

    public String getNamespace();

}
