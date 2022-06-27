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
package org.apache.brooklyn.tasks.kubectl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({ "rawtypes"})
public interface ContainerCommons {
    ConfigKey<String> CONTAINER_IMAGE = ConfigKeys.newStringConfigKey("image", "Container image");
    ConfigKey<String> CONTAINER_NAME = ConfigKeys.newStringConfigKey("containerName", "Container name");
    ConfigKey<Boolean> KEEP_CONTAINER_FOR_DEBUGGING = ConfigKeys.newBooleanConfigKey("keepContainerForDebugging", "When set to true, the namespace" +
            " and associated resources and services are not destroyed after execution. Defaults value is 'false'.", Boolean.FALSE);

    ConfigKey<List> COMMANDS = ConfigKeys.newConfigKey(List.class,"commands", "Commands to execute for container", Lists.newArrayList());
    ConfigKey<List> ARGUMENTS = ConfigKeys.newConfigKey(List.class,"args", "Arguments to execute for container", Lists.newArrayList());

    ConfigKey<String> WORKING_DIR = ConfigKeys.newStringConfigKey("workingDir", "Location where the container commands are executed");
    ConfigKey<List<Map<String,String>>> VOLUME_MOUNTS = ConfigKeys.newConfigKey("volumeMounts", "Configuration to  mount a volume  into a container.", Lists.newArrayList());
    ConfigKey<List<Map<String, Object>>> VOLUMES = ConfigKeys.newConfigKey("volumes", "List of directories with data that is accessible across multiple containers", Lists.newArrayList());

    String NAMESPACE_CREATE_CMD = "kubectl create namespace brooklyn-%s"; // namespace name
    String NAMESPACE_SET_CMD = "kubectl config set-context --current --namespace=brooklyn-%s"; // namespace name
    String JOBS_CREATE_CMD = "kubectl apply -f %s"; // deployment.yaml absolute path
    String JOBS_FEED_CMD = "kubectl wait --for=condition=complete job/%s"; // containerName
    String JOBS_LOGS_CMD = "kubectl logs jobs/%s"; // containerName
    String NAMESPACE_DELETE_CMD = "kubectl delete namespace brooklyn-%s"; // namespace name

}
