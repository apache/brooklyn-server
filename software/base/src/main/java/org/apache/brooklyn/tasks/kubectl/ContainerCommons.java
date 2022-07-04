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
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.SetConfigKey;

import java.util.*;

@SuppressWarnings({ "rawtypes"})
public interface ContainerCommons {
    ConfigKey<String> CONTAINER_IMAGE = ConfigKeys.newStringConfigKey("image", "Container image");
    ConfigKey<PullPolicy> CONTAINER_IMAGE_PULL_POLICY = ConfigKeys.newConfigKey(new TypeToken<PullPolicy>() {} ,
            "imagePullPolicy", "Container image pull policy. Allowed values: {IfNotPresent, Always, Never}. ", PullPolicy.ALWAYS);

    ConfigKey<String> CONTAINER_NAME = ConfigKeys.newStringConfigKey("containerName", "Container name");
    ConfigKey<Boolean> KEEP_CONTAINER_FOR_DEBUGGING = ConfigKeys.newBooleanConfigKey("keepContainerForDebugging", "When set to true, the namespace" +
            " and associated resources and services are not destroyed after execution. Defaults value is 'false'.", Boolean.FALSE);

    ConfigKey<List> COMMANDS = ConfigKeys.newConfigKey(List.class,"commands", "Commands to execute for container", Lists.newArrayList());
    ConfigKey<List> ARGUMENTS = ConfigKeys.newConfigKey(List.class,"args", "Arguments to execute for container", Lists.newArrayList());

    ConfigKey<String> WORKING_DIR = ConfigKeys.newStringConfigKey("workingDir", "Location where the container commands are executed");
    BasicConfigKey<Map<String,String>> VOLUME_MOUNTS = SetConfigKey.builder(new TypeToken<Map<String,String>>()  {}, "volumeMounts")
            .description("Configuration to mount a volume into a container.").defaultValue(null).build();

    BasicConfigKey<Map<String,Object>> VOLUMES = SetConfigKey.builder(new TypeToken<Map<String,Object>>()  {}, "volumes")
            .description("List of directories with data that is accessible across multiple containers").defaultValue(null).build();

    String NAMESPACE_CREATE_CMD = "kubectl create namespace brooklyn-%s"; // namespace name
    String NAMESPACE_SET_CMD = "kubectl config set-context --current --namespace=brooklyn-%s"; // namespace name
    String JOBS_CREATE_CMD = "kubectl apply -f %s"; // deployment.yaml absolute path
    String JOBS_FEED_CMD = "kubectl wait --for=condition=complete job/%s"; // containerName
    String JOBS_LOGS_CMD = "kubectl logs jobs/%s"; // containerName
    String NAMESPACE_DELETE_CMD = "kubectl delete namespace brooklyn-%s"; // namespace name

}
