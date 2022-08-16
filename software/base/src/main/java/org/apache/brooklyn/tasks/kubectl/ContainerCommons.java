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
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.config.SetConfigKey;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.util.time.Duration;

import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({ "rawtypes"})
public interface ContainerCommons {
    ConfigKey<String> CONTAINER_IMAGE = ConfigKeys.newStringConfigKey("image", "Container image");
    ConfigKey<PullPolicy> CONTAINER_IMAGE_PULL_POLICY = ConfigKeys.newConfigKey(new TypeToken<PullPolicy>() {} ,
            "imagePullPolicy", "Container image pull policy. Allowed values: {IfNotPresent, Always, Never}. Default IfNotPresent.", PullPolicy.IF_NOT_PRESENT);

    ConfigKey<Boolean> KEEP_CONTAINER_FOR_DEBUGGING = ConfigKeys.newBooleanConfigKey("keepContainerForDebugging", "When set to true, the namespace" +
            " and associated resources and services are not destroyed after execution. Defaults value is 'false'.", Boolean.FALSE);

    ConfigKey<Object> BASH_SCRIPT = ConfigKeys.newConfigKey(Object.class,"bashScript", "A bash script (as string or list of strings) to run, implies command '/bin/bash' '-c' and replaces arguments");
    ConfigKey<List> COMMAND = ConfigKeys.newConfigKey(List.class,"command", "Single command and optional arguments to execute for the container (overrides image EntryPoint and Cmd)", Lists.newArrayList());
    ConfigKey<List> ARGUMENTS = ConfigKeys.newConfigKey(List.class,"args", "Additional arguments to pass to the command at the container (in addition to the command supplied here or the default in the image)", Lists.newArrayList());

    MapConfigKey<Object> SHELL_ENVIRONMENT = BrooklynConfigKeys.SHELL_ENVIRONMENT;

    ConfigKey<Duration> TIMEOUT = ConfigKeys.newConfigKey(Duration.class, "timeout", "Container execution timeout (default 5 minutes)", Duration.minutes(5));

    ConfigKey<Boolean> REQUIRE_EXIT_CODE_ZERO = ConfigKeys.newConfigKey(Boolean.class, "requireExitCodeZero", "Whether task should fail if container returns non-zero exit code (default true)", true);

    ConfigKey<String> WORKING_DIR = ConfigKeys.newStringConfigKey("workingDir", "Location where the container commands are executed");
    ConfigKey<Set<Map<String,String>>> VOLUME_MOUNTS =  new SetConfigKey.Builder<>(new TypeToken<Map<String,String>>()  {}, "volumeMounts")
            .description("Configuration to mount a volume into a container.").defaultValue(null).build();

    ConfigKey<Set<Map<String,Object>>> VOLUMES = new SetConfigKey.Builder(new TypeToken<Map<String,Object>>()  {}, "volumes")
            .description("List of directories with data that is accessible across multiple containers").defaultValue(null).build();

    String NAMESPACE_CREATE_CMD = "kubectl create namespace %s";
    String NAMESPACE_SET_CMD = "kubectl config set-context --current --namespace=%s";
    String JOBS_CREATE_CMD = "kubectl apply -f %s --namespace=%s";
    String JOBS_WAIT_COMPLETE_CMD = "kubectl wait --timeout=%ds --for=condition=complete job/%s --namespace=%s";
    String JOBS_WAIT_FAILED_CMD = "kubectl wait --timeout=%ds --for=condition=failed job/%s --namespace=%s";
    String JOBS_LOGS_CMD = "kubectl logs jobs/%s --namespace=%s";
    String JOBS_DELETE_CMD = "kubectl delete job %s --namespace=%s";
    String PODS_CMD_PREFIX = "kubectl get pods --namespace=%s --selector=job-name=%s ";
    String PODS_STATUS_STATE_CMD = PODS_CMD_PREFIX + "-ojsonpath='{.items[0].status.containerStatuses[0].state}'";
    String PODS_STATUS_PHASE_CMD = PODS_CMD_PREFIX + "-ojsonpath='{.items[0].status.phase}'";
    String PODS_NAME_CMD = PODS_CMD_PREFIX + "-ojsonpath='{.items[0].metadata.name}'";
    String PODS_EXIT_CODE_CMD = PODS_CMD_PREFIX + "-ojsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}'";
    String SCOPED_EVENTS_CMD = "kubectl --namespace %s get events --field-selector=involvedObject.name=%s";
    String SCOPED_EVENTS_FAILED_JSON_CMD = "kubectl --namespace %s get events --field-selector=reason=Failed,involvedObject.name=%s -ojsonpath='{.items}'";
    String NAMESPACE_DELETE_CMD = "kubectl delete namespace %s";

    public static enum PodPhases {
        // from https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
        Failed, Running, Succeeded, Unknown, Pending
    }
}
