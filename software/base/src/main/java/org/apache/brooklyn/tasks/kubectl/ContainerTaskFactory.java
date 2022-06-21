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
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskStub;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.core.task.system.internal.SystemProcessTaskFactory;
import org.apache.brooklyn.util.text.Strings;

import java.util.List;
import java.util.Map;

import static org.apache.brooklyn.tasks.kubectl.ContainerCommons.*;

public class ContainerTaskFactory<T extends ContainerTaskFactory<T,RET>,RET>  implements TaskFactory<Task<RET>> {

    protected String summary;
    protected String tag = "";
    protected final ConfigBag config = ConfigBag.newInstance();

    @Override
    public Task<RET> newTask() {
        ConfigBag configBag = this.config;

        List<String> commandsCfg =  EntityInitializers.resolve(configBag, COMMANDS);
        List<String> argumentsCfg =  EntityInitializers.resolve(configBag, ARGUMENTS);
        String containerImage = EntityInitializers.resolve(configBag, CONTAINER_IMAGE);
        String containerNameFromCfg = EntityInitializers.resolve(configBag, CONTAINER_NAME);
        Boolean devMode = EntityInitializers.resolve(configBag, KEEP_CONTAINER_FOR_DEBUGGING);

        if(Strings.isBlank(containerImage)) {
            throw new IllegalStateException("You must specify containerImage when using " + this.getClass().getSimpleName());
        }


        final String containerName = (Strings.isBlank(containerNameFromCfg)
                ? ( (Strings.isNonBlank(this.tag) ? this.tag + "-" : "").concat(containerImage).concat("-").concat(Strings.makeRandomId(10)))
                : containerNameFromCfg).replace(" ", "-").replace("/", "-").toLowerCase();

        final String jobYamlLocation =  new JobBuilder()
                .withImage(containerImage)
                .withName(containerName)
                .withArgs(argumentsCfg)
                .withEnv(EntityInitializers.resolve(configBag, BrooklynConfigKeys.SHELL_ENVIRONMENT))
                .withCommands(Lists.newArrayList(commandsCfg))
                .build();


        Task<String> runCommandsTask = buildKubeTask(configBag, "Submit job", String.format(JOBS_CREATE_CMD,jobYamlLocation)).asTask();
        Task<String> waitTask =  buildKubeTask(configBag, "Wait For Completion", String.format(JOBS_FEED_CMD,containerName)).asTask();
        if(!devMode) {
            BrooklynTaskTags.addTagDynamically(waitTask, BrooklynTaskTags.INESSENTIAL_TASK);
        }

        TaskBuilder<RET> taskBuilder = Tasks.<RET>builder()
                .displayName(this.summary)
                .add(buildKubeTask(configBag, "Set Up and Run",
                        String.format(NAMESPACE_CREATE_CMD,containerName),
                        String.format(NAMESPACE_SET_CMD,containerName)))
                .add(runCommandsTask)
                .add(waitTask)
                .add(buildKubeTask(configBag, "Retrieve Output", String.format(JOBS_LOGS_CMD,containerName)));

        if(!devMode) {
            taskBuilder.add(buildKubeTask(configBag, "Tear Down", String.format(NAMESPACE_DELETE_CMD,containerName)));
        }
        return taskBuilder.build();
    }

    public T summary(String summary) {
        this.summary = summary;
        return self();
    }

    public T tag(String tag) {
        this.tag = tag;
        return self();
    }

    protected T self() { return (T)this; }

    public T configure(Map<?, ?> flags) {
        if (flags!=null)
            config.putAll(flags);
        return self();
    }

    public static ProcessTaskWrapper<String> buildKubeTask(final ConfigBag configBag, final String taskSummary, final String... kubeCommands) {
        Map<String, Object> env = EntityInitializers.resolve(configBag, BrooklynConfigKeys.SHELL_ENVIRONMENT);
        Map<String, String> envVars = MutableMap.of();
        if(env != null && env.size() > 0) {
            env.forEach((k,v) -> envVars.put(k, v.toString()));
        }
        return new SystemProcessTaskFactory.ConcreteSystemProcessTaskFactory<String>(kubeCommands)
                .summary(taskSummary)
                .configure(configBag.getAllConfig())
                .environmentVariables(envVars) // needed to be shown in the UI ;)
                .<String>returning(ProcessTaskStub.ScriptReturnType.STDOUT_STRING)
                .requiringExitCodeZero().newTask();
    }

    public static class ConcreteContainerTaskFactory<RET> extends ContainerTaskFactory<ConcreteContainerTaskFactory<RET>, RET> {
        public ConcreteContainerTaskFactory() {
            super();
        }
    }
}
