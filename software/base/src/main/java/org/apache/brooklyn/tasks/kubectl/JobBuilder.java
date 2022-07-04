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
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This was needed to ensure our Kubernetes Yaml Job configurations are valid.
 */
public class JobBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(JobBuilder.class);

    String jobName;
    String imageName;

    String imagePullPolicy;

    String workingDir;

    String prefix = "brooklyn-job";

    List<String> commands = Lists.newArrayList();
    List<String> args = Lists.newArrayList();

    Map<String, Object> env = Maps.newHashMap();

    List<Map<String,String>> volumeMounts = Lists.newArrayList();

    List<Map<String, Object>> volumes = Lists.newArrayList();

    public JobBuilder withName(final String name) {
        this.jobName = name;
        return this;
    }

    public JobBuilder withImage(final String image){
        this.imageName = image;
        return this;
    }

    /**
     * If {@code imagePullPolicy} is not set for a container, Kubernetes defaults to Always.
     * @param eimagePullPolicy
     * @return
     */
    public JobBuilder withImagePullPolicy(final PullPolicy eimagePullPolicy){
        if (eimagePullPolicy != null) {
            this.imagePullPolicy = eimagePullPolicy.val();
        }
        return this;
    }

    public JobBuilder withCommands(final List<String> commandsArg){
        if (commandsArg != null) {
            this.commands.addAll(commandsArg);
        }
        return this;
    }

    public JobBuilder withArgs(final List<String> args){
        if (args != null) {
            this.args.addAll(args);
        }
        return this;
    }

    public JobBuilder withVolumeMounts(final Set<Map<String,String>> volumeMounts) {
        if (volumeMounts != null) {
            this.volumeMounts.addAll(volumeMounts);
        }
        return this;
    }

    public JobBuilder withVolumes(final Set<Map<String, Object>> volumes) {
        if (volumes != null) {
            this.volumes.addAll(volumes);
        }
        return this;
    }

    public JobBuilder withWorkingDir(String workingDir) {
        this.workingDir = workingDir;
        return this;
    }

    public JobBuilder withPrefix(final String prefixArg){
        this.prefix = prefixArg;
        return this;
    }

    public JobBuilder withEnv(final Map<String,Object> env){
        if (env != null) {
            this.env.putAll(env);
        }
        return this;
    }

    public String build(){
        JobTemplate jobTemplate = new JobTemplate(jobName);

        ContainerSpec containerSpec = jobTemplate.getSpec().getTemplate().getContainerSpec(0);

        if(Strings.isNonBlank(workingDir)) {
            containerSpec.setWorkingDir(workingDir);
        }
        containerSpec.setImage(imageName);
        containerSpec.setImagePullPolicy(imagePullPolicy);

        if (!env.isEmpty()) {
            List<Map<String,String>> envList = env.entrySet().stream().map (e ->  {
                    Map<String,String> envItem = new HashMap<>();
                    envItem.put("name", e.getKey());
                    envItem.put("value", e.getValue().toString());
                    return envItem;
                }).collect(Collectors.toList());
            containerSpec.setEnv(envList);
        }
        if (!commands.isEmpty()) {
            containerSpec.setCommand(this.commands);
        }
        if (!args.isEmpty()) {
            containerSpec.setArgs(this.args);
        }

        final Set<String> volumeNames = new HashSet<>();
        if (!volumes.isEmpty()) {
            jobTemplate.getSpec().getTemplate().getSpec().setVolumes(volumes);

            volumes.stream().map(volumeSpec -> (String)volumeSpec.get("name")).forEach(volumeNames::add);
        }

        if (!volumeMounts.isEmpty()) {
            List<VolumeMount> vms = Lists.newArrayList();
            volumeMounts.forEach(vmMap -> {
                VolumeMount vm = new VolumeMount();
                vm.setName(vmMap.get("name"));
                if(!volumeNames.contains(vm.getName())) {
                   throw new IllegalArgumentException("The Job "  + this.jobName + "is invalid: spec.template.spec.containers[0].volumeMounts[0].name: Not found:\"" + vm.getName() + "\"");
                }
                vm.setMountPath(vmMap.get("mountPath"));
                vms.add(vm);
            });
            containerSpec.setVolumeMounts(vms);
        }
        return serializeAndWriteToTempFile(jobTemplate);
    }

    private String serializeAndWriteToTempFile(JobTemplate jobTemplate) {
        DumperOptions options = new DumperOptions();
        options.setIndent(2);
        options.setPrettyFlow(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Representer representer = new Representer(){
            @Override
            protected NodeTuple representJavaBeanProperty(Object javaBean, Property property, Object propertyValue, Tag customTag) {
                // if value of property is null, ignore it.
                if (propertyValue == null) {
                    return null;
                }
                else {
                    return super.representJavaBeanProperty(javaBean, property, propertyValue, customTag);
                }
            }
        };
        representer.addClassTag(JobTemplate.class, Tag.MAP);

        try {
            File jobBodyPath = File.createTempFile(prefix, ".yaml");
            jobBodyPath.deleteOnExit();  // We should have already deleted it, but just in case

            PrintWriter sw = new PrintWriter(jobBodyPath);
            Yaml yaml = new Yaml(representer, options);
            yaml.dump(jobTemplate, sw);
            LOG.info("Job body dumped at: {}" , jobBodyPath.getAbsolutePath());
            return jobBodyPath.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp file for container", e);
        }
    }
}

/**
 * Type mapping to the value of the {@code spec} element
 */
class TemplateSpec {

    /**
     * As pods successfully complete, the Job tracks the successful completions. When a specified number of successful completions is reached, the task (ie, Job) is complete.
     * Note that even if you specify .spec.parallelism = 1 and .spec.completions = 1 and .spec.template.spec.restartPolicy = "Never", the same program may sometimes be started twice.
     */
    Integer completions = 1;
    Integer parallelism = 1;

    /**
     * To do so, set .spec.backoffLimit to specify the number of retries before considering a Job as failed. The back-off limit is set by default to 6.
     */
    Integer backoffLimit = 1;

    JobSpec template;

    public TemplateSpec() {
        template = new JobSpec();
    }

    public Integer getCompletions() {
        return completions;
    }

    public void setCompletions(Integer completions) {
        this.completions = completions;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public JobSpec getTemplate() {
        return template;
    }

    public void setTemplate(JobSpec template) {
        this.template = template;
    }

    public Integer getBackoffLimit() {
        return backoffLimit;
    }

    public void setBackoffLimit(Integer backoffLimit) {
        this.backoffLimit = backoffLimit;
    }
}

/**
 * Matches the root of the yaml file
 */
class JobTemplate {
    String kind = "Job";
    String apiVersion = "batch/v1";
    Map<String, String> metadata;
    TemplateSpec spec;

    public JobTemplate() {
    }

    public JobTemplate(String name) {
        metadata = Maps.newHashMap();
        metadata.put("name", name);
        spec = new TemplateSpec();
    }

    public String getApiVersion() {
        return apiVersion;
    }

    // Do not explicitly call this
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    // Do not explicitly call this
    public void setKind(String kind) {
        this.kind = kind;
    }

    public String getKind() {
        return kind;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public TemplateSpec getSpec() {
        return spec;
    }

    public void setSpec(TemplateSpec spec) {
        this.spec = spec;
    }
}

/**
 * Type mapping to the value of the {@code template} element
 */
class JobSpec {
    ContainerSpecs spec;

    public JobSpec() {
        this.spec = new ContainerSpecs();
    }

    public ContainerSpecs getSpec() {
        return spec;
    }

    public void setSpec(ContainerSpecs spec) {
        this.spec = spec;
    }

    public ContainerSpec getContainerSpec(int index) {
        if(this.spec.containers.size() > 0) {
            return this.spec.containers.get(index);
        }
        return null;
    }
}


/**
 * Type mapping to the value of the {@code template.spec} element
 */
class ContainerSpecs {
    List<ContainerSpec> containers;

    List<Map<String, Object>> volumes;

    Boolean automountServiceAccountToken = false;
    String restartPolicy = "Never";

    public ContainerSpecs() {
        this.containers = Lists.newArrayList();
        this.containers.add(new ContainerSpec());}

    public List<ContainerSpec> getContainers() {
        return containers;
    }

    public void setContainers(List<ContainerSpec> containers) {
        this.containers = containers;
    }

    public String getRestartPolicy() {
        return restartPolicy;
    }

    public void setRestartPolicy(String restartPolicy) {
        this.restartPolicy = restartPolicy;
    }

    public Boolean getAutomountServiceAccountToken() {
        return automountServiceAccountToken;
    }

    public void setAutomountServiceAccountToken(Boolean automountServiceAccountToken) {
        this.automountServiceAccountToken = automountServiceAccountToken;
    }

    public List<Map<String, Object>> getVolumes() {
        return volumes;
    }

    public void setVolumes(List<Map<String, Object>> volumes) {
        this.volumes = volumes;
    }
}

/**
 * Type mapping to the value of the {@code template.spec.containers} element
 */
class ContainerSpec {
    String name = "test";
    String image = "defaultImage";

    String imagePullPolicy = "IfNotPresent";

    String workingDir = null; // default is /

    List<String> command = null;
    List<String> args = null;

    List<VolumeMount>  volumeMounts = null;

    List<Map<String, String>> env = null;

    public ContainerSpec() {
    }

    public String getName() {
        return name;
    }

    // Do not explicitly call this
    public void setName(String name) {
        this.name = name;
    }

    public String getImage() {
        return image;
    }

    public String getImagePullPolicy() {
        return imagePullPolicy;
    }

    public void setImagePullPolicy(String imagePullPolicy) {
        this.imagePullPolicy = imagePullPolicy;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public List<String> getCommand() {
        return command;
    }

    public void setCommand(List<String> command) {
        this.command = command;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public List<Map<String, String>> getEnv() {
        return env;
    }

    public void setEnv(List<Map<String, String>> env) {
        this.env = env;
    }
    public void setVolumeMounts(List<VolumeMount> volumeMounts) {
        this.volumeMounts = volumeMounts;
    }

    public List<VolumeMount> getVolumeMounts() {
        return volumeMounts;
    }

    public String getWorkingDir() {
        return workingDir;
    }

    public void setWorkingDir(String workingDir) {
        this.workingDir = workingDir;
    }
}

/**
 * Type mapping to the value of the {@code template.spec.containers.volumeMounts} element
 */
class VolumeMount {
    String name;
    String mountPath;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMountPath() {
        return mountPath;
    }

    public void setMountPath(String mountPath) {
        this.mountPath = mountPath;
    }
}

