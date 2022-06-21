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

    public JobBuilder withCommands(final List<String> commandsArg){
        this.commands.addAll(commandsArg);
        return this;
    }

    public JobBuilder withArgs(final List<String> args){
        this.args.addAll(args);
        return this;
    }

    public JobBuilder withVolumeMounts(final List<Map<String,String>> volumeMounts) {
        this.volumeMounts.addAll(volumeMounts);
        return this;
    }

    public JobBuilder withVolumes(final List<Map<String, Object>> volumes) {
        this.volumes.addAll(volumes);
        return this;
    }

    public JobBuilder withPrefix(final String prefixArg){
        this.prefix = prefixArg;
        return this;
    }

    public JobBuilder withEnv(final Map<String,Object> env){
        this.env.putAll(env);
        return this;
    }

    public String build(){
        JobTemplate jobTemplate = new JobTemplate(jobName);

        ContainerSpec containerSpec = jobTemplate.getSpec().get("template").getContainerSpec(0);

        containerSpec.setImage(imageName);
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
            jobTemplate.getSpec().get("template").getSpec().setVolumes(volumes);
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

class JobTemplate {
    String kind = "Job";
    String apiVersion = "batch/v1";
    Map<String, String> metadata;
    Map<String, JobSpec> spec;

    public JobTemplate() {
    }

    public JobTemplate(String name) {
        metadata = Maps.newHashMap();
        metadata.put("name", name);
        spec = new HashMap<>();
        spec.put("template", new JobSpec());
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

    public Map<String, JobSpec> getSpec() {
        return spec;
    }

    public void setSpec(Map<String, JobSpec> spec) {
        this.spec = spec;
    }

}

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

class ContainerSpec {
    String name = "test";
    String image = "defaultImage";

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

