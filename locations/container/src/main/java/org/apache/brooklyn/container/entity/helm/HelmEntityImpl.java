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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocation;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.sensor.function.FunctionSensor;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.process.ProcessTool;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;


public class HelmEntityImpl extends AbstractEntity implements HelmEntity {

    private FunctionFeed serviceUpFeed;

    protected void connectSensors() {
        connectServiceUpIsRunning();

        addHelmFeed("status", STATUS);
        addKubernetesFeeds();
    }

    @Override
    public Integer resize(String deploymentName, Integer desiredSize) {
        scaleDeployment(desiredSize, deploymentName);
        return desiredSize;
    }

    @Override
    public void start(Collection<? extends Location> locations) {
        addLocations(locations);
        doInstall();
        connectSensors();
    }

    @Override
    public void stop() {
        disconnectSensors();
        deleteHelmDeployment();
    }

    @Override
    public void restart() {
        stop();
        start(ImmutableList.<Location>of());
    }

    public boolean isRunning() {
        String helmNameInstallName = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String namespace = getNamespace();
        ImmutableList<String> command = ImmutableList.<String>of(String.format("helm status %s --namespace %s", helmNameInstallName, namespace));
        OutputStream out = new ByteArrayOutputStream();
        OutputStream err = new ByteArrayOutputStream();
        int exectionResponse = ProcessTool.execProcesses(command, null, null, out, err, ";", false, this);
        return 0 == exectionResponse;
    }

    public void scaleDeployment(Integer scale, String deploymentName) {
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);
        KubernetesClient client = getClient(config);
        client.apps().deployments().inNamespace(getNamespace()).withName(deploymentName).scale(scale);
    }

    protected void disconnectSensors() {
        disconnectServiceUpIsRunning();
    }

    private void addKubernetesFeeds() {
        Callable<Boolean> status = getKubeDeploymentsReady();
        FunctionSensor<Integer> initializer = new FunctionSensor<>(ConfigBag.newInstance()
                .configure(FunctionSensor.SENSOR_PERIOD, Duration.millis(1000))
                .configure(FunctionSensor.SENSOR_NAME, DEPLOYMENT_READY.getName())
                .configure(FunctionSensor.SENSOR_TYPE, Boolean.class.getName())
                .configure(FunctionSensor.FUNCTION, status));
        initializer.apply(this);

        FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<String, List<String>>(DEPLOYMENTS).callable(getKubeDeploymentsCallable()))
                .period(Duration.TEN_SECONDS)
                .build(true);

        FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<String, List<String>>(SERVICES).callable(getKubeServicesCallable()))
                .period(Duration.TEN_SECONDS)
                .build(true);
    }


    private void addHelmFeed(String command, AttributeSensor<String> sensor) {
        Callable<String> status = getCallable(command);
        FunctionPollConfig<String, String> pollConfig = new FunctionPollConfig<String, String>(sensor)
                .callable(status)
                ;

        FunctionFeed.builder()
                .entity(this)
                .poll(pollConfig)
                .period(Duration.FIVE_SECONDS)
                .build(true);
    }

    private void connectServiceUpIsRunning() {
        Duration period = Duration.FIVE_SECONDS;
        serviceUpFeed = FunctionFeed.builder()
                .entity(this)
                .period(period)
                .poll(new FunctionPollConfig<Boolean, Boolean>(Attributes.SERVICE_UP)
                        .suppressDuplicates(true)
                        .onException(Functions.constant(Boolean.FALSE))
                        .callable(() -> isRunning()))
                .build(true);  // only called at start so needs to be registered
    }


    private void disconnectServiceUpIsRunning() {
        serviceUpFeed.stop();
    }

    private void doInstall() {
        String repoName = getConfig(HelmEntity.REPO_NAME);
        String repoUrl = getConfig(HelmEntity.REPO_URL);

        String helmTemplate = getConfig(HelmEntity.HELM_TEMPLATE);
        String helmDeploymentName = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String installValues = getConfig(HelmEntity.HELM_INSTALL_VALUES);

        String namespace = getNamespace();

        if(Strings.isNonBlank(repoName) && Strings.isNonBlank(repoUrl)) {

            DynamicTasks.queue("install repo", new Runnable() {
                @Override
                public void run() {
                    ImmutableList<String> setupRepoCommand =
                            ImmutableList.<String>of(buildAddRepoCommand(repoName, repoUrl));
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    ByteArrayOutputStream err = new ByteArrayOutputStream();

                    ProcessTool.execProcesses(setupRepoCommand, null, null, out, err, ";", false, this);

                    Tasks.addTagDynamically(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDOUT, out));
                    Tasks.addTagDynamically(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDERR, err));
                }});
        }

        DynamicTasks.queue("install", new Runnable() {
            @Override
            public void run() {
                ImmutableList<String> installHelmTemplateCommand =
                        ImmutableList.<String>of(buildInstallCommand(helmDeploymentName, helmTemplate, installValues, namespace));
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ByteArrayOutputStream err = new ByteArrayOutputStream();

                ProcessTool.execProcesses(installHelmTemplateCommand, null, null, out, err, ";", false, this);

                Tasks.addTagDynamically(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDOUT, out));
                Tasks.addTagDynamically(BrooklynTaskTags.tagForStreamSoft(BrooklynTaskTags.STREAM_STDERR, err));
            }});
    }


    private String getNamespace() {
        return getLocation().getConfig(KubernetesLocation.NAMESPACE);
    }


    private void deleteHelmDeployment() {
        DynamicTasks.queue("stop", new Runnable() {
            @Override
            public void run() {
                String helmNameInstallName = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
                String namespace = getNamespace();
                ImmutableList<String> command = ImmutableList.<String>of(String.format("helm delete %s --namespace %s", helmNameInstallName, namespace));
                OutputStream out = new ByteArrayOutputStream();
                OutputStream err = new ByteArrayOutputStream();
                ProcessTool.execProcesses(command, null, null, out, err, ";", false, this);
            }
        });
    }


    private Callable<List<String>> getKubeServicesCallable() {
        String helmNameInstallName = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                KubernetesClient client = getClient(config);
                List<Service> services = client.services().inNamespace(getNamespace()).list().getItems();
                return services.stream()
                        .filter(service -> service.getMetadata().getAnnotations().get("meta.helm.sh/release-name").equals(helmNameInstallName))
                        .map(service -> service.getMetadata().getName())
                        .collect(Collectors.toList());

            }
        };
    }

    private Callable<List<String>> getKubeDeploymentsCallable() {
        String helmNameInstallName = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                KubernetesClient client = getClient(config);
                List<Deployment> deployments = getDeployments(client, helmNameInstallName);
                for (Deployment deployment : deployments) {
                    String sensorName = "helm.deployment." + deployment.getMetadata().getName() + ".replicas";
                    sensors().set(Sensors.newIntegerSensor(sensorName), deployment.getStatus().getReplicas());
                    sensors().set(Sensors.newIntegerSensor(sensorName+".available"), deployment.getStatus().getAvailableReplicas());
                }

                return deployments.stream().map(deployment -> deployment.getMetadata().getName()).collect(Collectors.toList());
            }
        };
    }

    private Callable<String> getCallable(String command) {
        String helmNameInstallName = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String namespace = getNamespace();
        ImmutableList<String> installHelmTemplateCommand =
                ImmutableList.<String>of(String.format("helm %s %s --namespace %s", command, helmNameInstallName, namespace));

        return new Callable<String>() {
            @Override
            public String call() throws Exception {
                OutputStream out = new ByteArrayOutputStream();
                OutputStream err = new ByteArrayOutputStream();
                ProcessTool.execProcesses(installHelmTemplateCommand, null, null, out, err,";", false, this);
                return out.toString();
            }
        };
    }

    private Callable<Boolean> getKubeDeploymentsReady() {
        String helmNameInstallName = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                KubernetesClient client = getClient(config);
                List<Deployment> deployments = getDeployments(client, helmNameInstallName);
                Integer availableReplicas = countAvailableReplicas(deployments);
                Integer replicas = countReplicas(deployments);
                return availableReplicas.equals(replicas);
            }
        };
    }

    private List<Deployment> getDeployments(KubernetesClient client, String helmNameInstallName) {
        List<Deployment> items = client.apps().deployments().inNamespace(getNamespace()).list().getItems();
        items.stream().filter(deployment -> deployment.getMetadata().getAnnotations().get("meta.helm.sh/release-name").equals(helmNameInstallName)).collect(Collectors.toList());
        return items;
    }

    private Integer countReplicas(List<Deployment> deployments) {
        return deployments.stream().map(deployment -> deployment.getStatus().getReplicas()).mapToInt(Integer::intValue).sum();
    }

    private Integer countAvailableReplicas(List<Deployment> deployments) {
        return deployments.stream().map(deployment -> deployment.getStatus().getAvailableReplicas()).mapToInt(Integer::intValue).sum();
    }

    private KubernetesLocation getLocation() {
        return (KubernetesLocation) Entities.getAllInheritedLocations(this).stream().filter(KubernetesLocation.class::isInstance).findFirst().get();
    }

    private Callable<Integer> getKubeReplicasCallable(String deploymentName) {
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                KubernetesClient client = getClient(config);
                Deployment deployment = client.apps().deployments().inNamespace(getNamespace()).withName(deploymentName).get();
                return deployment.getStatus().getReplicas();
            }
        };
    }


    KubernetesClient getClient(String configFile) {
        Path configPath = Paths.get(configFile);
        try {
            Config clientConfig = Config.fromKubeconfig(new String(Files.readAllBytes(configPath)));
            ConfigBuilder configBuilder = new ConfigBuilder(clientConfig);
            return new DefaultKubernetesClient(configBuilder.build());
        }catch (IOException ioe) {
            Exceptions.propagate(ioe);
            return null;
        }
    }

    private String buildAddRepoCommand(String repoName, String repoUrl) {
        return String.format("helm repo add %s %s", repoName, repoUrl);
    }

    private String buildInstallCommand(String helmDeploymentName, String helmTemplate, String installValues, String namespace) {
        String installCommand = String.format("helm install %s %s", helmDeploymentName, helmTemplate);

        if(Strings.isNonBlank(installValues)) {
            installCommand += String.format(" --values %s", installValues);
        }
        if(Strings.isNonBlank(namespace)) {
            installCommand += String.format(" --namespace %s", namespace);
        }
        return installCommand;
    }
}
