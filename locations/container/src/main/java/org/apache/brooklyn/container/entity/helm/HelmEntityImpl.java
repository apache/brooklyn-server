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

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.dsl.*;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocation;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.core.internal.ssh.process.ProcessTool;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

public class HelmEntityImpl extends AbstractEntity implements HelmEntity {

    private static final Logger LOG = LoggerFactory.getLogger(HelmEntityImpl.class);

    private FunctionFeed serviceUpFeed;

    @Override
    public void init() {
        super.init();
    }

    protected void connectSensors() {
        connectServiceUpIsRunning();

        //TODO do these reconnect after amp restart?
        addHelmFeed("status", STATUS);
        addKubernetesFeeds();
    }

    private void connectServiceUpIsRunning() {
        Duration period = Duration.FIVE_SECONDS;
        serviceUpFeed = FunctionFeed.builder()
                .entity(this)
                .period(period)
                .poll(new FunctionPollConfig<Boolean, Boolean>(Attributes.SERVICE_UP)
                        .suppressDuplicates(true)
                        .onException(Functions.constant(Boolean.FALSE))
                        .callable(new Callable<Boolean>() {
                            @Override
                            public Boolean call() {
                                return isRunning();
                            }
                        }))
                .build();
    }

    private void addKubernetesFeeds() {
        Callable status = getKubeDeploymentsCallable();
//        FunctionSensor<Integer> initializer = new FunctionSensor<Integer>(ConfigBag.newInstance()
//                .configure(FunctionSensor.SENSOR_PERIOD, Duration.millis(1000))
//                .configure(FunctionSensor.SENSOR_NAME, DEPLOYMENT_READY.getName())
//                .configure(FunctionSensor.SENSOR_TYPE, Boolean.class.getName())
//                .configure(FunctionSensor.FUNCTION, status));
//        initializer.apply(this);

        addFeed(FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<String, Boolean>(DEPLOYMENT_READY)
                        .callable(status))
                .period(Duration.FIVE_SECONDS)
                .build());

        Callable replicas = getKubeReplicasCallable();
        addFeed(FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<String, Integer>(REPLICAS)
                        .callable(replicas))
                .period(Duration.FIVE_SECONDS)
                .build());

        Callable availableReplicas = getKubeReplicasAvailableCallable();
        addFeed(FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<String, Integer>(AVAILABLE_REPLICAS)
                        .callable(availableReplicas))
                .period(Duration.FIVE_SECONDS)
                .build());
    }


    private void addHelmFeed(String command, AttributeSensor<String> sensor) {
        Callable status = getCallable(command);
        FunctionPollConfig pollConfig = new FunctionPollConfig<String, String>(sensor)
                .callable(status)
                ;

        addFeed(FunctionFeed.builder()
                .entity(this)
                .poll(pollConfig)
                .period(Duration.FIVE_SECONDS)
                .build());
    }


    protected void disconnectSensors() {
        disconnectServiceUpIsRunning();
    }

    private void disconnectServiceUpIsRunning() {
        serviceUpFeed.stop();
    }

    @Override
    public Integer resize(String deploymentName, Integer desiredSize) {
        scaleDeployment(desiredSize, deploymentName);
        return desiredSize;
    }

    public Integer getCurrentSize() {
        return sensors().get(REPLICAS);
    }

    @Override
    public void start(Collection<? extends Location> locations) {
        addLocations(locations);
        doInstall();
        connectSensors();
    }

    private void doInstall() {
        String repo_name = getConfig(HelmEntity.REPO_NAME);
        String repo_url = getConfig(HelmEntity.REPO_URL);

        String helm_template = getConfig(HelmEntity.HELM_TEMPLATE);
        String helm_deployment_name = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String install_values = getConfig(HelmEntity.HELM_INSTALL_VALUES);

        String namespace = getNamespace();

        if(Strings.isNonBlank(repo_name) && Strings.isNonBlank(repo_url)) {

            DynamicTasks.queue("install repo", new Runnable() {
                @Override
                public void run() {
                    ImmutableList<String> installHelmTemplateCommand =
                            ImmutableList.<String>of(buildAddRepoCommand(repo_name, repo_url));
                    OutputStream out = new ByteArrayOutputStream();
                    OutputStream err = new ByteArrayOutputStream();
                    ProcessTool.execProcesses(installHelmTemplateCommand, null, null, out, err, ";", false, this);
                }});
        }

        DynamicTasks.queue("install", new Runnable() {
            @Override
            public void run() {
                ImmutableList<String> installHelmTemplateCommand =
                        ImmutableList.<String>of(buildInstallCommand(helm_deployment_name, helm_template, install_values, namespace));
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


    @Override
    public void stop() {
        disconnectSensors();
        deleteHelmDeployment();
    }

    private void deleteHelmDeployment() {
        DynamicTasks.queue("stop", new Runnable() {
            @Override
            public void run() {
                String helm_name_install_name = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
                ImmutableList<String> command = ImmutableList.<String>of(String.format("helm delete %s", helm_name_install_name));
                OutputStream out = new ByteArrayOutputStream();
                OutputStream err = new ByteArrayOutputStream();
                ProcessTool.execProcesses(command, null, null, out, err, ";", false, this);
            }
        });
    }

    @Override
    public void restart() {
        stop();
        start(ImmutableList.<Location>of());
    }

    public boolean isRunning() {
        String helm_name_install_name = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String namespace = getNamespace();
        ImmutableList<String> command = ImmutableList.<String>of(String.format("helm status %s --namespace %s", helm_name_install_name, namespace));
        OutputStream out = new ByteArrayOutputStream();
        OutputStream err = new ByteArrayOutputStream();
        int exectionResponse = ProcessTool.execProcesses(command, null, null, out, err, ";", false, this);
        return 0 == exectionResponse;
    }


    public Callable<String> getCallable(String command) {
        String helm_name_install_name = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String namespace = getNamespace();
        ImmutableList<String> installHelmTemplateCommand =
                ImmutableList.<String>of(String.format("helm %s %s --namespace %s", command, helm_name_install_name, namespace));

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

    public Callable getKubeDeploymentsCallable() {
        String helm_name_install_name = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable() {
            @Override
            public Boolean call() throws Exception {
                KubernetesClient client = getClient(config);
                List<Deployment> deployments = getDeployments(client, helm_name_install_name);
                Integer availableReplicas = countAvailableReplicas(deployments);
                Integer replicas = countReplicas(deployments);
                return availableReplicas.equals(replicas);
            } ;
        };
    }

    private List<Deployment> getDeployments(KubernetesClient client, String helm_name_install_name) {
        AppsAPIGroupDSL apps = client.apps();
        MixedOperation<Deployment, DeploymentList, DoneableDeployment, RollableScalableResource<Deployment, DoneableDeployment>> deployments1 = apps.deployments();
        String namespace = getNamespace();
        NonNamespaceOperation<Deployment, DeploymentList, DoneableDeployment, RollableScalableResource<Deployment, DoneableDeployment>> deploymentDeploymentListDoneableDeploymentRollableScalableResourceNonNamespaceOperation = deployments1.inNamespace(namespace);
        FilterWatchListDeletable<Deployment, DeploymentList, Boolean, Watch, Watcher<Deployment>> release1 = deploymentDeploymentListDoneableDeploymentRollableScalableResourceNonNamespaceOperation.withLabel("app.kubernetes.io/instance", helm_name_install_name);
        FilterWatchListDeletable<Deployment, DeploymentList, Boolean, Watch, Watcher<Deployment>> release = release1;
        DeploymentList list = release.list();
        List<Deployment> deployments = list.getItems();
        return client.apps().deployments().inNamespace(getNamespace()).withLabel("app.kubernetes.io/instance", helm_name_install_name).list().getItems();
    }

    private Integer countReplicas(List<Deployment> deployments) {
        return deployments.stream().map(deployment -> deployment.getStatus().getReplicas()).mapToInt(Integer::intValue).sum();
    }

    private Integer countAvailableReplicas(List<Deployment> deployments) {
        return deployments.stream().map(deployment -> deployment.getStatus().getAvailableReplicas()).mapToInt(Integer::intValue).sum();
    }

    private KubernetesLocation getLocation() {
        return (KubernetesLocation) getLocations().stream().filter(KubernetesLocation.class::isInstance).findFirst().get();
    }

    //TODO get rid of this
    public Callable getKubeReplicasCallable() {
        String helm_name_install_name = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable() {
            @Override
            public Integer call() throws Exception {
                KubernetesClient client = getClient(config);
                return countReplicas(getDeployments(client, helm_name_install_name));
            } ;
        };
    }

    //TODO get rid of this
    public Callable getKubeReplicasAvailableCallable() {
        String helm_name_install_name = getConfig(HelmEntity.HELM_DEPLOYMENT_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable() {
            @Override
            public Integer call() throws Exception {
                KubernetesClient client = getClient(config);
                return countAvailableReplicas(getDeployments(client, helm_name_install_name));
            } ;
        };
    }

    public void scaleDeployment(Integer scale, String deploymentName) {
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);
        KubernetesClient client = getClient(config);
        client.apps().deployments().inNamespace(getNamespace()).withName(deploymentName).scale(scale);
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

    private String buildAddRepoCommand(String repo_name, String repo_url) {
        String install_command = String.format("helm repo add %s %s", repo_name, repo_url);
        return install_command;
    }

    private String buildInstallCommand(String helmDeploymentName, String helmTemplate, String installValues, String namespace) {
        String install_command = String.format("helm install %s %s", helmDeploymentName, helmTemplate);

        if(Strings.isNonBlank(installValues)) {
            install_command += String.format(" --values %s", installValues);
        }
        if(Strings.isNonBlank(namespace)) {
            install_command += String.format(" --namespace %s", namespace);
        }
        return install_command;
    }
}
