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
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocation;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcessImpl;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.core.internal.ssh.process.ProcessTool;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.Callable;

public class HelmEntityImpl extends AbstractEntity implements HelmEntity {

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
        FunctionFeed.builder()
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

        Callable availableReplicas = getKubeReplicasCallable();
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
        // TODO
    }

    @Override
    public Integer resize(Integer desiredSize) {
        scaleDeployment(desiredSize);
        return desiredSize;
    }

    @Override
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
        String helm_name_install_name = getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);

        if(Strings.isNonBlank(repo_name) && Strings.isNonBlank(repo_url)) {

            DynamicTasks.queue("install repo", new Runnable() {
                @Override
                public void run() {
                    ImmutableList<String> installHelmTemplateCommand =
                            ImmutableList.<String>of(String.format("helm repo add %s %s", repo_name, repo_url));
                    OutputStream out = new ByteArrayOutputStream();
                    OutputStream err = new ByteArrayOutputStream();
                    ProcessTool.execProcesses(installHelmTemplateCommand, null, null, out, err, ";", false, this);
                }});
        }

        DynamicTasks.queue("install", new Runnable() {
            @Override
            public void run() {
                ImmutableList<String> installHelmTemplateCommand =
                        ImmutableList.<String>of(String.format("helm install %s %s", helm_name_install_name, helm_template));
                OutputStream out = new ByteArrayOutputStream();
                OutputStream err = new ByteArrayOutputStream();
                ProcessTool.execProcesses(installHelmTemplateCommand, null, null, out, err, ";", false, this);
            }});
        //TODO Do something with output
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
                String helm_name_install_name = getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
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
        String helm_name_install_name = getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
        ImmutableList<String> command = ImmutableList.<String>of(String.format("helm status %s", helm_name_install_name));
        OutputStream out = new ByteArrayOutputStream();
        OutputStream err = new ByteArrayOutputStream();
        return 0 == ProcessTool.execProcesses(command, null, null, out, err,";",false, this);
    }


    public Callable<String> getCallable(String command) {
        String helm_name_install_name = getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
        ImmutableList<String> installHelmTemplateCommand =
                ImmutableList.<String>of(String.format("helm %s %s", command, helm_name_install_name));

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
        String helm_name_install_name = getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable() {
            @Override
            public Object call() throws Exception {
                KubernetesClient client = getClient(config);
                Deployment deploy = client.apps().deployments().inNamespace("default").withName(helm_name_install_name).get();
                Integer availableReplicas = deploy.getStatus().getAvailableReplicas();
                Integer replicas = deploy.getStatus().getReplicas();
                return availableReplicas.equals(replicas);
            } ;
        };
    }

    private KubernetesLocation getLocation() {
        return (KubernetesLocation) getLocations().stream().filter(KubernetesLocation.class::isInstance).findFirst().get();
    }

    public Callable getKubeReplicasCallable() {
        String helm_name_install_name = getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable() {
            @Override
            public Object call() throws Exception {
                KubernetesClient client = getClient(config);
                Deployment deploy = client.apps().deployments().inNamespace("default").withName(helm_name_install_name).get();
                return deploy.getStatus().getReplicas();
            } ;
        };
    }

    public Callable getKubeReplicasAvailableCallable() {
        String helm_name_install_name = getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);

        return new Callable() {
            @Override
            public Object call() throws Exception {
                KubernetesClient client = getClient(config);
                Deployment deploy = client.apps().deployments().inNamespace("default").withName(helm_name_install_name).get();
                return deploy.getStatus().getAvailableReplicas();
            } ;
        };
    }

    public void scaleDeployment(Integer scale) {
        String helm_name_install_name = getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);

        String config = getLocation().getConfig(KubernetesLocation.KUBECONFIG);
        KubernetesClient client = getClient(config);
        client.apps().deployments().inNamespace("default").withName(helm_name_install_name).scale(scale);
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
}
