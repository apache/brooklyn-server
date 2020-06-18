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

import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.container.location.kubernetes.KubernetesLocation;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.entity.java.JavaSoftwareProcessSshDriver;
import org.apache.brooklyn.entity.software.base.AbstractSoftwareProcessDriver;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcessDriver;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.process.ProcessTool;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Callable;

public class HelmSshDriver extends AbstractSoftwareProcessDriver implements HelmDriver{

    public HelmSshDriver(EntityLocal entity, Location location) {
        super(entity, location);
    }

    @Override
    public boolean isRunning() {
        String helm_name_install_name = getEntity().getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
        ImmutableList<String> command = ImmutableList.<String>of(String.format("helm status %s", helm_name_install_name));
        OutputStream out = new ByteArrayOutputStream();
        OutputStream err = new ByteArrayOutputStream();
        return 0 == ProcessTool.execProcesses(command, null, null, out, err,";",false, this);
    }

    @Override
    public void stop() {
        DynamicTasks.queue("stop", new Runnable() {
                    @Override
                    public void run() {
                        String helm_name_install_name = getEntity().getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
                        ImmutableList<String> command = ImmutableList.<String>of(String.format("helm delete %s", helm_name_install_name));
                        OutputStream out = new ByteArrayOutputStream();
                        OutputStream err = new ByteArrayOutputStream();
                        ProcessTool.execProcesses(command, null, null, out, err, ";", false, this);
                    }
                });
        //TODO Do something with output
    }

    @Override
    public void install() {
        String repo_name = getEntity().getConfig(HelmEntity.REPO_NAME);
        String repo_url = getEntity().getConfig(HelmEntity.REPO_URL);

        String helm_template = getEntity().getConfig(HelmEntity.HELM_TEMPLATE);
        String helm_name_install_name = getEntity().getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);

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
    public Callable<String> getCallable(String command) {
        String helm_name_install_name = getEntity().getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
        ImmutableList<String> installHelmTemplateCommand =
                ImmutableList.<String>of(String.format("helm %s %s", command, helm_name_install_name));

        return new Callable() {
            @Override
            public Object call() throws Exception {
                OutputStream out = new ByteArrayOutputStream();
                OutputStream err = new ByteArrayOutputStream();
                ProcessTool.execProcesses(installHelmTemplateCommand, null, null, out, err,";", false, this);
                return out.toString();
            }
        };
    }

    @Override
    public Callable getKubeCallable() {
        String helm_name_install_name = getEntity().getConfig(HelmEntity.HELM_TEMPLATE_INSTALL_NAME);
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

    @Override
    public void runPreInstallCommand() {

    }

    @Override
    public void setup() {

    }

    @Override
    public void runPostInstallCommand() {

    }

    @Override
    public void runPreCustomizeCommand() {

    }

    @Override
    public void customize() {

    }

    @Override
    public void runPostCustomizeCommand() {

    }

    @Override
    public void runPreLaunchCommand() {

    }

    @Override
    public void launch() {

    }

    @Override
    public void runPostLaunchCommand() {

    }

    @Override
    protected void createDirectory(String directoryName, String summaryForLogging) {

    }

    @Override
    public int copyResource(Map<Object, Object> sshFlags, String sourceUrl, String target, boolean createParentDir) {
        return 0;
    }

    @Override
    public int copyResource(Map<Object, Object> sshFlags, InputStream source, String target, boolean createParentDir) {
        return 0;
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
