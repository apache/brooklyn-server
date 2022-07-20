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
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.ha.BrooklynBomOsgiArchiveInstaller;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.json.ShellEnvironmentSerializer;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.system.ProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskStub;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.core.task.system.SimpleProcessTaskFactory;
import org.apache.brooklyn.util.core.task.system.internal.SystemProcessTaskFactory;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.StringShortener;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.brooklyn.tasks.kubectl.ContainerCommons.*;

public class ContainerTaskFactory<T extends ContainerTaskFactory<T,RET>,RET> implements SimpleProcessTaskFactory<T, ContainerTaskResult,RET, Task<RET>> {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerTaskFactory.class);

    protected String summary;
    protected String jobIdentifier = "";
    protected final ConfigBag config = ConfigBag.newInstance();
    private String namespace;
    private Boolean createNamespace;
    private Boolean deleteNamespace;
    Function<ContainerTaskResult,RET> returnConversion;

    @Override
    public Task<RET> newTask() {
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        TaskBuilder<RET> taskBuilder = Tasks.<RET>builder().dynamic(true)
                .displayName(this.summary)
                .tag(BrooklynTaskTags.tagForStream(BrooklynTaskTags.STREAM_STDOUT, stdout))
                .body(()-> {
                    List<String> commandsCfg =  EntityInitializers.resolve(config, COMMAND);
                    List<String> argumentsCfg =  EntityInitializers.resolve(config, ARGUMENTS);

                    Object bashScript = EntityInitializers.resolve(config, BASH_SCRIPT);
                    if (bashScript!=null) {
                        if (!commandsCfg.isEmpty()) LOG.warn("Ignoring 'command' "+commandsCfg+" because bashScript is set");
                        if (!argumentsCfg.isEmpty()) LOG.warn("Ignoring 'args' "+argumentsCfg+" because bashScript is set");

                        commandsCfg = MutableList.of("/bin/bash", "-c");
                        List<Object> argumentsCfgO = bashScript instanceof Iterable ? MutableList.copyOf((Iterable) bashScript) : MutableList.of(bashScript);
                        argumentsCfg = MutableList.of(argumentsCfgO.stream().map(x -> ""+x).collect(Collectors.joining("\n")));
                    }

                    String containerImage = EntityInitializers.resolve(config, CONTAINER_IMAGE);
                    PullPolicy containerImagePullPolicy = EntityInitializers.resolve(config, CONTAINER_IMAGE_PULL_POLICY);
                    Boolean devMode = EntityInitializers.resolve(config, KEEP_CONTAINER_FOR_DEBUGGING);

                    String workingDir = EntityInitializers.resolve(config, WORKING_DIR);
                    Set<Map<String,String>> volumeMounts = (Set<Map<String,String>>) EntityInitializers.resolve(config, VOLUME_MOUNTS);
                    Set<Map<String, Object>> volumes = (Set<Map<String, Object>>) EntityInitializers.resolve(config, VOLUMES);

                    if(Strings.isBlank(containerImage)) {
                        throw new IllegalStateException("You must specify containerImage when using " + this.getClass().getSimpleName());
                    }

                    Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
                    if (entity == null) {
                        throw new IllegalStateException("Task must run in context of entity to background jobs");
                    }

                    final String cleanImageName = containerImage.contains(":") ? containerImage.substring(0, containerImage.indexOf(":")) : containerImage;

                    StringShortener ss = new StringShortener().separator("-");
                    if (Strings.isNonBlank(this.jobIdentifier)) ss.append("job", this.jobIdentifier).canTruncate("job", 10);
                    ss.append("image", cleanImageName).canTruncate("image", 10);
                    ss.append("uid", Strings.makeRandomId(9)+Identifiers.makeRandomPassword(1, Identifiers.LOWER_CASE_ALPHA));
                    final String containerName = ss.getStringOfMaxLength(50)
                            .replaceAll("[^A-Za-z0-9-]+", "-") // remove all symbols
                            .toLowerCase();
                    if (namespace==null) {
                        namespace = "brooklyn-" + containerName;
                    }

                    LOG.debug("Submitting container job in namespace "+namespace);

                    Map<String, String> env = new ShellEnvironmentSerializer(((EntityInternal)entity).getManagementContext()).serialize(EntityInitializers.resolve(config, BrooklynConfigKeys.SHELL_ENVIRONMENT));
                    final BrooklynBomOsgiArchiveInstaller.FileWithTempInfo<File> jobYaml =  new KubeJobFileCreator()
                            .withImage(containerImage)
                            .withImagePullPolicy(containerImagePullPolicy)
                            .withName(containerName)
                            .withCommand(Lists.newArrayList(commandsCfg))
                            .withArgs(argumentsCfg)
                            .withEnv(env)
                            .withVolumeMounts(volumeMounts)
                            .withVolumes(volumes)
                            .withWorkingDir(workingDir)
                            .createFile();
                    Tasks.addTagDynamically(BrooklynTaskTags.tagForEnvStream(BrooklynTaskTags.STREAM_ENV, env));

                    try {

                        Duration timeout = EntityInitializers.resolve(config, TIMEOUT);

                        ContainerTaskResult result = new ContainerTaskResult();
                        result.interestingJobs = MutableList.of();

                        ProcessTaskWrapper<ProcessTaskWrapper<?>> createNsJob = null;
                        if (!Boolean.FALSE.equals(createNamespace)) {
                            ProcessTaskFactory<ProcessTaskWrapper<?>> createNsJobF = newSimpleTaskFactory(
                                    String.format(NAMESPACE_CREATE_CMD, namespace)
                                    //, String.format(NAMESPACE_SET_CMD, namespace)
                            ).summary("Set up namespace").returning(x -> x);
                            if (createNamespace==null) {
                                createNsJobF.allowingNonZeroExitCode();
                            }
                            createNsJob = DynamicTasks.queue(createNsJobF.newTask());
                        }

                        // only delete if told to always, unless we successfully create it
                        boolean deleteNamespaceHere = Boolean.TRUE.equals(deleteNamespace);
                        try {
                            if (createNsJob!=null) {
                                ProcessTaskWrapper<?> nsDetails = createNsJob.get();
                                if (nsDetails.getExitCode()==0) {
                                    LOG.debug("Namespace created");
                                    if (deleteNamespace==null) deleteNamespaceHere = true;
                                } else if (nsDetails.getExitCode()==1 && nsDetails.getStderr().contains("AlreadyExists")) {
                                    if (Boolean.TRUE.equals(createNamespace)) {
                                        LOG.warn("Namespace "+namespace+" already exists; failing");
                                        throw new IllegalStateException("Namespace "+namespace+" exists when creating a job that expects to create this namespace");
                                    } else {
                                        LOG.debug("Namespace exists already; reusing it");
                                    }
                                } else {
                                    LOG.warn("Unexpected namespace creation problem: "+nsDetails.getStderr()+ "(code "+nsDetails.getExitCode()+")");
                                    if (deleteNamespace==null) deleteNamespaceHere = true;
                                    throw new IllegalStateException("Unexpected namespace creation problem ("+namespace+"); see log for more details");
                                }
                            }

                            result.interestingJobs.add(DynamicTasks.queue(
                                    newSimpleTaskFactory(String.format(JOBS_CREATE_CMD, jobYaml.getFile().getAbsolutePath(), namespace)).summary("Submit job").newTask()));

                            final CountdownTimer timer = CountdownTimer.newInstanceStarted(timeout);

                            // use `wait --for` api, but in a 5s loop in case there are other issues
                            boolean succeeded = DynamicTasks.queue(Tasks.<Boolean>builder().dynamic(true).displayName("Wait for success or failure").body(() -> {
                                while (true) {
                                    LOG.debug("Container job submitted, now waiting on success or failure");

                                    long secondsLeft = Math.min(Math.max(1, timer.getDurationRemaining().toSeconds()), 5);
                                    final AtomicInteger finishCount = new AtomicInteger(0);

                                    ProcessTaskWrapper<String> waitForSuccess = Entities.submit(entity, newSimpleTaskFactory(String.format(JOBS_FEED_CMD, secondsLeft, containerName, namespace))
                                            .summary("Wait for success ('complete')").allowingNonZeroExitCode().newTask());
                                    Entities.submit(entity, Tasks.create("Wait for success then notify", () -> {
                                        try {
                                            if (waitForSuccess.get().contains("condition met")) LOG.debug("Container job "+namespace+" detected as completed (succeeded) in kubernetes");
                                        } finally {
                                            synchronized (finishCount) {
                                                finishCount.incrementAndGet();
                                                finishCount.notifyAll();
                                            }
                                        }
                                    }));

                                    ProcessTaskWrapper<String> waitForFailed = Entities.submit(entity, newSimpleTaskFactory(String.format(JOBS_FEED_FAILED_CMD, secondsLeft, containerName, namespace))
                                            .summary("Wait for failed").allowingNonZeroExitCode().newTask());
                                    Entities.submit(entity, Tasks.create("Wait for failed then notify", () -> {
                                        try {
                                            if (waitForFailed.get().contains("condition met")) LOG.debug("Container job "+namespace+" detected as failed in kubernetes (may be valid non-zero exit)");
                                        } finally {
                                            synchronized (finishCount) {
                                                finishCount.incrementAndGet();
                                                finishCount.notifyAll();
                                            }
                                        }
                                    }));

                                    while (finishCount.get() == 0) {
                                        LOG.debug("Container job "+namespace+" waiting on complete or failed");
                                        try {
                                            synchronized (finishCount) {
                                                finishCount.wait(Duration.TEN_SECONDS.toMilliseconds());
                                            }
                                        } catch (InterruptedException e) {
                                            throw Exceptions.propagate(e);
                                        }
                                    }

                                    if (waitForSuccess.isDone() && waitForSuccess.getExitCode() == 0) return true;
                                    if (waitForFailed.isDone() && waitForFailed.getExitCode() == 0) return false;
                                    LOG.debug("Container job "+namespace+" not yet complete, will retry");
                                    // probably timed out or job not yet available; short wait then retry
                                    Time.sleep(Duration.millis(50));
                                    if (timer.isExpired())
                                        throw new IllegalStateException("Timeout waiting for success or failure");

                                    // any other one-off checks for job error, we could do here
                                    // e.g. if image can't be pulled for instance

                                    // finally get the partial log for reporting
                                    ProcessTaskWrapper<String> outputSoFarCmd = DynamicTasks.queue(newSimpleTaskFactory(String.format(JOBS_LOGS_CMD, containerName, namespace)).summary("Retrieve output so far").allowingNonZeroExitCode().newTask());
                                    BrooklynTaskTags.setTransient(outputSoFarCmd.asTask());
                                    outputSoFarCmd.block();
                                    if (outputSoFarCmd.getExitCode()!=0) {
                                        throw new IllegalStateException("Error detected with container job while reading logs (exit code "+outputSoFarCmd.getExitCode()+"): "+outputSoFarCmd.getStdout() + " / "+outputSoFarCmd.getStderr());
                                    }
                                    String outputSoFar = outputSoFarCmd.get();
                                    String newOutput = outputSoFar.substring(stdout.size());
                                    LOG.debug("Container job "+namespace+" output: "+newOutput);
                                    stdout.write(newOutput.getBytes(StandardCharsets.UTF_8));
                                }

                            }).build()).getUnchecked();
                            LOG.debug("Container job "+namespace+" completed, success "+succeeded);

                            ProcessTaskWrapper<String> retrieveOutput = DynamicTasks.queue(newSimpleTaskFactory(String.format(JOBS_LOGS_CMD, containerName, namespace)).summary("Retrieve output").newTask());
                            result.interestingJobs.add(retrieveOutput);

                            ProcessTaskWrapper<String> retrieveExitCode = DynamicTasks.queue(newSimpleTaskFactory(String.format(PODS_EXIT_CODE_CMD, namespace)).summary("Retrieve exit code").newTask());
                            result.interestingJobs.add(retrieveExitCode);

                            DynamicTasks.waitForLast();
                            result.mainStdout = retrieveOutput.get();
                            stdout.write(result.mainStdout.substring(stdout.size()).getBytes(StandardCharsets.UTF_8));

                            String exitCodeS = retrieveExitCode.getStdout();
                            if (Strings.isNonBlank(exitCodeS)) result.mainExitCode = Integer.parseInt(exitCodeS.trim());
                            else result.mainExitCode = -1;

                            if (result.mainExitCode!=0 && config.get(REQUIRE_EXIT_CODE_ZERO)) {
                                LOG.info("Failed container job "+namespace+" (exit code "+result.mainExitCode+") output: "+result.mainStdout);
                                throw new IllegalStateException("Non-zero exit code (" + result.mainExitCode + ") disallowed");
                            }

                            return returnConversion==null ? (RET) result : returnConversion.apply(result);

                        } finally {
                            // clean up - delete namespace
                            if (!devMode && deleteNamespaceHere) {
                                LOG.debug("Deleting namespace "+namespace);
                                // do this not as a subtask so we can run even if the main queue fails
                                Entities.submit(entity, newSimpleTaskFactory(String.format(NAMESPACE_DELETE_CMD, namespace)).summary("Tear down containers").newTask()).block();
                            }
                            System.runFinalization();
                        }
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    } finally {
                        jobYaml.deleteIfTemp();
                    }
                });

        return taskBuilder.build();
    }

    public T summary(String summary) { this.summary = summary; return self(); }
    public T timeout(Duration timeout) { config.put(TIMEOUT, timeout); return self(); }
    public T command(List<String> commands) { config.put(COMMAND, commands); return self(); }
    public T command(String baseCommandWithNoArgs, String ...extraCommandArguments) { config.put(COMMAND, MutableList.of(baseCommandWithNoArgs).appendAll(Arrays.asList(extraCommandArguments))); return self(); }
    public T bashScriptCommands(List<String> commands) {
        config.put(BASH_SCRIPT, commands);
        return self();
    }
    public T bashScriptCommands(String firstCommandAndArgs, String ...otherCommandAndArgs) { return bashScriptCommands(MutableList.of(firstCommandAndArgs).appendAll(Arrays.asList(otherCommandAndArgs))); }
    public T image(String image) { config.put(CONTAINER_IMAGE, image); return self(); }
    public T allowingNonZeroExitCode() { return allowingNonZeroExitCode(true); }
    public T allowingNonZeroExitCode(boolean allowNonZero) { config.put(REQUIRE_EXIT_CODE_ZERO, !allowNonZero); return self(); }
    public T imagePullPolicy(PullPolicy policy) { config.put(CONTAINER_IMAGE_PULL_POLICY, policy); return self(); }
    @Override
    public T environmentVariables(Map<String,String> map) {
        return environmentVariablesRaw(map);
    }
    public T environmentVariablesRaw(Map<String,?> map) {
        config.put(BrooklynConfigKeys.SHELL_ENVIRONMENT, MutableMap.copyOf( map ) );
        return self();
    }

    @Override
    public T environmentVariable(String key, String val) {
        return this.environmentVariableRaw(key, (Object)val);
    }
    public T environmentVariableRaw(String key, Object val) {
        return environmentVariablesRaw(MutableMap.copyOf( config.get(BrooklynConfigKeys.SHELL_ENVIRONMENT) ).add(key, val));
    }

    @Override
    public <RET2> ContainerTaskFactory<?,RET2> returning(Function<ContainerTaskResult,RET2> conversion) {
        ContainerTaskFactory<?,RET2> result = (ContainerTaskFactory<?,RET2>) self();
        result.returnConversion = conversion;
        return result;
    }
    @Override
    public ContainerTaskFactory<?,String> returningStdout() {
        return returning(ContainerTaskResult::getMainStdout);
    }
    @Override
    public ContainerTaskFactory<?,Integer> returningExitCodeAllowingNonZero() {
        return allowingNonZeroExitCode().returning(ContainerTaskResult::getMainExitCode);
    }

    /** specify the namespace to use, and whether to create or delete it. by default a randomly generated namespace is used and always cleaned up,
     * but by using this, a caller can ensure the namespace persists. if using this, it is the caller's responsibility to avoid collisions.
     *
     * @param namespace namespace to use
     * @param create whether to create; null (default) is to auto-detect, create it if it doesn't exist
     * @param delete wehther to delete; null (default) means to delete it if and only if we created it (and not dev mode)
     */
    public T useNamespace(String namespace, Boolean create, Boolean delete) {
        this.namespace = namespace;
        this.createNamespace = create;
        this.deleteNamespace = delete;
        return self();
    }

    public T deleteNamespace(Boolean delete) { this.deleteNamespace = delete; return self(); }

    /** visible in the container environment */
    public T jobIdentifier(String jobIdentifier) {
        this.jobIdentifier = jobIdentifier;
        return self();
    }

    protected T self() { return (T)this; }

    public T configure(Map<?, ?> flags) {
        if (flags!=null)
            config.putAll(flags);
        return self();
    }

    private ProcessTaskFactory<String> newSimpleTaskFactory(final String... kubeCommands) {
        return new SystemProcessTaskFactory.ConcreteSystemProcessTaskFactory<String>(kubeCommands)
                //i think we don't care about any of these configs, and most cause debug messages about them being ignored
                //.configure(config.getAllConfig())
                //.environmentVariables(envVars) // needed to be shown in the UI ;)

                .<String>returning(ProcessTaskStub.ScriptReturnType.STDOUT_STRING)
                .requiringExitCodeZero();
    }

    public static ConcreteContainerTaskFactory<ContainerTaskResult> newInstance() {
        return new ConcreteContainerTaskFactory<>();
    }

    public static class ConcreteContainerTaskFactory<RET> extends ContainerTaskFactory<ConcreteContainerTaskFactory<RET>,RET> {
        private ConcreteContainerTaskFactory() {
            super();
        }
    }

}
