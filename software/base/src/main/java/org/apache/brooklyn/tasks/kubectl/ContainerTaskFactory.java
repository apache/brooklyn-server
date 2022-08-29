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
import com.google.gson.Gson;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
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
import org.apache.brooklyn.util.core.task.TaskTags;
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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.brooklyn.tasks.kubectl.ContainerCommons.*;

public class ContainerTaskFactory<T extends ContainerTaskFactory<T,RET>,RET> implements SimpleProcessTaskFactory<T, ContainerTaskResult,RET, Task<RET>> {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerTaskFactory.class);

    protected String summary;
    protected String jobIdentifier = "";
    protected final ConfigBag config = ConfigBag.newInstance();
    private String namespace;
    private boolean namespaceRandom = false;
    private Boolean createNamespace;
    private Boolean deleteNamespace;
    Function<ContainerTaskResult,RET> returnConversion;

    private <T extends TaskAdaptable<?>> T runTask(Entity entity, T t, boolean block, boolean markTransient) {
        // previously we queued all the callers of this as sub-tasks, but that bloats the kilt diagram, so use entity.submit instead, optionally with blocking.
        // most will be transient, apart from the main flow, so that they get GC'd quicker and don't clutter the kilt
        //DynamicTasks.queue(t);

        if (markTransient) BrooklynTaskTags.setTransient(t.asTask());
        Entities.submit(entity, t);
        if (block) { t.asTask().blockUntilEnded(Duration.PRACTICALLY_FOREVER); }
        return t;
    }

    @Override
    public Task<RET> newTask() {
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        TaskBuilder<RET> taskBuilder = Tasks.<RET>builder().dynamic(true)
                .displayName(this.summary)
                .tag(BrooklynTaskTags.tagForStream(BrooklynTaskTags.STREAM_STDOUT, stdout))
                .tag(new ContainerTaskResult())
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

                    PullPolicy containerImagePullPolicy = EntityInitializers.resolve(config, CONTAINER_IMAGE_PULL_POLICY);

                    String workingDir = EntityInitializers.resolve(config, WORKING_DIR);
                    Set<Map<String,String>> volumeMounts = (Set<Map<String,String>>) EntityInitializers.resolve(config, VOLUME_MOUNTS);
                    Set<Map<String, Object>> volumes = (Set<Map<String, Object>>) EntityInitializers.resolve(config, VOLUMES);

                    final String kubeJobName = initNamespaceAndGetNewJobName();
                    String containerImage = EntityInitializers.resolve(config, CONTAINER_IMAGE);
                    Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());

                    LOG.debug("Submitting container job in namespace "+namespace+", name "+kubeJobName);

                    Map<String, String> env = new ShellEnvironmentSerializer(((EntityInternal)entity).getManagementContext()).serialize(EntityInitializers.resolve(config, SHELL_ENVIRONMENT));
                    final BrooklynBomOsgiArchiveInstaller.FileWithTempInfo<File> jobYaml =  new KubeJobFileCreator()
                            .withImage(containerImage)
                            .withImagePullPolicy(containerImagePullPolicy)
                            .withName(kubeJobName)
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

                        ContainerTaskResult result = (ContainerTaskResult) TaskTags.getTagsFast(Tasks.current()).stream().filter(x -> x instanceof ContainerTaskResult).findAny().orElseGet(() -> {
                            LOG.warn("Result object not set on tag at "+Tasks.current()+"; creating");
                            ContainerTaskResult x = new ContainerTaskResult();
                            TaskTags.addTagDynamically(Tasks.current(), x);
                            return x;
                        });
                        result.namespace = namespace;
                        result.kubeJobName = kubeJobName;

                        // validate these as they are passed to shell script, prevent injection
                        if (!namespace.matches("[A-Za-z0-9_.-]+")) throw new IllegalStateException("Invalid namespace: "+namespace);
                        if (!kubeJobName.matches("[A-Za-z0-9_.-]+")) throw new IllegalStateException("Invalid job name: "+kubeJobName);

                        ProcessTaskWrapper<ProcessTaskWrapper<?>> createNsJob = null;
                        if (!Boolean.FALSE.equals(createNamespace)) {
                            ProcessTaskFactory<ProcessTaskWrapper<?>> createNsJobF = newSimpleTaskFactory(
                                    String.format(NAMESPACE_CREATE_CMD, namespace)
                                    //, String.format(NAMESPACE_SET_CMD, namespace)
                            ).summary("Set up namespace").returning(x -> x);
                            if (createNamespace==null) {
                                createNsJobF.allowingNonZeroExitCode();
                            }
                            createNsJob = runTask(entity, createNsJobF.newTask(), true, true);
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

                            runTask(entity,
                                    newSimpleTaskFactory(String.format(JOBS_CREATE_CMD, jobYaml.getFile().getAbsolutePath(), namespace)).summary("Submit job").newTask(), true, true);

                            final CountdownTimer timer = CountdownTimer.newInstanceStarted(timeout);

                            // wait for it to be running (or failed / succeeded) -
                            PodPhases phaseOnceActive = waitForContainerAvailable(entity, kubeJobName, result, timer);
                            result.containerStarted = true;
//                            waitForContainerPodContainerState(kubeJobName, result, timer);

                            // notify once pod is available
                            synchronized (result) { result.notifyAll(); }

                            boolean succeeded = PodPhases.Succeeded == phaseOnceActive ||
                                    (PodPhases.Failed != phaseOnceActive &&
                                            //use `wait --for` api, but in a 5s loop in case there are other issues
//                                            waitForContainerCompletedUsingK8sWaitFor(stdout, kubeJobName, entity, timer)
                                            waitForContainerCompletedUsingPodState(stdout, kubeJobName, entity, timer)
                                    );

                            LOG.debug("Container job "+kubeJobName+" completed, success "+succeeded);

                            ProcessTaskWrapper<String> retrieveOutput = runTask(entity, newSimpleTaskFactory(String.format(JOBS_LOGS_CMD, kubeJobName, namespace)).summary("Retrieve output").newTask(), false, true);
                            ProcessTaskWrapper<String> retrieveExitCode = runTask(entity, newSimpleTaskFactory(String.format(PODS_EXIT_CODE_CMD, namespace, kubeJobName)).summary("Retrieve exit code").newTask(), false, true);

                            result.mainStdout = retrieveOutput.get();

                            updateStdoutWithNewData(stdout, result.mainStdout);

                            retrieveExitCode.get();
                            String exitCodeS = retrieveExitCode.getStdout();
                            if (Strings.isNonBlank(exitCodeS)) result.mainExitCode = Integer.parseInt(exitCodeS.trim());
                            else result.mainExitCode = -1;

                            result.containerEnded = true;
                            synchronized (result) { result.notifyAll(); }

                            if (result.mainExitCode!=0 && config.get(REQUIRE_EXIT_CODE_ZERO)) {
                                LOG.info("Failed container job "+namespace+" (exit code "+result.mainExitCode+") output: "+result.mainStdout);
                                throw new IllegalStateException("Non-zero exit code (" + result.mainExitCode + ") disallowed");
                            }

                            return returnConversion==null ? (RET) result : returnConversion.apply(result);

                        } finally {
                            if (deleteNamespaceHere) {
                                doDeleteNamespace(!namespaceRandom, true);  // if a one-off job, namespace has random id in it so can safely be deleted in background (no one else risks reusing it)
                            } else {
                                Boolean devMode = EntityInitializers.resolve(config, KEEP_CONTAINER_FOR_DEBUGGING);
                                if (!Boolean.TRUE.equals(devMode)) {
                                    Task<String> deletion = Entities.submit(entity, BrooklynTaskTags.setTransient(newDeleteJobTask(kubeJobName)
                                            // namespace might have been deleted in parallel so okay if we don't delete the job;
                                            .allowingNonZeroExitCode()
                                            .newTask().asTask()));
                                    // no big deal if not deleted, job ID will always be unique, so allow to delete in background and not block subsequent tasks
                                    //deletion.get();
                                }
                            }
                            DynamicTasks.waitForLast();
                        }
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    } finally {
                        jobYaml.deleteIfTemp();
                    }
                });

        return taskBuilder.build();
    }

    private Boolean waitForContainerCompletedUsingK8sWaitFor(ByteArrayOutputStream stdout, String kubeJobName, Entity entity, CountdownTimer timer) {
        return runTask(entity, Tasks.<Boolean>builder().dynamic(true).displayName("Wait for success or failure").body(() -> {
            while (true) {
                LOG.debug("Container job " + kubeJobName + " submitted, now waiting on success or failure");

                long secondsLeft = Math.min(Math.max(1, timer.getDurationRemaining().toSeconds()), 5);
                Boolean x = checkForContainerCompletedUsingK8sWaitFor(kubeJobName, entity, secondsLeft);

                if (x != null) return x;
                LOG.debug("Container job " + namespace + " not yet complete, will retry");

                // other one-off checks for job error, we could do here
                // e.g. if image can't be pulled, for instance

                refreshStdout(entity, stdout, kubeJobName, timer);

                // probably timed out or job not yet available; short wait then retry
                Time.sleep(Duration.millis(50));
            }

        }).build(), false, true).getUnchecked();
    }

    private Boolean waitForContainerCompletedUsingPodState(ByteArrayOutputStream stdout, String kubeJobName, Entity entity, CountdownTimer timer) {
        return runTask(entity, Tasks.<Boolean>builder().dynamic(true).displayName("Wait for success or failure").body(() -> {
            long retryDelay = 10;
            while (true) {
                LOG.debug("Container job " + kubeJobName + " submitted, now waiting on success or failure");

                PodPhases phase = checkPodPhase(entity, kubeJobName);
                if (phase.equals(PodPhases.Succeeded)) return true;
                if (phase.equals(PodPhases.Failed)) return false;

                LOG.debug("Container job " + namespace + " not yet complete, will sleep then retry");

                // other one-off checks for job error, we could do here
                // e.g. if image can't be pulled, for instance

                refreshStdout(entity, stdout, kubeJobName, timer);

                // probably timed out or job not yet available; short wait then retry
                Time.sleep(Duration.millis(retryDelay));
                retryDelay *= 1.5;
                if (retryDelay > 250) {
                    // max out at 500ms
                    retryDelay = 500;
                }
            }

        }).build(), false, true).getUnchecked();
    }

    private void refreshStdout(Entity entity, ByteArrayOutputStream stdout, String kubeJobName, CountdownTimer timer) throws IOException {
        // finally get the partial log for reporting
        ProcessTaskWrapper<String> outputSoFarCmd = runTask(entity,
                newSimpleTaskFactory(String.format(JOBS_LOGS_CMD, kubeJobName, namespace)).summary("Retrieve output so far").allowingNonZeroExitCode().newTask(), true, true);
        if (outputSoFarCmd.getExitCode() != 0) {
            throw new IllegalStateException("Error detected with container job while reading logs (exit code " + outputSoFarCmd.getExitCode() + "): " + outputSoFarCmd.getStdout() + " / " + outputSoFarCmd.getStderr());
        }
        updateStdoutWithNewData(stdout, outputSoFarCmd.get());

        if (timer.isExpired())
            throw new IllegalStateException("Timeout waiting for success or failure");
    }

    private void updateStdoutWithNewData(ByteArrayOutputStream receiverStream, String outputFound) throws IOException {
        int bytesAlreadyRead = receiverStream.size();
        if (bytesAlreadyRead <= outputFound.length()) {
            String newOutput = outputFound.substring(receiverStream.size());
            LOG.debug("Container job " + namespace + " output: " + newOutput);
            receiverStream.write(newOutput.getBytes(StandardCharsets.UTF_8));
        } else {
            // not sure why this happens, but it does sometimes; for now just reset
            LOG.debug("Container job " + namespace + " output reset, length " + outputFound.length() + " less than " + bytesAlreadyRead + "; ignoring new output:\n" + outputFound + "\n" + new String(receiverStream.toByteArray()));
            receiverStream.reset();
            receiverStream.write(outputFound.getBytes(StandardCharsets.UTF_8));
        }
    }

    private Boolean checkForContainerCompletedUsingK8sWaitFor(String kubeJobName, Entity entity, long timeoutSeconds) {
        final AtomicInteger finishCount = new AtomicInteger(0);

        ProcessTaskWrapper<String> waitForSuccess = Entities.submit(entity, newSimpleTaskFactory(String.format(JOBS_WAIT_COMPLETE_CMD, timeoutSeconds, kubeJobName, namespace))
                .summary("Wait for success ('complete')").allowingNonZeroExitCode().newTask());
        Entities.submit(entity, Tasks.create("Wait for success then notify", () -> {
            try {
                if (waitForSuccess.get().contains("condition met"))
                    LOG.debug("Container job " + kubeJobName + " detected as completed (succeeded) in kubernetes");
            } finally {
                synchronized (finishCount) {
                    finishCount.incrementAndGet();
                    finishCount.notifyAll();
                }
            }
        }));

        ProcessTaskWrapper<String> waitForFailed = Entities.submit(entity, newSimpleTaskFactory(String.format(JOBS_WAIT_FAILED_CMD, timeoutSeconds, kubeJobName, namespace))
                .summary("Wait for failed").allowingNonZeroExitCode().newTask());
        Entities.submit(entity, Tasks.create("Wait for failed then notify", () -> {
            try {
                if (waitForFailed.get().contains("condition met"))
                    LOG.debug("Container job " + kubeJobName + " detected as failed in kubernetes (may be valid non-zero exit)");
            } finally {
                synchronized (finishCount) {
                    finishCount.incrementAndGet();
                    finishCount.notifyAll();
                }
            }
        }));

        while (finishCount.get() == 0) {
            LOG.debug("Container job " + kubeJobName + " waiting on complete or failed");
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
        return null;
    }

    private Boolean checkForContainerCompletedUsingPodState(String kubeJobName, Entity entity, long timeoutSeconds) {
        final AtomicInteger finishCount = new AtomicInteger(0);

        ProcessTaskWrapper<String> waitForSuccess = Entities.submit(entity, newSimpleTaskFactory(String.format(JOBS_WAIT_COMPLETE_CMD, timeoutSeconds, kubeJobName, namespace))
                .summary("Wait for success ('complete')").allowingNonZeroExitCode().newTask());
        Entities.submit(entity, Tasks.create("Wait for success then notify", () -> {
            try {
                if (waitForSuccess.get().contains("condition met"))
                    LOG.debug("Container job " + kubeJobName + " detected as completed (succeeded) in kubernetes");
            } finally {
                synchronized (finishCount) {
                    finishCount.incrementAndGet();
                    finishCount.notifyAll();
                }
            }
        }));

        ProcessTaskWrapper<String> waitForFailed = Entities.submit(entity, newSimpleTaskFactory(String.format(JOBS_WAIT_FAILED_CMD, timeoutSeconds, kubeJobName, namespace))
                .summary("Wait for failed").allowingNonZeroExitCode().newTask());
        Entities.submit(entity, Tasks.create("Wait for failed then notify", () -> {
            try {
                if (waitForFailed.get().contains("condition met"))
                    LOG.debug("Container job " + kubeJobName + " detected as failed in kubernetes (may be valid non-zero exit)");
            } finally {
                synchronized (finishCount) {
                    finishCount.incrementAndGet();
                    finishCount.notifyAll();
                }
            }
        }));

        while (finishCount.get() == 0) {
            LOG.debug("Container job " + kubeJobName + " waiting on complete or failed");
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
        return null;
    }

    private PodPhases waitForContainerAvailable(Entity entity, String kubeJobName, ContainerTaskResult result, CountdownTimer timer) {
        return runTask(entity, Tasks.<PodPhases>builder().dynamic(true).displayName("Wait for container to be running (or fail)").body(() -> {
            long first = System.currentTimeMillis();
            long last = first;
            long backoffMillis = 10;
            PodPhases phase = PodPhases.Unknown;
            long startupReportDelay = 1000;  // report any start longer than 1s
            while (timer.isNotExpired()) {
                phase = checkPodPhase(entity, kubeJobName);
                if (phase!=PodPhases.Unknown && Strings.isBlank(result.kubePodName)) {
                    result.kubePodName = runTask(entity, newSimpleTaskFactory(String.format(PODS_NAME_CMD, namespace, kubeJobName)).summary("Get pod name").allowingNonZeroExitCode().newTask(), false, true).get().trim();
                }
                if (phase == PodPhases.Failed || phase == PodPhases.Succeeded || phase == PodPhases.Running) {
                    if (startupReportDelay>5000) LOG.info("Container detected in state "+phase+" after "+Duration.millis(System.currentTimeMillis()-first));
                    else LOG.debug("Container detected in state "+phase+" after "+Duration.millis(System.currentTimeMillis()-first));
                    return phase;
                }

                if (phase == PodPhases.Pending && Strings.isNonBlank(result.kubePodName)) {
                    // if pending, need to look for errors
                    String failedEvents = runTask(entity, newSimpleTaskFactory(String.format(SCOPED_EVENTS_FAILED_JSON_CMD, namespace, result.kubePodName)).summary("Check pod failed events").allowingNonZeroExitCode().newTask(),
                            false, true).get().trim();
                    if (!"[]".equals(failedEvents)) {
                        String events = runTask(entity, newSimpleTaskFactory(String.format(SCOPED_EVENTS_CMD, namespace, result.kubePodName)).summary("Get pod events on failure").allowingNonZeroExitCode().newTask(),
                                false, false).get().trim();
                        throw new IllegalStateException("Job pod failed: "+failedEvents+"\n"+events);
                    }
                }

                if (System.currentTimeMillis() - last > startupReportDelay) {
                    last = System.currentTimeMillis();

                    // log debug after 1s, then info after 5s, 20s, etc
                    // seems bad that it often takes 1s+ just to start the container :/
                    Consumer<String> log = startupReportDelay<3*1000 ? LOG::debug : LOG::info;

                    log.accept("Container taking a while to start ("+Duration.millis(last-first)+"): "+namespace+" "+ kubeJobName +" "+ result.kubePodName+" / phase '"+phase+"'");
                    String stateJsonS = runTask(entity, newSimpleTaskFactory(String.format(PODS_STATUS_STATE_CMD, namespace, kubeJobName)).summary("Get pod state").allowingNonZeroExitCode().newTask(),
                            false, true).get().trim();
                    if (Strings.isNonBlank(stateJsonS)) {
                        log.accept("Pod state: "+stateJsonS);
                    }
                    if (Strings.isNonBlank(result.kubePodName)) {
                        String events = runTask(entity, newSimpleTaskFactory(String.format(SCOPED_EVENTS_CMD, namespace, result.kubePodName)).summary("Get pod events").allowingNonZeroExitCode().newTask(),
                                false, true).get().trim();
                        log.accept("Pod events: \n"+events);
                    } else {
                        String events = runTask(entity, newSimpleTaskFactory(String.format(SCOPED_EVENTS_CMD, namespace, kubeJobName)).summary("Get job events").allowingNonZeroExitCode().newTask(),
                                false, true).get().trim();
                        log.accept("Job events: \n"+events);
                    }

                    // first 1s, then 5s, then every 20s
                    startupReportDelay *= 5;
                    if (startupReportDelay > 20*1000) startupReportDelay = 20*1000;
                }
                long backoffMillis2 = backoffMillis;
                Tasks.withBlockingDetails("waiting "+backoffMillis2+"ms for pod to be available (current status '" + phase + "')", () -> {
                    Time.sleep(backoffMillis2);
                    return null;
                });
                if (backoffMillis<80) backoffMillis*=2;
            }
            throw new IllegalStateException("Timeout waiting for pod to be available; current status is '" + phase + "'");
        }).build(), false, true).getUnchecked();
    }

    private PodPhases checkPodPhase(Entity entity, String kubeJobName) {
        PodPhases succeeded = getPodPhaseFromContainerState(entity, kubeJobName);
        if (succeeded != null) return succeeded;

        // this is the more official way, fall back to it if above is not recognised (eg waiting)
        String phase = runTask(entity, newSimpleTaskFactory(String.format(PODS_STATUS_PHASE_CMD, namespace, kubeJobName)).summary("Get pod phase").allowingNonZeroExitCode().newTask(), false, true).get().trim();
        for (PodPhases candidate: PodPhases.values()) {
            if (candidate.name().equalsIgnoreCase(phase)) return candidate;
        }
        return PodPhases.Unknown;
    }

    private PodPhases getPodPhaseFromContainerState(Entity entity, String kubeJobName) {
        // pod container state is populated much sooner than the pod status and job fields and wait, so prefer it
        String stateJsonS = runTask(entity, newSimpleTaskFactory(String.format(PODS_STATUS_STATE_CMD, namespace, kubeJobName)).summary("Get pod state").allowingNonZeroExitCode().newTask(),
                false, true).get().trim();
        if (Strings.isNonBlank(stateJsonS)) {
            Object stateO = new Gson().fromJson(stateJsonS, Object.class);
            if (stateO instanceof Map) {
                if (!((Map<?, ?>) stateO).keySet().isEmpty()) {
                    Object stateK = (((Map<?, ?>) stateO).keySet().iterator().next());
                    if (stateK instanceof String) {
                        String stateS = (String) stateK;
                        if ("terminated".equalsIgnoreCase(stateS)) return PodPhases.Succeeded;
                        if ("running".equalsIgnoreCase(stateS)) return PodPhases.Running;
                    }
                }
            }
        }
        return null;
    }

    public ProcessTaskFactory<String> newDeleteJobTask(String kubeJobName) {
        return newSimpleTaskFactory(String.format(JOBS_DELETE_CMD, kubeJobName, namespace)).summary("Delete job");
    }

    private String initNamespaceAndGetNewJobName() {
        Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
        if (entity == null) {
            throw new IllegalStateException("Task must run in context of entity to background jobs");
        }

        String containerImage = EntityInitializers.resolve(config, CONTAINER_IMAGE);
        if(Strings.isBlank(containerImage)) {
            throw new IllegalStateException("You must specify containerImage when using " + this.getClass().getSimpleName());
        }

        final String cleanImageName = containerImage.contains(":") ? containerImage.substring(0, containerImage.indexOf(":")) : containerImage;

        StringShortener ss = new StringShortener().separator("-");
        if (Strings.isNonBlank(this.jobIdentifier)) {
            ss.append("job", this.jobIdentifier).canTruncate("job", 20);
        } else {
            ss.append("brooklyn", "brooklyn").canTruncate("brooklyn", 2);
            ss.append("appId", entity.getApplicationId()).canTruncate("appId", 4);
            ss.append("entityId", entity.getId()).canTruncate("entityId", 4);
            ss.append("image", cleanImageName).canTruncate("image", 10);
        }
        ss.append("uid", Strings.makeRandomId(9)+Identifiers.makeRandomPassword(1, Identifiers.LOWER_CASE_ALPHA));
        final String kubeJobName = ss.getStringOfMaxLength(50)
                .replaceAll("[^A-Za-z0-9-]+", "-") // remove all symbols
                .toLowerCase();
        if (namespace==null) {
            namespace = kubeJobName;
            namespaceRandom = true;
        }
        return kubeJobName;
    }

    public String getNamespace() {
        return namespace;
    }

    public ProcessTaskWrapper<String> doDeleteNamespace(boolean wait, boolean requireSuccess) {
        if (namespace==null) return null;
        Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
        if (entity==null) return null;
        // clean up - delete namespace
        Boolean devMode = EntityInitializers.resolve(config, KEEP_CONTAINER_FOR_DEBUGGING);
        if (Boolean.TRUE.equals(devMode)) {
            return null;
        }

        LOG.debug("Deleting namespace " + namespace);
        // do this not as a subtask so we can run even if the main queue fails
        ProcessTaskFactory<String> tf = newSimpleTaskFactory(String.format(NAMESPACE_DELETE_CMD, namespace)).summary("Tear down containers").allowingNonZeroExitCode();
        if (!requireSuccess) tf = tf.allowingNonZeroExitCode();
        else tf = tf.requiringExitCodeZero();
        ProcessTaskWrapper<String> task = tf.newTask();
        Entities.submit(entity, BrooklynTaskTags.setTransient(task.asTask()));
        if (wait) {
            task.get();
            LOG.info("Deleted namespace " + namespace);
            System.runFinalization();
        }
        return task;
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
        config.put(SHELL_ENVIRONMENT, MutableMap.copyOf( map ) );
        return self();
    }

    @Override
    public T environmentVariable(String key, String val) {
        return this.environmentVariableRaw(key, (Object)val);
    }
    public T environmentVariableRaw(String key, Object val) {
        return environmentVariablesRaw(MutableMap.copyOf( config.get(SHELL_ENVIRONMENT) ).add(key, val));
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

    public T setDeleteNamespaceAfter(Boolean delete) { this.deleteNamespace = delete; return self(); }
    @Deprecated /** @deprecated since 1.1 when introduced */
    public T deleteNamespace(Boolean delete) { return setDeleteNamespaceAfter(delete); }

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
