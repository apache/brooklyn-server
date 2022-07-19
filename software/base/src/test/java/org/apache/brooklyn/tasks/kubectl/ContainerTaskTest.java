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

import com.beust.jcommander.internal.Maps;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.assertTrue;

/**
 * These tests require Minikube installed locally
 */
@Test(groups = {"Live"})
public class ContainerTaskTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testSuccessfulContainerTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Task<ContainerTaskFactory.ContainerTaskResult> containerTask =  new ContainerTaskFactory.ConcreteContainerTaskFactory()
                .summary("Running container task")
                .jobIdentifier("test-container-task")
                .imagePullPolicy(PullPolicy.IF_NOT_PRESENT)
                .timeout(Duration.TWO_MINUTES)
                .image("perl")
                .command( "/bin/bash", "-c","echo 'hello test'" )
                .newTask();

        DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);
        ContainerTaskFactory.ContainerTaskResult result = containerTask.getUnchecked(Duration.ONE_MINUTE);
        Asserts.assertEquals(result.getMainStdout().trim(), "hello test");
    }

    @Test
    public void testContainerTaskWithVar() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Task<ContainerTaskFactory.ContainerTaskResult> containerTask =  new ContainerTaskFactory.ConcreteContainerTaskFactory()
                .summary("Running container task")
                .jobIdentifier("test-container-task")
                .imagePullPolicy(PullPolicy.IF_NOT_PRESENT)
                .timeout(Duration.TWO_MINUTES)
                .image("perl")
                .env("test_name", "EnvTest")
                .bashScriptCommands("echo hello ${test_name}" )
                .newTask();

        DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);
        ContainerTaskFactory.ContainerTaskResult result = containerTask.getUnchecked(Duration.ONE_MINUTE);
        Asserts.assertEquals(result.getMainStdout().trim(), "hello EnvTest");
        Asserts.assertEquals(BrooklynTaskTags.stream(containerTask, BrooklynTaskTags.STREAM_ENV).streamContents.get().trim(), "test_name=\"EnvTest\"");
    }

    @Test
    public void testSuccessfulContainerTerraformTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Task<ContainerTaskFactory.ContainerTaskResult> containerTask = new ContainerTaskFactory.ConcreteContainerTaskFactory()
                .summary("Running terraform-container task")
                .jobIdentifier("test-container-task")
                .timeout(Duration.TWO_MINUTES)
                .image("hashicorp/terraform:latest")
                .imagePullPolicy(PullPolicy.IF_NOT_PRESENT)
                .command( "terraform", "version" )
                .newTask();
        DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);

        ContainerTaskFactory.ContainerTaskResult result = containerTask.getUnchecked();
        assertTrue(result.getMainStdout().startsWith("Terraform"));
    }

    @Test // execute local command, assert we get exit code, and it fails
    public void testExpectedFailingContainerTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Task<ContainerTaskFactory.ContainerTaskResult> containerTask =  new ContainerTaskFactory.ConcreteContainerTaskFactory()
                .summary("Running container task")
                .jobIdentifier("test-container-task")
                .imagePullPolicy(PullPolicy.IF_NOT_PRESENT)
                .timeout(Duration.TWO_MINUTES)
                .image("perl")
                .command( "/bin/bash", "-c","echo 'hello test' && exit 42" )
                .allowNonZeroExitCode()
                .newTask();
        DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);

        Asserts.assertTrue(containerTask.blockUntilEnded(Duration.ONE_MINUTE));  // should complete in 1 minute, i.e. we detect the failed
        Asserts.assertTrue(containerTask.isDone());
        Asserts.assertEquals((int)containerTask.getUnchecked().getMainExitCode(), 42);
        Asserts.assertEquals(containerTask.getUnchecked().getMainStdout().trim(), "hello test");
    }


    @Test // execute local command, assert we get exit code, and it fails
    public void testSleepingAndExpectedFailingContainerTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Task<ContainerTaskFactory.ContainerTaskResult> containerTask =  new ContainerTaskFactory.ConcreteContainerTaskFactory()
                .summary("Running container task")
                .jobIdentifier("test-container-task")
                .imagePullPolicy(PullPolicy.IF_NOT_PRESENT)
                .timeout(Duration.TWO_MINUTES)
                .image("perl")
                .bashScriptCommands("echo starting", "sleep 6", "echo done", "exit 42", "echo ignored")
                .allowNonZeroExitCode()
                .newTask();
        DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);

        Asserts.eventually(() -> {
                    BrooklynTaskTags.WrappedStream stream = BrooklynTaskTags.stream(containerTask, BrooklynTaskTags.STREAM_STDOUT);
                    if (stream==null) return null;
                    return stream.streamContents.get().trim();
                }, x -> x.equals("starting"));
        Asserts.assertFalse(containerTask.isDone());

        // may be occasional timing glitches here, eg if namespace cleanup takes > 3s
        Stopwatch sw = Stopwatch.createStarted();
        // should complete in under 10s, i.e. we detect the failed in less than 5s after
        Asserts.assertTrue(containerTask.blockUntilEnded(Duration.seconds(15)), "should definitely finish within 15s of starting");
        Asserts.assertThat(Duration.of(sw.elapsed()), dur -> dur.isShorterThan(Duration.seconds(10)));

        Asserts.assertTrue(containerTask.isDone());
        Asserts.assertEquals(BrooklynTaskTags.stream(containerTask, BrooklynTaskTags.STREAM_STDOUT).streamContents.get().trim(), "starting\ndone");
        Asserts.assertEquals((int)containerTask.getUnchecked().getMainExitCode(), 42);
        Asserts.assertEquals(containerTask.getUnchecked().getMainStdout().trim(), "starting\ndone");
    }

    @Test // execute local command, assert fails if exit code not allowed to be zero
    public void testNotExpectedFailingContainerTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Task<ContainerTaskFactory.ContainerTaskResult> containerTask =  new ContainerTaskFactory.ConcreteContainerTaskFactory()
                .summary("Running container task")
                .jobIdentifier("test-container-task")
                .imagePullPolicy(PullPolicy.IF_NOT_PRESENT)
                .timeout(Duration.TWO_MINUTES)
                .image("perl")
                .command( "/bin/bash", "-c","echo 'hello test' && exit 42" )
//                .allowNonZeroExitCode()
                .newTask();
        DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);

        Asserts.assertTrue(containerTask.blockUntilEnded(Duration.ONE_MINUTE));  // should complete in 1 minute, i.e. we detect the failed
        Asserts.assertTrue(containerTask.isDone());
        Asserts.assertTrue(containerTask.isError());
        Asserts.assertFailsWith(() -> containerTask.getUnchecked(), error -> Asserts.expectedFailureContainsIgnoreCase(error, "Non-zero", "42"));
    }

    @Test
    public void testScriptContainerTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        Map<String,Object> volumes = Maps.newHashMap();
        String volumeId = "brooklyn-container-task-test-volume";
        String uid = Identifiers.makeRandomId(8).toLowerCase();
        volumes.put("name", volumeId);
        // sometimes this can be mounted from local drive, but does not always need to be
        volumes.put("hostPath", Maps.newHashMap("path", "/tmp/brooklyn-container-task-test-shared"));

        Map<String,Object> configBag = new HashMap<>();
        configBag.put("workingDir", "/brooklyn-mount-dir/scripts");
        configBag.put("volumes", Sets.newHashSet(volumes));
        configBag.put("volumeMounts", Sets.newHashSet(Maps.newHashMap("name", volumeId, "mountPath", "/brooklyn-mount-dir")));

        ContainerTaskFactory.ConcreteContainerTaskFactory baseFactory = new ContainerTaskFactory.ConcreteContainerTaskFactory()
                .summary("Running container task")
                .jobIdentifier("test-container-task")
                .imagePullPolicy(PullPolicy.IF_NOT_PRESENT)
                .timeout(Duration.TWO_MINUTES)
                .image("hhwang927/ubuntu_base")
                .useNamespace("brooklyn-container-test-"+uid, null, false)
                .configure(configBag);

        try {
            // first create the script
            Task<ContainerTaskFactory.ContainerTaskResult> setup = baseFactory.bashScriptCommands(
                    "mkdir -p /brooklyn-mount-dir/scripts",
                    "cd /brooklyn-mount-dir/scripts",
                    "echo echo hello " + uid + " > hello-"+uid+".sh",
                    "chmod +x hello-"+uid+".sh"
            ).newTask();
            DynamicTasks.queueIfPossible(setup).orSubmitAsync(entity).getTask().getUnchecked();

            // now make a new container that should see the same mount point, and try running the command
            Task<ContainerTaskFactory.ContainerTaskResult> containerTask = baseFactory.bashScriptCommands(
                            "./hello-"+uid+".sh"
                    )
                    .newTask();
            DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);
            ContainerTaskFactory.ContainerTaskResult result = containerTask.getUnchecked(Duration.ONE_MINUTE);
            Asserts.assertEquals(result.getMainStdout().trim(), "hello " + uid);

        } finally {
            DynamicTasks.queueIfPossible( baseFactory.summary("cleaning up").deleteNamespace(true).bashScriptCommands("rm hello-"+uid+".sh") ).orSubmitAsync(entity);
        }
    }

}
