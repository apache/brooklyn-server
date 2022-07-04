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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertTrue;

/**
 * These tests require Minikube installed locally
 */
@Test(groups = {"Live"})
public class ContainerTaskTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testSuccessfulContainerTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Map<String,Object> configBag = new HashMap<>();
        configBag.put("name", "test-container-task");
        configBag.put("image", "perl");
        configBag.put("commands", Lists.newArrayList("/bin/bash", "-c","echo 'hello test'"));
        configBag.put("imagePullPolicy", PullPolicy.IF_NOT_PRESENT);

        Task<String> containerTask =  new ContainerTaskFactory.ConcreteContainerTaskFactory<String>()
                .summary("Running container task")
                .configure(configBag)
                .newTask();
        DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);
        Object result = containerTask.getUnchecked(Duration.of(5, TimeUnit.MINUTES));
        List<String> res = (List<String>) result;
        while(!res.isEmpty() && Iterables.getLast(res).matches("namespace .* deleted\\s*")) res = res.subList(0, res.size()-1);

        String res2 = res.isEmpty() ? null : Iterables.getLast(res);
        assertTrue(res2.startsWith("hello test"));
    }

    @Test
    public void testSuccessfulContainerTerraformTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Map<String,Object> configBag = new HashMap<>();
        configBag.put("name", "test-container-task");
        configBag.put("image", "hashicorp/terraform:latest");
        configBag.put("commands",  ImmutableList.of("terraform", "version" ));
        configBag.put("imagePullPolicy", PullPolicy.IF_NOT_PRESENT);

        Task<String> containerTask =  new ContainerTaskFactory.ConcreteContainerTaskFactory<String>()
                .summary("Running terraform-container task")
                .configure(configBag)
                .newTask();
        DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity);
        Object result = containerTask.getUnchecked(Duration.of(5, TimeUnit.MINUTES));
        List<String> res = (List<String>) result;
        while(!res.isEmpty() && Iterables.getLast(res).matches("namespace .* deleted\\s*")) res = res.subList(0, res.size()-1);

        String res2 = res.isEmpty() ? null : Iterables.getLast(res);
        assertTrue(res2.startsWith("Terraform"));
    }

    @Test// tries to execute local command, wants it to fail, but even so best as integration
    public void testFailingContainerTask() {
        TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        List<String> commands = MutableList.of("/bin/bash", "-c","echo 'hello test' & exit 1");

        Map<String,Object> configBag = new HashMap<>();
        configBag.put("name", "test-container-task");
        configBag.put("image", "perl");
        configBag.put("commands", commands);

        Task<String> containerTask =  new ContainerTaskFactory.ConcreteContainerTaskFactory<String>()
                .summary("Running docker task")
                .configure(configBag)
                .newTask();
        try {
            DynamicTasks.queueIfPossible(containerTask).orSubmitAsync(entity).getTask().get();
            if (containerTask instanceof HasTaskChildren) {
                for (Task<?> child: ((HasTaskChildren)containerTask).getChildren()) {
                    if(child.getTags().contains(BrooklynTaskTags.INESSENTIAL_TASK) && child.isError()) {
                       child.get();
                    }
                }
            }
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Process task ended with exit code", "when 0 was required");
        }
    }

}
