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
package org.apache.brooklyn.camp.brooklyn;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.util.core.task.TaskPredicates;
import org.apache.brooklyn.util.text.StringPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
 * Tests Windows YAML blueprint features.
 */
public abstract class AbstractWindowsYamlTest extends AbstractYamlTest {
    
    // TODO Remove duplication of assertStreams and VanillaWindowsProcessWinrmStreamsLiveTest.assertStreams
    
    private static final Logger log = LoggerFactory.getLogger(AbstractWindowsYamlTest.class);

    @Override
    protected ManagementContextInternal mgmt() {
        return (ManagementContextInternal) super.mgmt();
    }
    
    protected void assertStreams(SoftwareProcess entity, Map<String, List<String>> stdouts) {
        Set<Task<?>> tasks = BrooklynTaskTags.getTasksInEntityContext(mgmt().getExecutionManager(), entity);

        for (Map.Entry<String, List<String>> entry : stdouts.entrySet()) {
            String taskNameRegex = entry.getKey();
            List<String> expectedOuts = entry.getValue();

            Task<?> subTask = findTaskOrSubTask(tasks, TaskPredicates.displayNameSatisfies(StringPredicates.matchesRegex(taskNameRegex))).get();

            String stdin = getStreamOrFail(subTask, BrooklynTaskTags.STREAM_STDIN);
            String stdout = getStreamOrFail(subTask, BrooklynTaskTags.STREAM_STDOUT);
            String stderr = getStreamOrFail(subTask, BrooklynTaskTags.STREAM_STDERR);
            String env = getStream(subTask, BrooklynTaskTags.STREAM_ENV);
            String msg = "stdin="+stdin+"; stdout="+stdout+"; stderr="+stderr+"; env="+env;

            for (String expectedOut : expectedOuts) {
                assertTrue(stdout.contains(expectedOut), msg);
            }
        }
    }

    protected void assertSubTaskFailures(SoftwareProcess entity, Map<String, Predicate<CharSequence>> taskErrs) throws Exception {
        Set<Task<?>> tasks = BrooklynTaskTags.getTasksInEntityContext(mgmt().getExecutionManager(), entity);

        for (Map.Entry<String, Predicate<CharSequence>> entry : taskErrs.entrySet()) {
            String taskNameRegex = entry.getKey();
            Predicate<? super String> errChecker = entry.getValue();
            Task<?> subTask = findTaskOrSubTask(tasks, TaskPredicates.displayNameSatisfies(StringPredicates.matchesRegex(taskNameRegex))).get();
            String msg = "regex="+taskNameRegex+"; task="+subTask;
            assertNotNull(subTask, msg);
            assertTrue(subTask.isDone(), msg);
            assertTrue(subTask.isError(), msg);
            try {
                subTask.get();
                fail();
            } catch (Exception e) {
                if (!errChecker.apply(e.toString())) {
                    throw e;
                }
            }
        }
    }

    public static String getStreamOrFail(Task<?> task, String streamType) {
        String msg = "task="+task+"; stream="+streamType;
        BrooklynTaskTags.WrappedStream stream = checkNotNull(BrooklynTaskTags.stream(task, streamType), "Stream null: " + msg);
        return checkNotNull(stream.streamContents.get(), "Contents null: "+msg);
    }

    public static String getStream(Task<?> task, String streamType) {
        BrooklynTaskTags.WrappedStream stream = BrooklynTaskTags.stream(task, streamType);
        return (stream != null) ? stream.streamContents.get() : null;
    }

    protected Optional<Task<?>> findTaskOrSubTask(Entity entity, Predicate<? super Task<?>> matcher) {
        return findTaskOrSubTask(BrooklynTaskTags.getTasksInEntityContext(mgmt().getExecutionManager(), entity), matcher);
    }
    
    protected Optional<Task<?>> findTaskOrSubTask(Iterable<? extends Task<?>> tasks, Predicate<? super Task<?>> matcher) {
        List<String> taskNames = Lists.newArrayList();
        Optional<Task<?>> result = findTaskOrSubTaskImpl(tasks, matcher, taskNames);
        if (!result.isPresent() && log.isDebugEnabled()) {
            log.debug("Task not found matching "+matcher+"; contender names were "+taskNames);
        }
        return result;
    }

    protected Optional<Task<?>> findTaskOrSubTaskImpl(Iterable<? extends Task<?>> tasks, Predicate<? super Task<?>> matcher, List<String> taskNames) {
        for (Task<?> task : tasks) {
            if (matcher.apply(task)) return Optional.<Task<?>>of(task);

            if (task instanceof HasTaskChildren) {
                Optional<Task<?>> subResult = findTaskOrSubTask(((HasTaskChildren) task).getChildren(), matcher);
                if (subResult.isPresent()) return subResult;
            }
        }

        return Optional.<Task<?>>absent();
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
