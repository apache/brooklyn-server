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
package org.apache.brooklyn.core.workflow;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;

public class WorkflowInitializer extends EntityInitializers.InitializerPatternWithConfigKeys implements WorkflowCommonConfig {

    public static final Logger log = LoggerFactory.getLogger(WorkflowInitializer.class);

    public static final ConfigKey<String> WORKFLOW_NAME = ConfigKeys.newStringConfigKey("name", "Name of the workflow to run as part of entity initialization", "Workflow initializer");
    public static final ConfigKey<Object> DELAY = ConfigKeys.newConfigKey(Object.class, "delay", "Either false to run synchronously during entity initialization (the default), " +
            "true to run after management is fully started, or a duration to delay that long before running");

    private EntityLocal entity;

    public WorkflowInitializer() {}
    public WorkflowInitializer(ConfigBag params) { super(params); }
    public WorkflowInitializer(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    @Override
    public void apply(EntityLocal entity) {
        this.entity = entity;

        Object delay = initParams().get(DELAY);
        boolean delayed;
        Duration delayDuration;
        if (delay==null || Boolean.FALSE.equals(delay) || (delay instanceof String && (Strings.isBlank((String) delay) || "false".equalsIgnoreCase((String) delay) || "sync".equalsIgnoreCase((String) delay)))) {
            delayed = false;
            delayDuration = null;
        } else {
            delayed = true;
            if (Boolean.TRUE.equals(delay) || (delay instanceof String && ("true".equalsIgnoreCase((String) delay) || "async".equalsIgnoreCase((String) delay)))) {
                delayDuration = Duration.ZERO;
            } else {
                delayDuration = Duration.parse(delay.toString());
            }
        }
        String delaySummary = delayed ? ((delayDuration.isPositive() ? ""+delayDuration+" " : "") +
                "after management start") : null;

        Callable<Object> callable = () -> {
            try {
                WorkflowExecutionContext w = WorkflowExecutionContext.newInstancePersisted(entity, WorkflowExecutionContext.WorkflowContextType.OTHER,
                        initParam(WORKFLOW_NAME) + (delayed ? " (" + delaySummary + ")" : ""),
                        ConfigBag.newInstanceCopying(initParams()),
                        null, null, MutableMap.of("tags", MutableList.of(BrooklynTaskTags.ENTITY_INITIALIZATION)));

                Maybe<Task<Object>> task = w.getTask(true);

                if (task.isAbsent()) {
                    log.debug("Skipping workflow initializer on " + entity + ", condition not met: " + initParams());
                    if (delayed) DynamicTasks.queue(Tasks.warning("Skipping: condition not met", null));
                    return null;

                } else {
                    log.debug("Submitting workflow initializer on " + entity + ": " + initParams());
                    w.persist();
                    if (delayed) {
                        DynamicTasks.queue(Tasks.create("Delaying until " + delaySummary, () -> {
                            ((EntityInternal) entity).getManagementContext().waitForManagementStartupComplete(null);
                            while (!Entities.isManagedActive(entity)) {
                                if (!Entities.isManagedActiveOrComingUp(entity)) {
                                    return;
                                }
                                Time.sleep(Repeater.DEFAULT_REAL_QUICK_PERIOD);
                            }
                            if (delayDuration.isPositive()) Time.sleep(delayDuration);
                        }));
                    }
                    Object result;
                    if (delayed) {
                        result = DynamicTasks.queue(task.get());
                        DynamicTasks.waitForLast();
                    } else {
                        //result = ((EntityInternal)entity).getExecutionContext().get(task.get());
                        if (((EntityInternal) entity).getManagementSupport().wasDeployed()) {
                            result = Entities.submit(entity, task.get()).getUnchecked();
                        } else {
                            // if initializer is pre-deployment, it should run in background, deferred until after management started
                            Entities.submit(entity, task.get());
                            result = "<in progress>";
                        }
                    }
                    log.debug("Applied workflow initializer on " + entity + ", result: " + result);
                    return result;
                }
            } catch (Exception e) {
                log.warn("Error running workflow initializer (rethrowing): "+e, e);
                throw Exceptions.propagate(e);
            }
        };

        if (delayed) {
            Entities.submit(entity, Tasks.builder().displayName(initParam(WORKFLOW_NAME)).dynamic(true).body(callable).build());

        } else {
            try {
                callable.call();
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }

    }

}
