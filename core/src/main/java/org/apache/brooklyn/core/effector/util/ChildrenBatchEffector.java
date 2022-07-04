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
package org.apache.brooklyn.core.effector.util;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.AddEffectorInitializerAbstract;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.EffectorTasks.EffectorBodyTaskFactory;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Effector to  invoking an effector across children or members,
 */
public class ChildrenBatchEffector extends AddEffectorInitializerAbstract {
    public static final Effector<Object> EFFECTOR = Effectors.effector(Object.class, "childrenBatchEffector").
            description("Invokes an effector e.g. across children or members").buildAbstract();
    public static final ConfigKey<String> EFFECTOR_NAME = ConfigKeys.builder(String.class)
            .name("name")
            .description("Effector name")
            .defaultValue("childrenBatchEffector")
            .build();
    public static final ConfigKey<Integer> BATCH_SIZE = ConfigKeys.builder(Integer.class)
            .name("batchSize")
            .description("Supply a limit to the number of elements replaced at a time; 0 (default) means no limit")
            .defaultValue(0)
            .build();
    public static final ConfigKey<String> EFFECTOR_TO_INVOKE = ConfigKeys.builder(String.class)
            .name("effectorToInvoke")
            .description("Name of the effector to invoke; if not supplied will use 'repave'")
            .defaultValue("repave")
            .build();
    public static final ConfigKey<Boolean> FAIL_ON_MISSING_EFFECTOR_TO_INVOKE = ConfigKeys.builder(Boolean.class)
            .name("failOnMissingEffector")
            .description("Whether to fail (true) or ignore (false, default) if an entity does not have a matching target effector")
            .defaultValue(false)
            .build();
    public static final ConfigKey<Boolean> FAIL_ON_EFFECTOR_FAILURE = ConfigKeys.builder(Boolean.class)
            .name("failOnEffectorFailure")
            .description("Whether to fail (true) or not (false, default) if an effector invocation failed")
            .defaultValue(false)
            .build();
    public static final MapConfigKey<Object> EFFECTOR_ARGS = new MapConfigKey.Builder<Object>(Object.class, "effectorArgs")
            .build();
    private static final Logger log = LoggerFactory.getLogger(ChildrenBatchEffector.class);

    public ChildrenBatchEffector(ConfigBag params) {
        super(params);
    }

    @Override
    protected Effectors.EffectorBuilder<Object> newEffectorBuilder() {
        Effectors.EffectorBuilder<Object> eff = newAbstractEffectorBuilder(Object.class);
        eff.impl(new ChildrenBatchEffector.ChildrenBatchEffectorBody(initParams()));
        return eff;    }

    public ChildrenBatchEffector(Map<String, String> params) {
        this(ConfigBag.newInstance(params));
    }


    // wire up the entity to call the task factory to create the task on invocation

    private static Effector<Object> makeEffector(ConfigBag paramsCreationTime) {
        return Effectors.effector(EFFECTOR)
                .name(paramsCreationTime.get(EFFECTOR_NAME))
                .parameter(Integer.class, BATCH_SIZE.getName(), BATCH_SIZE.getDescription(), paramsCreationTime.get(BATCH_SIZE))
                .parameter(String.class, EFFECTOR_TO_INVOKE.getName(), EFFECTOR_TO_INVOKE.getDescription(), paramsCreationTime.get(EFFECTOR_TO_INVOKE))
                .parameter(Boolean.class, FAIL_ON_MISSING_EFFECTOR_TO_INVOKE.getName(), FAIL_ON_MISSING_EFFECTOR_TO_INVOKE.getDescription(), paramsCreationTime.get(FAIL_ON_MISSING_EFFECTOR_TO_INVOKE))
                .parameter(Boolean.class, FAIL_ON_EFFECTOR_FAILURE.getName(), FAIL_ON_EFFECTOR_FAILURE.getDescription(), paramsCreationTime.get(FAIL_ON_EFFECTOR_FAILURE))
                .parameter(EFFECTOR_ARGS)
                .impl(new EffectorBodyTaskFactory<>(new ChildrenBatchEffectorBody(paramsCreationTime))).build();
    }

    @Override
    public void apply(@SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity) {
        ((EntityInternal) entity).getMutableEntityType().addEffector(makeEffector(initParams()));
    }

    protected static class ChildrenBatchEffectorBody extends EffectorBody<Object> {

        private final ConfigBag paramsCreationTime;

        public ChildrenBatchEffectorBody(ConfigBag paramsCreationTime) {
            this.paramsCreationTime = paramsCreationTime;
        }

        @Override
        public Object call(ConfigBag explicitEffectorInvocationParams) {
            ConfigBag params = ConfigBag.newInstanceCopying(paramsCreationTime);
            explicitEffectorInvocationParams.getAllConfig().forEach((k, v) ->
            {
                if (v != null) params.putStringKey(k, v);
            });

            final int batchSize = params.get(BATCH_SIZE);

            // pass through parameters that were explicitly defined, and those supplied as extra args
            // but defaults specified in the effector initializer block (in yaml) do _not_ get passed through
            ConfigBag argsToPass = ConfigBag.newInstanceCopying(explicitEffectorInvocationParams).putAll(params.get(EFFECTOR_ARGS));

            final boolean failOnMissingEffector = params.get(FAIL_ON_MISSING_EFFECTOR_TO_INVOKE);
            final boolean failOnEffectorFailure = params.get(FAIL_ON_EFFECTOR_FAILURE);

            List<Task<?>> activeTasks = MutableList.of();
            List<Entity> items = MutableList.copyOf(entity().getChildren());
            int initialSize = items.size();
            int count = 0;

            while (!items.isEmpty()) {
                Entity child = items.remove(0);
                String effectorName = params.get(EFFECTOR_TO_INVOKE);

                Effector<?> effector = ((EntityInternal) child).getMutableEntityType().getEffector(effectorName);
                if (effector == null) {
                    if (failOnMissingEffector) {
                        throw new IllegalStateException("No effector '" + effectorName + "' found on " + child);
                    } else {
                        // Skipping
                        log.debug("Skipping {} when invoking effector on {} because it does not have an effector '{}'", child, entity(), effectorName);
                    }
                } else {
                    Task<Object> t = Tasks.builder()
                            .displayName("Invoking " + effectorName + " on " + child.getDisplayName() + " (" + child.getId() + ")")
                            .swallowChildrenFailures(!failOnEffectorFailure)
                            .add(Effectors.invocation(effector, argsToPass.getAllConfig(), child)).build();
                    activeTasks.add(t);
                    BasicExecutionContext.getCurrentExecutionContext().submit(t);
                    count++;
                    queue(t);
                }

                if (batchSize > 0 && !items.isEmpty()) {
                    while (activeTasks.size() >= batchSize) {
                        activeTasks.removeIf(Task::isDone);
                        if (activeTasks.size() >= batchSize) {
                            try {
                                Tasks.withBlockingDetails("Waiting for something in current batch of " + batchSize + " to complete "
                                                + "before submitting the remaining " + items.size() + " invocation" + Strings.s(items.size()),
                                        () -> {
                                            Time.sleep(Duration.millis(100));
                                            return null;
                                        });
                            } catch (Exception e) {
                                throw Exceptions.propagate(e);
                            }
                        }
                    }
                }
            }
            return "Invoked " + count + " of " + initialSize + " effectors";
        }
    }
}
