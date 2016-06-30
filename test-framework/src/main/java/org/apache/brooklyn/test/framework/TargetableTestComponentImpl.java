/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that can resolve the target for a test component
 */
public abstract class TargetableTestComponentImpl extends AbstractEntity implements TargetableTestComponent {

    private static final Logger LOG = LoggerFactory.getLogger(TargetableTestComponentImpl.class);

    /**
     * Find the target entity using "target" config key, if entity provided directly in config, or by doing an implicit
     * lookup using DSL ($brooklyn:component("myNginX")), if id of entity provided as "targetId" config key.
     *
     * @return The target entity.
     * @throws @RuntimeException if no target can be determined.
     */
    public Entity resolveTarget() {
        return resolveTarget(getExecutionContext(), this);
    }

    /**
     * Find the target entity in the given execution context.
     *
     * @see {@link #resolveTarget()}.
     */
    public static Entity resolveTarget(ExecutionContext executionContext, Entity entity) {
        Entity target = entity.getConfig(TARGET_ENTITY);
        if (null == target) {
            target = getTargetById(executionContext, entity);
        }
        return target;
    }

    private static Entity getTargetById(final ExecutionContext executionContext, final Entity entity) {
        final String targetId = entity.getConfig(TARGET_ID);
        Duration resolutionTimeout = entity.getConfig(TARGET_RESOLUTION_TIMEOUT);
        
        if(targetId == null){
            return null;
        }

        final AtomicReference<Entity> result = new AtomicReference<>();
        final DslComponent dslComponent = new DslComponent(targetId);
        Callable<Boolean> resolver = new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
                Task<Entity> task = dslComponent.newTask();
                result.set(Tasks.resolveValue(task, Entity.class, executionContext, "Finding entity " + targetId));
                return true;
            }
        };
        try {
            if (resolutionTimeout == null || resolutionTimeout.toMilliseconds() <= 0) {
                resolver.call();
            } else {
                Repeater.create("find entity "+targetId)
                        .backoffTo(resolutionTimeout.multiply(0.1))
                        .limitTimeTo(resolutionTimeout)
                        .rethrowException()
                        .until(resolver)
                        .runRequiringTrue();
            }
            LOG.debug("Found target {} by id {}", result.get(), targetId);
            return result.get();
        } catch (Exception e) {
            LOG.error("Error finding target {}", targetId);
            throw Exceptions.propagate(e);
        }
    }
}
