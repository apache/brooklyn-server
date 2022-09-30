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

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.core.effector.AddEffectorInitializerAbstract;
import org.apache.brooklyn.core.effector.EffectorTasks;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;

import java.util.Map;
import java.util.stream.Collectors;

public class WorkflowEffector extends AddEffectorInitializerAbstract implements WorkflowCommonConfig {

    private EntityLocal entity;

    public WorkflowEffector() {}
    public WorkflowEffector(ConfigBag params) { super(params); }
    public WorkflowEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    @Override
    protected Effectors.EffectorBuilder<Object> newEffectorBuilder() {
        Effectors.EffectorBuilder<Object> eff = newAbstractEffectorBuilder(Object.class);
        eff.impl(new BodyFactory( entity, eff.buildAbstract(), initParams() ));
        return eff;
    }

    @Override
    public void apply(EntityLocal entity) {
        this.entity = entity;
        super.apply(entity);
    }

    protected static class BodyFactory extends EffectorTasks.EffectorBodyTaskFactory<Object> {
        // extending the class above means that our newTask is called synchronously at invocation time;
        // we make sure to set the right flags for our task to look like an effector call,
        // so effector can be re-invoked, or workflow can be replayed.
        private final ConfigBag definitionParams;

        public BodyFactory(Entity entity, Effector<?> eff, ConfigBag definitionParams) {
            super(null);
            this.definitionParams = definitionParams;

            WorkflowStepResolution.validateWorkflowParameters(entity, definitionParams);
        }

        public Task<Object> newTask(Entity entity, Effector<Object> effector, ConfigBag invocationParams) {
            return WorkflowExecutionContext.of(entity, null, "Workflow for effector "+effector.getName(), this.definitionParams,
                    effector.getParameters().stream().map(Effectors::asConfigKey).collect(Collectors.toSet()),
                    invocationParams,
                    getFlagsForTaskInvocationAt(entity, effector, invocationParams)).getOrCreateTask().get();
        }
    }

}
