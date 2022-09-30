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
import org.apache.brooklyn.core.effector.AddEffectorInitializerAbstract;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;

import java.util.Map;
import java.util.stream.Collectors;

public class WorkflowEffector extends AddEffectorInitializerAbstract implements WorkflowCommonConfig {

    public WorkflowEffector() {}
    public WorkflowEffector(ConfigBag params) { super(params); }
    public WorkflowEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    @Override
    protected Effectors.EffectorBuilder<Object> newEffectorBuilder() {
        Effectors.EffectorBuilder<Object> eff = newAbstractEffectorBuilder(Object.class);
        eff.impl(new WorkflowEffector.Body(eff.buildAbstract(), initParams()));
        return eff;
    }

    protected static class Body extends EffectorBody<Object> {
        private final Effector<?> effector;
        private final ConfigBag definitionParams;

        public Body(Effector<?> eff, ConfigBag definitionParams) {
            this.effector = eff;
            this.definitionParams = definitionParams;

            WorkflowStepResolution.validateWorkflowParameters(entity(), definitionParams);
        }

        @Override
        public Object call(final ConfigBag invocationParams) {
            return DynamicTasks.queue( WorkflowExecutionContext.of(entity(), null, "Workflow for effector "+effector.getName(), this.definitionParams,
                    effector.getParameters().stream().map(Effectors::asConfigKey).collect(Collectors.toSet()),
                    invocationParams).getOrCreateTask().get() ).getUnchecked();
        }
    }

}
