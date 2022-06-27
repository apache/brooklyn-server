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

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.effector.AddEffectorInitializerAbstract;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.brooklyn.core.mgmt.BrooklynTaskTags.EFFECTOR_TAG;

public class DockerEffector extends AddEffectorInitializerAbstract implements  ContainerCommons {

    private static final Logger LOG = LoggerFactory.getLogger(DockerEffector.class);

    public DockerEffector() {
    }

    public DockerEffector(ConfigBag configBag) {
        super(configBag);
    }

    @Override
    protected  Effectors.EffectorBuilder<String> newEffectorBuilder() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating container effector {}", initParam(EFFECTOR_NAME));
        }
        Effectors.EffectorBuilder<String> eff = newAbstractEffectorBuilder(String.class);
        eff.impl(new Body(eff.buildAbstract(), initParams()));
        return eff;
    }

    protected static class Body extends EffectorBody<String> {
        private final Effector<String> effector;
        private final ConfigBag params;

        public Body(Effector<String> eff, final ConfigBag params) {
            this.effector = eff;
            checkNotNull(params.getAllConfigRaw().get(CONTAINER_IMAGE.getName()), "container image must be supplied when defining this effector");
            this.params = params;
        }

        @Override
        public String call(ConfigBag parameters) {
            ConfigBag configBag = ConfigBag.newInstanceCopying(this.params).putAll(parameters);
            Task<String> dockerTask = new ContainerTaskFactory.ConcreteContainerTaskFactory<String>()
                    .summary("Executing Docker Image: " + EntityInitializers.resolve(configBag, CONTAINER_IMAGE))
                    .tag(entity().getId() + "-" + EFFECTOR_TAG)
                    .configure(configBag.getAllConfig())
                    .newTask();
            DynamicTasks.queueIfPossible(dockerTask).orSubmitAsync(entity());
            Object result = dockerTask.getUnchecked(Duration.of(5, TimeUnit.MINUTES));
            return result.toString();
        }
    }
}
