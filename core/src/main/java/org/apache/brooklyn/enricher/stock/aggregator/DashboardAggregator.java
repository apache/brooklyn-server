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
package org.apache.brooklyn.enricher.stock.aggregator;

import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.task.ScheduledTask;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.time.Duration;

public class DashboardAggregator extends AbstractEnricher {

    @SuppressWarnings("unchecked")
    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        Callable<Task<?>> taskFactory = () -> Tasks.builder()
                .dynamic(false)
                .body(new AggregationJob(entity))
                .displayName("DashboardAggregator task")
                .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                .description("Retrieves and aggregates sensor values")
                .build();

        ScheduledTask task = ScheduledTask.builder(taskFactory).period(Duration.seconds(1)).displayName("scheduled:[DashboardAggregator task]").tagTransient().build();
        this.getManagementContext().getExecutionManager().submit(task);

    }

}