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

import java.util.Map;
import java.util.concurrent.Callable;

import com.google.common.annotations.Beta;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.BasicAttributeSensorAndConfigKey;
import org.apache.brooklyn.util.core.task.ScheduledTask;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.time.Duration;

/**
 * The DashboardAggregator is an enricher that combines config/sensor values from the children of the entity it is attached to.
 * The combined values are set as sensors on the entity that it is attached to. Preference is given to the child entities sensors values,
 * but if none are set, config values are used instead.
 *
 * The reason that this exists is to provide high level summary information, that could be useful to display on a dashboard.
 * Whilst brooklyn itself has no such dashboard, we can imagine this entity being used in that fashion.
 *
 * Please note, that the DashboardAggregator will aggregate all descendants of the entity it is attached to, even intermediate level children.
 * Therefore, please only ever attach one DashboardAggregator to the top most entity.
 *
 * For a detailed list of the config that is combined, please see the AggregationJob class.
 *
 */
@Beta
public class DashboardAggregator extends AbstractEnricher {

    private ScheduledTask task;

    public static BasicAttributeSensorAndConfigKey<Duration> DASHBOARD_COST_PER_MONTH = new BasicAttributeSensorAndConfigKey(Duration.class,
            "dashboard.period",
            "The amount of time to wait between aggregation jobs",
            Duration.seconds(1));

    @SuppressWarnings("unchecked")
    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);

        Duration duration = config().get(DASHBOARD_COST_PER_MONTH);

        Callable<Task<?>> taskFactory = () -> Tasks.builder()
                .dynamic(false)
                .body(new AggregationJob(entity))
                .displayName("DashboardAggregator task")
                .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                .description("Retrieves and aggregates sensor values")
                .build();

        task = ScheduledTask.builder(taskFactory).period(duration).displayName("scheduled:[DashboardAggregator task]").tagTransient().build();
        this.getManagementContext().getExecutionManager().submit(task);

    }

    @Override
    public void destroy() {
        super.destroy();
        if (task != null) {
            task.cancel();
        }
    }
}