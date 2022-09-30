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
package org.apache.brooklyn.core.workflow.store;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;

import java.util.Map;
import java.util.Set;

public class WorkflowStatePersistenceViaSensors {

    public static final AttributeSensor<Map<String,WorkflowExecutionContext>> INTERNAL_WORKFLOWS = Sensors.newSensor(new TypeToken<Map<String, WorkflowExecutionContext>>() {}, "internals.brooklyn.workflow");


    private final ManagementContext mgmt;

    public WorkflowStatePersistenceViaSensors(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

    public void checkpoint(WorkflowExecutionContext context) {
        // clear interrupt status so we can persist e.g. if we are interrupted or shutdown
        boolean interrupted = Thread.interrupted();
        try {
            Entity entity = context.getEntity();
            entity.sensors().modify(INTERNAL_WORKFLOWS, v -> {
                if (v == null) v = MutableMap.of();
                v.put(context.getWorkflowId(), context);
                return Maybe.of(v);
            });
            mgmt.getRebindManager().forcePersistNow(false, null);

        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    public Map<String,WorkflowExecutionContext> getWorkflows(Entity entity) {
        Map<String, WorkflowExecutionContext> result = entity.sensors().get(INTERNAL_WORKFLOWS);
        if (result==null) result = ImmutableMap.of();
        return ImmutableMap.copyOf(result);
    }

    public void expireOldWorkflows(Entity entity) {
        // TODO
    }

}
