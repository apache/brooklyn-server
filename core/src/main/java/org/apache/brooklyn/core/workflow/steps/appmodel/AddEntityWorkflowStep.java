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
package org.apache.brooklyn.core.workflow.steps.appmodel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.internal.EntityManagerInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepResolution;
import org.apache.brooklyn.core.workflow.steps.appmodel.DeployApplicationWorkflowStep.StartMode;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class AddEntityWorkflowStep extends WorkflowStepDefinition implements HasBlueprintWorkflowStep {

    private static final Logger LOG = LoggerFactory.getLogger(AddEntityWorkflowStep.class);

    public static final String SHORTHAND = "[ ${type} ]";

    public static final ConfigKey<String> FORMAT = ConfigKeys.newStringConfigKey("format");

    // sync is not completely idempotent (in the call to start it) but very useful for testing;
    // option not included in documentation, but used for tests
    public static final ConfigKey<StartMode> START = ConfigKeys.newConfigKey(StartMode.class, "start", "Default 'async'");

    @Override
    public Logger logger() {
        return LOG;
    }

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep(WorkflowStepResolution workflowStepResolution) {
        super.validateStep(workflowStepResolution);
        validateStepBlueprint(workflowStepResolution);
    }

    @Override
    protected List<String> getStepState(WorkflowStepInstanceExecutionContext context) {
        return (List<String>) super.getStepState(context);
    }
    void setStepState(WorkflowStepInstanceExecutionContext context, List<String> entityIds) {
        context.setStepState(entityIds, true);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Object blueprint = resolveBlueprint(context);

        List<String> preCreatedEntityIds = Maybe.ofDisallowingNull(getStepState(context)).map(MutableList::copyOf).orNull();
        List<String> newCreatedEntityIds = MutableList.of();

        List<EntitySpec<?>> specs;
        ManagementContext mgmt = context.getManagementContext();
        try {
            specs = blueprint instanceof EntitySpec ? MutableList.of((EntitySpec) blueprint)
                    : EntityManagementUtils.getAddChildrenSpecs(mgmt,
                      blueprint instanceof String ? (String) blueprint :
                            BeanWithTypeUtils.newYamlMapper(mgmt, false, null, false).writeValueAsString(blueprint));
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }

        specs.forEach(spec -> spec.parent(Entities.proxy(context.getEntity())));

        StartMode start = context.getInput(START);

        List<Task<?>> startTasks = MutableList.of();
        List<Entity> childEntities = MutableList.of();

        Map<String,Object> result = MutableMap.of("entities", childEntities);
        context.setOutput(result);

        Map<String,EntitySpec<?>> specsToCreate = MutableMap.of();

        specs.forEach(spec -> {
            String entityId;
            Entity child = null;
            if (preCreatedEntityIds != null) {
                entityId = preCreatedEntityIds.remove(0);
                child = context.getManagementContext().getEntityManager().getEntity(entityId);
            } else {
                entityId = Identifiers.makeRandomLowercaseId(10);
            }
            if (child==null) {
                specsToCreate.put(entityId, spec);
            } else {
                childEntities.add(child);
            }
            newCreatedEntityIds.add(entityId);
        });

        setStepState(context, newCreatedEntityIds);

        specsToCreate.forEach( (entityId, spec) -> {
            Entity child = ((EntityInternal) context.getEntity()).getExecutionContext().get(Tasks.<Entity>builder().dynamic(false)
                    .displayName("Creating entity " +
                            (Strings.isNonBlank(spec.getDisplayName()) ? spec.getDisplayName() : spec.getType().getName()))
                    .body(() -> ((EntityManagerInternal) mgmt.getEntityManager()).createEntity(spec, Optional.of(entityId)))
                    .build());
            childEntities.add(child);

            if (start == StartMode.DISABLED) {
                // nothing
            } else {
                if (child instanceof Startable) {
                    startTasks.add(Entities.submit(child, Effectors.invocation(child, Startable.START, ImmutableMap.of("locations", ImmutableList.of()))).asTask());
                }
            }
        });

        if (childEntities.size()==1) result.put("entity", Iterables.getOnlyElement(childEntities));

        if (start == StartMode.SYNC) {
            startTasks.forEach(Task::getUnchecked);
        }

        return context.getOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
