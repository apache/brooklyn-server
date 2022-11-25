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
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class DeployApplicationWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DeployApplicationWorkflowStep.class);

    public static final String SHORTHAND = "[ ${type} ]";

    public static final ConfigKey<Object> BLUEPRINT = ConfigKeys.newConfigKey(Object.class, "blueprint");
    public static final ConfigKey<String> TYPE = ConfigKeys.newStringConfigKey("type");
    public static final ConfigKey<String> FORMAT = ConfigKeys.newStringConfigKey("format");

    // sync is not completely idempotent (in the call to start it) but very useful for testing;
    // option not included in documentation, but used for tests
    public static final ConfigKey<StartMode> START = ConfigKeys.newConfigKey(StartMode.class, "start", "Default 'async'");

    enum StartMode {
        SYNC, ASYNC, DISABLED
    }

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        boolean hasBlueprint = getInput().containsKey(BLUEPRINT.getName());
        boolean hasType = getInput().containsKey(TYPE.getName());
        if (!hasBlueprint && !hasType) throw new IllegalArgumentException("A '"+BLUEPRINT.getName()+"' must be defined or a type supplied in shorthand");
        if (hasBlueprint && hasType) throw new IllegalArgumentException("Cannot provide both a '"+BLUEPRINT.getName()+"' and a type in shorthand");
    }

    @Override
    protected String getStepState(WorkflowStepInstanceExecutionContext context) {
        return (String) super.getStepState(context);
    }
    void setStepState(WorkflowStepInstanceExecutionContext context, String entityId) {
        context.setStepState(entityId, true);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Object blueprint = context.getInput(BLUEPRINT);
        if (blueprint == null)
            blueprint = "services: [ { type: " + StringEscapes.JavaStringEscapes.wrapJavaString(context.getInput(TYPE)) + " } ]";

        String id = getStepState(context);
        Application app = null;

        if (Strings.isNonBlank(id)) {
            app = (Application) context.getManagementContext().getEntityManager().getEntity(id);
            if (app!=null) {
                context.setOutput(MutableMap.of("app", app));
            }
        } else {
            id = Identifiers.makeRandomLowercaseId(10);
            setStepState(context, id);
        }

        if (app==null) {

            EntitySpec spec;
            try {
                spec = EntityManagementUtils.createEntitySpecForApplication(context.getManagementContext(), context.getInput(FORMAT),
                        blueprint instanceof String ? (String) blueprint :
                                BeanWithTypeUtils.newYamlMapper(context.getManagementContext(), false, null, false).writeValueAsString(blueprint));
            } catch (JsonProcessingException e) {
                throw Exceptions.propagate(e);
            }

            app = EntityManagementUtils.createUnstarted(context.getManagementContext(), spec, Optional.of(id));

            context.setOutput(MutableMap.of("app", app));

            StartMode start = context.getInput(START);
            if (start==null) start = StartMode.ASYNC;

            if (start == StartMode.DISABLED) {
                // nothing
            } else {
                EntityManagementUtils.CreationResult<Application, Void> startTask = EntityManagementUtils.start(app);
                if (start==StartMode.SYNC) startTask.task().getUnchecked();
            }
        }

        return context.getOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
