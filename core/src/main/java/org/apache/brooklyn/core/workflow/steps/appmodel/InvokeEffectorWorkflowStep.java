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

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowReplayUtils;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepResolution;
import org.apache.brooklyn.core.workflow.utils.ExpressionParser;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.CharactersCollectingParseMode;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.CommonParseMode;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNode;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNodeOrValue;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseValue;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.brooklyn.core.workflow.utils.WorkflowSettingItemsUtils.makeMutableCopy;

public class InvokeEffectorWorkflowStep extends WorkflowStepDefinition implements WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow {

    private static final Logger LOG = LoggerFactory.getLogger(InvokeEffectorWorkflowStep.class);

    public static final String SHORTHAND = "${effector} [ \" on \" ${entity} ] [ \" with \" ${args...} ]";

    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");
    public static final ConfigKey<String> EFFECTOR = ConfigKeys.newStringConfigKey("effector");
    public static final ConfigKey<Map<String,Object>> ARGS = new MapConfigKey.Builder<>(Object.class, "args").build();

    @Override
    public void populateFromShorthand(String value) {
        populateFromShorthandTemplate(SHORTHAND, value, true, true);
    }
    @Override
    protected Map<String, Object> getFromShorthandTemplate(String template, String value, boolean finalMatchRaw, boolean failOnError) {
        Map<String, Object> result = super.getFromShorthandTemplate(template, value, finalMatchRaw, failOnError);
        if (result!=null && result.containsKey(ARGS.getName())) {
            result = makeMutableCopy(result).get();
            Object args = result.remove(ARGS.getName());
            Map<String,Object> argsM = MutableMap.of();
            if (args instanceof Map) result.put(ARGS.getName(), args);
            else if (args instanceof String) {
                result.put(ARGS.getName(), parseKeyEqualsValueExpressionStringList((String) args));
            } else {
                throw new IllegalArgumentException("args provided to invoke-effector must be a map or a comma-separated sequence of key = value pairs");
            }
        }
        return result;
    }

    public static Map<String,String> parseKeyEqualsValueExpressionStringList(String args) {
        ExpressionParserImpl ep = ExpressionParser.newDefaultAllowingUnquotedLiteralValues()
                .includeGroupingBracketsAtUsualPlaces()
                .includeAllowedTopLevelTransition(ExpressionParser.WHITESPACE)
                .includeAllowedTopLevelTransition(new CharactersCollectingParseMode("equals", '='))
                .includeAllowedTopLevelTransition(new CharactersCollectingParseMode("colon", ':'))
                .includeAllowedTopLevelTransition(new CharactersCollectingParseMode("comma", ','));
        ParseNode pr = ep.parse(args).get();
        Map<String,String> result = MutableMap.of();

        List<String> key = MutableList.of();
        List<String> value = MutableList.of();
        List<String> ws = MutableList.of();

        Runnable save = () -> {
            String old = result.put(Strings.join(key, ""), Strings.join(value, ""));
            if (old!=null) throw new IllegalArgumentException("Duplicate argument: "+Strings.join(key, ""));
        };
        List<String> current = key;
        for (ParseNodeOrValue c: pr.getContents()) {
            Maybe<String> add = null;
            Boolean advance = null;

            if (c.isParseNodeMode(ExpressionParser.WHITESPACE) ) {
                ws.add(c.getSource());
                continue;
            }

            if (c.isParseNodeMode(ExpressionParser.COMMON_BRACKETS) || c.isParseNodeMode(ExpressionParser.INTERPOLATED)) {
                if (current==key) throw new IllegalArgumentException("Cannot use "+c.getParseNodeMode()+" in argument name");
                add = Maybe.of(c.getSource());
            } else if (ExpressionParser.isQuotedExpressionNode(c) || c.isParseNodeMode(ParseValue.MODE) || c.isParseNodeMode(ExpressionParser.BACKSLASH_ESCAPE)) {
                add = Maybe.of(ExpressionParser.getUnquoted(c));
            } else if (c.isParseNodeMode("equals", "colon")) {
                if (current==value) throw new IllegalArgumentException("Cannot use "+c.getParseNodeMode()+" after argument value");
                add = Maybe.absent();
                advance = true;
            } else if (c.isParseNodeMode("comma")) {
                if (current==key) throw new IllegalArgumentException("Cannot use "+c.getParseNodeMode()+" in argument key");
                add = Maybe.absent();
                advance = true;
            }

            if (add==null) throw new IllegalArgumentException("Unexpected expression for argument: "+c.getSource());
            if (add.isPresent()) {
                if (!current.isEmpty()) {
                    if (current==key) throw new IllegalArgumentException("Multiple words not permitted for argument name");
                    current.addAll(ws);
                }
                current.add(add.get());
            }
            ws.clear();
            if (Boolean.TRUE.equals(advance)) {
                if (current==key) {
                    if (current.isEmpty()) throw new IllegalArgumentException("Missing argument name");
                    current = value;
                } else {
                    if (current.isEmpty()) throw new IllegalArgumentException("Missing argument value");
                    current = key;
                    save.run();
                    key.clear();
                    value.clear();
                }
            }
        }

        if (current==key) {
            if (!current.isEmpty()) throw new IllegalArgumentException("Missing argument value");
        } else if (current==value) {
            if (current.isEmpty()) throw new IllegalArgumentException("Missing argument value");
            save.run();
        }
        return result;
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        if (!getInput().containsKey(EFFECTOR.getName())) throw new IllegalArgumentException("Missing required input: "+EFFECTOR.getName());
    }

    @Override @JsonIgnore
    public SubWorkflowsForReplay getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, boolean forced, boolean peekingOnly, boolean allowInternallyEvenIfDisabled) {
        return WorkflowReplayUtils.getSubWorkflowsForReplay(context, forced, peekingOnly, allowInternallyEvenIfDisabled, sw -> {
            // comes here if subworkflows were abnormal
            StepState state = getStepState(context);
            if (!state.submitted) sw.isResumableOnlyAtParent = true;  // no subworkflows, fine to resume
            else if (state.nonWorkflowEffector) sw.isResumableOnlyAtParent = false;  // subworkflows definitely not resumable, can't resume
            else sw.isResumableOnlyAtParent = true; // odd case, subworkflows perhaps cancelled and superseded but then not submitted or something odd like that
        });
    }

    @Override
    public Object doTaskBodyWithSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, @Nonnull List<WorkflowExecutionContext> subworkflows, ReplayContinuationInstructions instructions) {
        return WorkflowReplayUtils.replayResumingInSubWorkflow("workflow effector", context, Iterables.getOnlyElement(subworkflows), instructions,
                (w, e)-> {
                    LOG.debug("Sub workflow "+w+" is not replayable; running anew ("+ Exceptions.collapseText(e)+")");
                    return doTaskBody(context);
                }, true);
    }

    static class StepState {
        boolean submitted;
        boolean nonWorkflowEffector;
    }

    @Override
    protected StepState getStepState(WorkflowStepInstanceExecutionContext context) {
        StepState result = (StepState) super.getStepState(context);
        if (result==null) result = new StepState();
        return result;
    }
    void setStepState(WorkflowStepInstanceExecutionContext context, StepState state) {
        context.setStepState(state, true);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Entity entity = WorkflowStepResolution.findEntity(context, ObjectUtils.firstNonNull(context.getInput(ENTITY), context.getEntity()) ).get();

        Effector<Object> effector = (Effector) entity.getEntityType().getEffectorByName(context.getInput(EFFECTOR)).get();
        TaskAdaptable<Object> invocation = Effectors.invocationPossiblySubWorkflow(entity, effector, context.getInput(ARGS), context.getWorkflowExectionContext(), workflowTag -> {
            WorkflowReplayUtils.setNewSubWorkflows(context, MutableList.of(workflowTag), workflowTag.getWorkflowId());
            // unlike nested case, no need to persist as single child workflow will persist themselves imminently, and if not no great shakes to recompute
        });

        StepState state = getStepState(context);
        state.nonWorkflowEffector = context.getSubWorkflows().isEmpty();
        state.submitted = true;
        setStepState(context, state);

        return DynamicTasks.queue(invocation).asTask().getUnchecked();
    }

    @Override protected Boolean isDefaultIdempotent() { return null; }
}
