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
package org.apache.brooklyn.core.workflow.steps.flow;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;

public class FailWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "[ ?${rethrow} \"rethrow\" ] [ \"message\" ${message...} ]";

    public static final ConfigKey<Boolean> RETHROW = ConfigKeys.newBooleanConfigKey("rethrow");
    public static final ConfigKey<String> MESSAGE = ConfigKeys.newStringConfigKey("message");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Boolean rethrow = context.getInput(RETHROW);
        String message = context.getInput(MESSAGE);
        Object value = context.getInput(VALUE);
        Throwable cause = context.getError();
        if (cause==null && Boolean.TRUE.equals(rethrow)) cause = new IllegalArgumentException("Fail specified with rethrow but no contextual error available");
        if (Boolean.FALSE.equals(rethrow)) cause = null;

        if (Strings.isBlank(message) && cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (Strings.isBlank(message) && cause instanceof Error) {
            throw (Error) cause;
        }
        if (value==null && cause!=null && !context.hasInput(VALUE)) {
            value = WorkflowFailException.getValueFromCausalChain(cause);
        }
        if (value!=null) {
            context.noteOtherMetadata("Value", value);
        }

        throw new WorkflowFailException(message, cause, value);
    }


    public static class WorkflowFailException extends RuntimeException {
        Object value;

        public WorkflowFailException() {
        }

        public WorkflowFailException(String message) {
            super(message);
        }

        public WorkflowFailException(String message, Throwable cause) {
            super(message, cause);
        }

        public WorkflowFailException(String message, Throwable cause, Object value) {
            super(message, cause);
            this.value = value;
        }

        public WorkflowFailException(Throwable cause) {
            super(cause);
        }

        public Object getValue() {
            return value;
        }

        public static Object getValueFromCausalChain(Throwable cause) {
            WorkflowFailException wfeInCausalChain = Exceptions.getFirstThrowableOfType(cause, WorkflowFailException.class);
            if (wfeInCausalChain==null) return null;
            return wfeInCausalChain.getValue();
        }
    }

    @Override protected Boolean isDefaultIdempotent() { return false; }
}
