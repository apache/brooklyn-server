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
package org.apache.brooklyn.core.workflow.steps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.ShorthandProcessor;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RetryWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(RetryWorkflowStep.class);

    public static final String SHORTHAND = "retry [${replay} \"replay\"] [ \" from \" ${next} ] [ \" limit \" ${limit...} ] [ \" backoff \" ${backoff...} ] [ \" timeout \" ${timeout} ]";

    public static final ConfigKey<RetryReplayOption> REPLAY = ConfigKeys.newConfigKey(RetryReplayOption.class, "replay");
    public static final ConfigKey<List<RetryLimit>> LIMIT = ConfigKeys.newConfigKey(new TypeToken<List<RetryLimit>>() {}, "limit");
    public static final ConfigKey<RetryBackoff> BACKOFF = ConfigKeys.newConfigKey(RetryBackoff.class, "backoff");

    public enum RetryReplayOption { TRUE, FALSE, FORCE }

    public static class RetryLimit {
        int count;
        Duration duration;

        public static RetryLimit fromInteger(Integer i) {
            return fromString(""+i);
        }

        public static RetryLimit fromString(String s) {
            RetryLimit result = new RetryLimit();
            String[] parts = s.trim().split(" +");
            if (parts.length==1 || (parts.length==3 && "in".equals(parts[1]))) {
                result.count = Integer.parseInt(parts[0]);
                if (parts.length==3) result.duration = Duration.of(parts[2]);
            } else {
                throw new IllegalStateException("Illegal expression for retry limit, should be '${count} in ${duration}': "+s);
            }
            return result;
        }

        public boolean isExceeded(List<Instant> retries) {
            Instant now = Instant.now();
            if (retries.stream().filter(r -> duration==null || duration.isLongerThan(Duration.between(r, now))).count() > count) return true;
            return false;
        }

        @Override
        public String toString() {
            return count + (duration!=null ? " in "+duration : "");
        }
    }

    public static class RetryBackoff {
        List<Duration> initial;
        Double factor;
        Double jitter;
        Duration max;

        /** accepts eg: "0 0 100ms *1.1 max 1m" */
        public static RetryBackoff fromString(String s) {
            Maybe<Map<String, Object>> resultM = new ShorthandProcessor("${initial...} [ \"*\" ${factor} ] [\" max \" ${max}]").process(s);
            if (resultM.isAbsent()) throw new IllegalArgumentException("Invalid shorthand expression for backoff: '"+s+"'", Maybe.Absent.getException(resultM));

            RetryBackoff result = new RetryBackoff();

            String initialS = TypeCoercions.coerce(resultM.get().get("initial"), String.class);
            if (Strings.isBlank(initialS)) {
                throw new IllegalArgumentException("initial duration required for backoff");
            }
            result.initial = QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(false).keepInternalQuotes(true).failOnOpenQuote(true).build(initialS).remainderAsList().stream()
                    .map(Duration::of).collect(Collectors.toList());

            result.factor = TypeCoercions.coerce(resultM.get().get("factor"), Double.class);
            result.max = TypeCoercions.coerce(resultM.get().get("max"), Duration.class);

            return result;
        }
    }

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);

        Object limit = input.remove(LIMIT.getName());
        if (limit!=null) {
            if (limit instanceof String) setInput(LIMIT, MutableList.of(RetryLimit.fromString((String) limit)));
            else if (limit instanceof List) setInput(LIMIT, (List) limit);
            else throw new IllegalStateException("Invalid value for limit: " + limit);
        }

        String next = TypeCoercions.coerce(input.remove("next"), String.class);
        if (Strings.isNonBlank(next)) this.next = next;

        Duration timeout = TypeCoercions.coerce(input.remove("timeout"), Duration.class);
        if (timeout!=null) this.timeout = timeout;
    }

    @Override
    public void validateStep() {
        super.validateStep();
        TypeCoercions.coerce(input.get(REPLAY.getName()), REPLAY.getTypeToken());
        TypeCoercions.coerce(input.get(LIMIT.getName()), LIMIT.getTypeToken());
        TypeCoercions.coerce(input.get(BACKOFF.getName()), BACKOFF.getTypeToken());
    }

    /** The meaning of stock step 'timeout' field is overridden here, because as retry is quick it has no timeout.
     * See {@link #getMaximumRetryTimeout()}. */
    @JsonIgnore @Override
    public Duration getTimeout() {
        return null;
    }

    @JsonIgnore
    public Duration getMaximumRetryTimeout() {
        return super.getTimeout();
    }

    public static class RetriesExceeded extends RuntimeException {
        public RetriesExceeded(String message) { super(message); }
        public RetriesExceeded(String message, Throwable cause) { super(message, cause); }
        public RetriesExceeded(Throwable cause) { super(cause); }
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {

        String key = context.getWorkflowExectionContext().getWorkflowStepReference(Tasks.current());
        List<Instant> retries = context.getWorkflowExectionContext().getRetryRecords().compute(key, (k, v) -> v != null ? v : MutableList.of());

        List<RetryLimit> limit = context.getInput(LIMIT);
        if (limit!=null) {
            limit.forEach(l -> {
                if (l.isExceeded(retries)) throw new RetriesExceeded("More than "+l, context.getError());
            });
        }
        // TODO check overall timeout
        // TODO delay for backoff

        retries.add(Instant.now());
        context.getWorkflowExectionContext().getRetryRecords().put(key, retries);

        RetryReplayOption replay = context.getInput(REPLAY);
        if (replay==null) {
            // default is to replay if next not specified
            replay = next==null ? RetryReplayOption.TRUE : RetryReplayOption.FALSE;
        }

        if (replay!=RetryReplayOption.FALSE) {
            if (next==null) {
                context.nextReplay = context.getWorkflowExectionContext().makeInstructionsForReplayingLast(
                        "workflow requested retry to '"+next+"' from step " + context.getWorkflowExectionContext().getWorkflowStepReference(Tasks.current()), replay == RetryReplayOption.FORCE);
            } else {
                context.nextReplay = context.getWorkflowExectionContext().makeInstructionsForReplayingFromStep(context.getWorkflowExectionContext().getIndexOfStepId(next).get().getLeft(),
                        "workflow requested retry of last from step " + context.getWorkflowExectionContext().getWorkflowStepReference(Tasks.current()), replay == RetryReplayOption.FORCE);
            }
            log.debug("Retrying with "+context.nextReplay);
        } else {
            if (next==null) {
                throw new IllegalStateException("Cannot retry with replay disabled and no specified next");
            } else {
                log.debug("Retrying to explicit next step '"+next+"'");
                // will go to next
            }
        }
        return context.getPreviousStepOutput();
    }

}
