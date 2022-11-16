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
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class RetryWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(RetryWorkflowStep.class);

    public static final String SHORTHAND = "[ ?${replay} \"replay\" ] [ \" from \" ${next} ] [ \" limit \" ${limit...} ] [ \" backoff \" ${backoff...} ] [ \" timeout \" ${timeout} ]";

    public static final ConfigKey<RetryReplayOption> REPLAY = ConfigKeys.newConfigKey(RetryReplayOption.class, "replay");
    public static final ConfigKey<List<RetryLimit>> LIMIT = ConfigKeys.newConfigKey(new TypeToken<List<RetryLimit>>() {}, "limit");
    public static final ConfigKey<RetryBackoff> BACKOFF = ConfigKeys.newConfigKey(RetryBackoff.class, "backoff");

    // if multiple retry steps declare the same key, their counts will be combined; used if the same error might be handled in different ways
    // note that the limits and backoff _instructions_ apply only at the step where they are defing, so they may need to be defined at each step
    public static final ConfigKey<String> KEY = ConfigKeys.newStringConfigKey("key");

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

        public Maybe<String> isReached(List<Instant> retries) {
            Instant now = Instant.now();
            List<Instant> filtered = retries.stream().filter(r -> duration == null || duration.isLongerThan(Duration.between(r, now))).collect(Collectors.toList());
            if (filtered.size() >= count) {
                if (filtered.isEmpty()) return Maybe.of("Max count 0 reached");
                return Maybe.of(
                        (filtered.size() < retries.size() ? retries.size()+" retries total, "+filtered.size() :
                                (retries.size()==1 ? "1 retry" : retries.size()+" retries")+" total" )+
                        " since "+(Duration.between(filtered.get(0), now))+" ago (limit "+this+")");
            }
            return Maybe.absent("Limit not reached");
        }

        @Override
        public String toString() {
            return count + (duration!=null ? " in "+duration : "");
        }
    }

    public static class RetryBackoff {
        List<Duration> initial;
        Double factor;
        Duration increase;
        Double jitter;
        Duration max;

        public void setInitial(List<Duration> initial) {
            this.initial = initial;
        }
        public void setInitial(String initial) {
            this.initial = MutableList.of(Duration.of(initial));
        }

        /** accepts eg: "0 0 100ms increasing 100% up to 1m" */
        public static RetryBackoff fromString(String s) {
            Maybe<Map<String, Object>> resultM = new ShorthandProcessor("${initial...} [ \" increasing \" ${factor} ] [ \" up to \" ${max}] [ \" jitter \" ${jitter} ]").process(s);
            if (resultM.isAbsent()) throw new IllegalArgumentException("Invalid shorthand expression for backoff: '"+s+"'", Maybe.Absent.getException(resultM));

            RetryBackoff result = new RetryBackoff();

            String initialS = TypeCoercions.coerce(resultM.get().get("initial"), String.class);
            if (Strings.isBlank(initialS)) {
                throw new IllegalArgumentException("initial duration required for backoff");
            }
            result.initial = QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(false).expectQuotesDelimited(true).failOnOpenQuote(true).build(initialS).remainderAsList().stream()
                    .map(Duration::of).collect(Collectors.toList());

            String factor = (String) resultM.get().get("factor");
            if (factor!=null) {
                factor = factor.trim();
                if (factor.endsWith("x")) {
                    result.factor = TypeCoercions.coerce(Strings.removeFromEnd(factor, "x"), Double.class);
                } else if (factor.endsWith("%")) {
                    result.factor = 1 + TypeCoercions.coerce(Strings.removeFromEnd(factor, "%"), Double.class) / 100;
                } else {
                    result.increase = Duration.of(factor);
                }
            }

            result.max = TypeCoercions.coerce(resultM.get().get("max"), Duration.class);

            String j = (String) resultM.get().get("jitter");
            if (j!=null) {
                j = j.trim();
                boolean percent = j.endsWith("%");
                if (percent) j = Strings.removeFromEnd(j, "%").trim();
                result.jitter = TypeCoercions.coerce(j, Double.class);
                if (percent) result.jitter /= 100;
            }

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

        if (Boolean.FALSE.equals(input.get(REPLAY.getName()))) input.remove(REPLAY.getName()); // remove replay=false
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

        String key = Strings.firstNonBlank(context.getInput(KEY), context.getWorkflowExectionContext().getWorkflowStepReference(Tasks.current()));
        List<Instant> retries = context.getWorkflowExectionContext().getRetryRecords().compute(key, (k, v) -> v != null ? v : MutableList.of());

        List<RetryLimit> limit = context.getInput(LIMIT);
        if (limit!=null) {
            limit.forEach(l -> {
                Maybe<String> reachedMessage = l.isReached(retries);
                if (reachedMessage.isPresent()) throw new RetriesExceeded(reachedMessage.get(), context.getError());
            });
        }

        Duration t = getMaximumRetryTimeout();
        if (t!=null) {
            Instant oldest = retries.stream().min((i1, i2) -> i1.compareTo(i2)).orElse(null);
            if (oldest != null) {
                Duration sinceFirst = Duration.between(oldest, Instant.now());
                if (sinceFirst.isLongerThan(t)) {
                    throw Exceptions.propagate(new TimeoutException("Workflow duration of "+sinceFirst+" exceeds timeout of "+t).initCause(context.getError()));
                }
            }
        }

        RetryBackoff backoff = context.getInput(BACKOFF);
        if (backoff!=null) {
            Duration delay;
            int exponent = 0;
            if (backoff.initial!=null && !backoff.initial.isEmpty()) {
                if (backoff.initial.size() > retries.size()) {
                    delay = backoff.initial.get(retries.size());
                } else {
                    delay = backoff.initial.get(backoff.initial.size()-1);
                    exponent = 1 + retries.size() - backoff.initial.size();
                }
            } else {
                // shouldn't be possible
                delay = Duration.ZERO;
            }
            if (backoff.factor!=null) while (exponent-- > 0) delay = delay.multiply(backoff.factor);
            if (backoff.increase !=null) delay = delay.add(backoff.increase.multiply(exponent));

            if (backoff.jitter!=null) delay = delay.multiply(1 + Math.random()*backoff.jitter);

            if (delay.isPositive()) {
                Duration ddelay = delay;
                try {
                    Tasks.withBlockingDetails("Waiting " + delay + " before retry #" + (retries.size() + 1), () -> {
                        log.debug("Waiting " + ddelay + " before retry #" + (retries.size() + 1));
                        Time.sleep(ddelay);
                        return null;
                    });
                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }
            }
        }

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
                        "Retry replay per step " + context.getWorkflowExectionContext().getWorkflowStepReference(Tasks.current()), replay == RetryReplayOption.FORCE);
            } else {
                context.nextReplay = context.getWorkflowExectionContext().makeInstructionsForReplayingFromStep(context.getWorkflowExectionContext().getIndexOfStepId(next).get().getLeft(),
                        "Retry replay from '"+next+"' per step " + context.getWorkflowExectionContext().getWorkflowStepReference(Tasks.current()), replay == RetryReplayOption.FORCE);
            }
            log.debug("Retrying with "+context.nextReplay);
        } else {
            if (next==null) {
                throw new IllegalStateException("Cannot retry with replay disabled and no specified next");
            } else {
                log.debug("Retrying from explicit next step '"+next+"'");
                // will go to next
            }
        }
        return context.getPreviousStepOutput();
    }

}
