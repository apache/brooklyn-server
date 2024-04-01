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
package org.apache.brooklyn.core.workflow.utils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.store.WorkflowRetentionAndExpiration;
import org.apache.brooklyn.core.workflow.store.WorkflowRetentionAndExpiration.WorkflowRetentionSettings;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.tuple.Pair;

public class WorkflowRetentionParser {

    /*
tracks a number and/or duration to indicate how many and how long workflows should be retained in memory after completion;
can take:

* a number, to indicate how many instances of a workflow should be kept
* a duration, to indicate for how long workflows should be kept
* `forever`, to never expire
* `context`, to use the previous retention values (often used together with `max`)
* `parent`, to use the value of any parent workflow or else the system default
* `system`, to use the system default (from brooklyn.properties)
* `min(<value>, <value>, ...)` or `max(<value>, <value>, ...)` of any of the expressions on this line or above (but not `disabled` or `hash`)
* `disabled`, to prevent persistence of a workflow, causing less work for the system where workflows don't need to be stored; such workflows will not be replayable by an operator or recoverable on failover

the semantics of `min` and `max` are
* `min` means completed workflow instances must only be retained if they meet all the constraints implied by the `<value>` arguments, i.e. `min(2, 3, 1h, 2h)` means only the most recent two instances need to be kept and only if it has been less than an hour since they completed
* `max` means completed workflow instances must be retained if they meet any of the constraints implied by the `<value>` arguments, i.e. `max(2, 3, 1h, 2h)` means to keep the 3 most recent instances irrespective of when they run, and to keep all instances for up to two hours

also allows a `hash <value>` to be set at the start or the end

also allows `hard` at start or end, or `soft [limit]` at end
     */

    public static WorkflowRetentionSettings parse(String retentionExpression, @Nullable WorkflowExecutionContext context) {

        WorkflowRetentionSettings result = new WorkflowRetentionSettings();
        if (Strings.isBlank(retentionExpression)) return result;
        retentionExpression = retentionExpression.trim().toLowerCase();

        do {
            if (retentionExpression.startsWith("hash ")) {
                if (result.hash != null)
                    throw new IllegalArgumentException("Cannot set multiple 'hash' in retention expression");
                retentionExpression = Strings.removeFromStart(retentionExpression, "hash").trim();
                result.hash = Strings.getFirstWord(retentionExpression);
                retentionExpression = retentionExpression.substring(result.hash.length()).trim();
                continue;
            }
            if (retentionExpression.startsWith("hard ")) {
                if (result.softExpiry != null)
                    throw new IllegalArgumentException("Cannot set multiple 'hard' or 'soft' in retention expression");
                retentionExpression = Strings.removeFromStart(retentionExpression, "hard").trim();
                result.softExpiry = "0";
                continue;
            }

            List<Pair<String,Integer>> specialTerms = MutableList.of();
            for (String term: MutableList.of("hash", "soft", "hard"))
                specialTerms.add(Pair.of(term, retentionExpression.indexOf(" "+term+" ")));
            Collections.sort(specialTerms, (x,y) -> -Integer.compare(x.getRight(), y.getRight()));
            Pair<String, Integer> last = specialTerms.iterator().next();
            if (last.getRight()>=0) {
                if ("hash".equals(last.getLeft())) {
                    if (result.hash != null)
                        throw new IllegalArgumentException("Cannot set multiple 'hash' in retention expression");
                    result.hash = Strings.removeFromStart(retentionExpression.substring(last.getRight()).trim(), last.getLeft()).trim();
                    retentionExpression = retentionExpression.substring(0, last.getRight()).trim();
                    continue;
                }
                if ("hard".equals(last.getLeft())) {
                    if (result.softExpiry != null)
                        throw new IllegalArgumentException("Cannot set multiple 'hard' or 'soft' in retention expression");
                    result.softExpiry = "0";
                    String hardTrailing = Strings.removeFromStart(retentionExpression.substring(last.getRight()).trim(), last.getLeft()).trim();
                    if (Strings.isNonBlank(hardTrailing)) {
                        if (last.getRight() == 0) retentionExpression = hardTrailing;
                        else throw new IllegalArgumentException("Cannot have retention definition both before and after 'hard' keyword");
                    } else {
                        retentionExpression = retentionExpression.substring(0, last.getRight()).trim();
                    }
                    continue;
                }
                if ("soft".equals(last.getLeft())) {
                    if (result.softExpiry != null)
                        throw new IllegalArgumentException("Cannot set multiple 'hard' or 'soft' in retention expression");
                    String softTrailing = Strings.removeFromStart(retentionExpression.substring(last.getRight()).trim(), last.getLeft()).trim();
                    if (Strings.isNonBlank(softTrailing)) {
                        result.softExpiry = softTrailing;
                        new WorkflowRetentionParser(result.softExpiry).soft().parse();
                        retentionExpression = retentionExpression.substring(0, last.getRight()).trim();
                    } else {
                        throw new IllegalArgumentException("Specification for 'soft' retetntion must provide retention expression after the keyword");
                    }
                    continue;
                }
            }
            break;
        } while (true);

        if (retentionExpression.equals("disabled")) {
            result.disabled = true;

        } else {

            if (Strings.isNonBlank(result.hash) && context!=null) {
                result.hash = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, result.hash, String.class);
            }
            if (Strings.isBlank(retentionExpression)) return result;

            result.expiry = retentionExpression;
            // catch parse errors now; fn won't be accessible without a workflow execution context however
            new WorkflowRetentionParser(result.expiry).parse();
        }

        return result;
    }

    public interface WorkflowRetentionFilter extends Function<Collection<WorkflowExecutionContext>,Collection<WorkflowExecutionContext>> {
        default WorkflowRetentionFilter init(WorkflowExecutionContext context) { return this; }
    }

    static class KeepAll implements WorkflowRetentionFilter {
        @Override
        public Collection<WorkflowExecutionContext> apply(Collection<WorkflowExecutionContext> workflowExecutionContexts) {
            return workflowExecutionContexts;
        }
        @Override
        public String toString() {
            return "forever";
        }
    }

    static class KeepMax implements WorkflowRetentionFilter {
        private final List<WorkflowRetentionFilter> values;
        KeepMax(List<WorkflowRetentionFilter> values) { this.values = values; }
        @Override
        public Collection<WorkflowExecutionContext> apply(Collection<WorkflowExecutionContext> workflowExecutionContexts) {
            return values.stream().map(v -> v.apply(workflowExecutionContexts)).reduce(MutableSet.of(), (t1, t2) -> { t1.addAll(t2); return t1; });
        }
        @Override
        public String toString() {
            return "max("+values.stream().map(Object::toString).collect(Collectors.joining(","))+")";
        }
        @Override
        public WorkflowRetentionFilter init(WorkflowExecutionContext context) {
            values.forEach(v -> v.init(context));
            return WorkflowRetentionFilter.super.init(context);
        }
    }

    static class KeepMin implements WorkflowRetentionFilter {
        private final List<WorkflowRetentionFilter> values;
        KeepMin(List<WorkflowRetentionFilter> values) { this.values = values; }
        @Override
        public Collection<WorkflowExecutionContext> apply(Collection<WorkflowExecutionContext> workflowExecutionContexts) {
            List<Collection<WorkflowExecutionContext>> workflowsToKeep = values.stream().map(v -> v.apply(workflowExecutionContexts)).collect(Collectors.toList());
            Iterator<Collection<WorkflowExecutionContext>> wi = workflowsToKeep.iterator();
            if (!wi.hasNext()) return workflowExecutionContexts;
            Set<WorkflowExecutionContext> intersection = MutableSet.copyOf(wi.next());
            while (wi.hasNext()) intersection.retainAll(wi.next());
            return intersection;
        }
        @Override
        public String toString() {
            return "min("+values.stream().map(Object::toString).collect(Collectors.joining(","))+")";
        }
        @Override
        public WorkflowRetentionFilter init(WorkflowExecutionContext context) {
            values.forEach(v -> v.init(context));
            return WorkflowRetentionFilter.super.init(context);
        }
    }

    static class KeepCount implements WorkflowRetentionFilter {
        private final int count;
        KeepCount(int count) { this.count = count; }
        @Override
        public Collection<WorkflowExecutionContext> apply(Collection<WorkflowExecutionContext> workflowExecutionContexts) {
            if (workflowExecutionContexts.size()>count) {
                return workflowExecutionContexts.stream().sorted(Comparator.comparing(WorkflowExecutionContext::getLastStatusChangeTime).reversed()).limit(count).collect(Collectors.toList());
            } else {
                return workflowExecutionContexts;
            }
        }
        @Override
        public String toString() {
            return ""+count;
        }
    }

    static class KeepDuration implements WorkflowRetentionFilter {
        private final Duration duration;
        KeepDuration(Duration duration) { this.duration = duration; }
        @Override
        public Collection<WorkflowExecutionContext> apply(Collection<WorkflowExecutionContext> workflowExecutionContexts) {
            Instant expiry = Instant.now().minus(duration.toMilliseconds()+1, ChronoUnit.MILLIS);
            return workflowExecutionContexts.stream().filter(c -> c.getLastStatusChangeTime().isAfter(expiry)).collect(Collectors.toList());
        }
        @Override
        public String toString() {
            return ""+duration;
        }
    }

    static abstract class KeepDelegate implements WorkflowRetentionFilter {
        WorkflowRetentionFilter delegate;
        final boolean soft;
        KeepDelegate(boolean soft) { this.soft = soft; }
        @Override
        public Collection<WorkflowExecutionContext> apply(Collection<WorkflowExecutionContext> workflowExecutionContexts) {
            if (delegate==null) throw new IllegalStateException("Not initialized");
            return delegate.apply(workflowExecutionContexts);
        }
        @Override
        public final WorkflowRetentionFilter init(WorkflowExecutionContext workflow) {
            if (delegate==null) delegate = findDelegate(workflow);
            return this;
        }
        protected abstract WorkflowRetentionFilter findDelegate(WorkflowExecutionContext workflow);
    }
    static class KeepSystem extends KeepDelegate {
        KeepSystem(boolean soft) { super(soft); }
        @Override
        public WorkflowRetentionFilter findDelegate(WorkflowExecutionContext workflow) {
            if (workflow==null) throw new IllegalStateException("Retention 'system' cannot be used here");
            return new WorkflowRetentionParser(workflow.getManagementContext().getConfig().getConfig(
                    soft ? WorkflowRetentionAndExpiration.WORKFLOW_RETENTION_DEFAULT_SOFT : WorkflowRetentionAndExpiration.WORKFLOW_RETENTION_DEFAULT))
                    .soft(soft).parse().init(null);
        }
        @Override
        public String toString() {
            return "system";
        }
    }
    public static WorkflowRetentionFilter newDefaultFilter(boolean soft) {
        return new KeepParent(soft);
    }
    static class KeepParent extends KeepDelegate {
        KeepParent(boolean soft) { super(soft); }
        @Override
        public WorkflowRetentionFilter findDelegate(WorkflowExecutionContext workflow) {
            if (workflow == null) throw new IllegalStateException("Retention 'parent' cannot be used here");
            else if (workflow.getParent()!=null) {
                return soft ? workflow.getParent().getRetentionSettings().getSoftExpiryFn(workflow.getParent()) : workflow.getParent().getRetentionSettings().getExpiryFn(workflow.getParent());
            } else {
                return new KeepSystem(soft).init(workflow);
            }
        }
        @Override
        public String toString() {
            return "parent";
        }
    }
    static class KeepContext extends KeepDelegate {
        KeepContext(boolean soft) { super(soft); }
        @Override
        public WorkflowRetentionFilter findDelegate(WorkflowExecutionContext workflow) {
            if (workflow == null) throw new IllegalStateException("Retention 'context' cannot be used here");

            // expands to string to something that doesn't reference context so that this does not infinitely recurse
            return soft ? workflow.getRetentionSettings().getSoftExpiryFn(workflow) : workflow.getRetentionSettings().getExpiryFn(workflow);
        }
        @Override
        public String toString() {
            return delegate==null ? "context" : delegate.toString();
        }
    }

    String fullExpression;
    String rest;
    boolean soft = false;

    public WorkflowRetentionParser(String fullExpression) {
        this.fullExpression = fullExpression;
    }

    public WorkflowRetentionParser soft() { return soft(true); }
    public WorkflowRetentionParser soft(boolean soft) { this.soft = soft; return this; }

    public WorkflowRetentionFilter parse() {
        if (Strings.isBlank(fullExpression)) return newDefaultFilter(soft);

        rest = Strings.trimStart(fullExpression.toLowerCase());
        WorkflowRetentionFilter result = parseTerm();
        if (!Strings.isBlank(rest)) return newDefaultFilter(soft);
        return result;
    }

    Maybe<WorkflowRetentionFilter> eatFn(String word, Function<List<WorkflowRetentionFilter>, WorkflowRetentionFilter> fn) {
        if (eatNA(word)) {
            List<WorkflowRetentionFilter> args = parseGroupedList(false);
            return Maybe.of(fn.apply(args));
        }
        return Maybe.absent();
    }

    public <T> T notNull(T value, String message) {
        if (value!=null) return value;
        throw error(message);
    }

    public boolean eat(String word) {
        return eat(word, false);
    }
    public boolean eat(String word, boolean requireNextNonAlpha) {
        if (rest.startsWith(word)) {
            rest = rest.substring(word.length());
            if (requireNextNonAlpha && !rest.isEmpty() && Character.isJavaIdentifierPart(rest.charAt(0))) {
                rest = word + rest;
                return false;
            }
            rest = Strings.trimStart(rest);
            return true;
        }
        return false;
    }

    public boolean eatNA(String word) {
        return eat(word, true);
    }

    protected RuntimeException error(String prefix) {
        throw new IllegalArgumentException(prefix + " at position "+(fullExpression.length() - rest.length()));
    }

    public List<WorkflowRetentionFilter> parseGroupedList(boolean consumedStart) {
        if (!consumedStart) {
            if (!eat("(")) throw new IllegalStateException("Expected '('");
        }
        List<WorkflowRetentionFilter> terms = MutableList.of();
        while (true) {
            WorkflowRetentionFilter expr = parseTerm();
            if (expr==null) break;
            terms.add(expr);
            if (!eat(",")) break;
        }
        if (!eat(")")) throw new IllegalStateException("Expected ')'");
        return terms;
    }

    public WorkflowRetentionFilter parseTerm() {
        Maybe<? extends WorkflowRetentionFilter> term;

        term = eatFn("min", KeepMin::new);
        if (term.isPresent()) return term.get();

        term = eatFn("max", KeepMax::new);
        if (term.isPresent()) return term.get();

        if (eatNA("all") || eatNA("forever")) return new KeepAll();
        if (eatNA("system")) return new KeepSystem(soft);
        if (eatNA("parent")) return new KeepParent(soft);
        if (eatNA("context")) return new KeepContext(soft);

        int i = maxPositive(rest.indexOf(","), rest.indexOf(")"));
        if (i==-1) i = rest.length();
        String nextWord = rest.substring(0, i).trim();
        rest = rest.substring(i);

        if (nextWord.matches("[0-9]+")) {
            return new KeepCount(Integer.parseInt(nextWord));
        } else {
            try {
                return new KeepDuration(Duration.of(nextWord));
            } catch (Exception e) {
                throw error("Expected a valid retention term, instead had '"+nextWord+"'");
            }
        }
    }

    private int maxPositive(int i1, int i2) {
        if (i1<0) return i2;
        if (i2<0) return i1;
        return Math.min(i1, i2);
    }
}
