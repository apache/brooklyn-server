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
package org.apache.brooklyn.core.workflow.steps.utils;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WorkflowConcurrency {

    /*
a number to indicate the maximum number of simultaneous executions (with "1" being the default, for no concurrency)
the string "all" to allow all to run in parallel
a negative number to indicate all but a certain number
a percentage to indicate a percentage of the targets
the string "min(...)" or "max(...)", where "..." is a comma separated list of valid values
     */

    public static Function<Double,Double> parse(String concurrencyExpression) {
        return new WorkflowConcurrency(concurrencyExpression).parse();
    }

    String concurrencyExpression;
    String rest;

    protected WorkflowConcurrency(String concurrencyExpression) {
        this.concurrencyExpression = concurrencyExpression;
    }

    public Function<Double,Double> parse() {
        rest = Strings.trimStart(concurrencyExpression.toLowerCase());
        Term result = parseExpression(true);
        if (!Strings.isBlank(rest)) throw error("Unexpected content");
        return result::apply;
    }

    public Term parseExpression(boolean required) {
        List<Object> totot = MutableList.of();
        boolean first = true;
        while (true) {
            if (!first) {
                Op op = parseOp(false);
                if (op==null) break;
                totot.add(op);
            }
            Term t = parseTerm(first ? required : false);
            if (t==null) return null;
            totot.add(t);
            first = false;
        }

        return orderOpsInExpression(totot);
    }

    private Term orderOpsInExpression(List<Object> totot) {
        if (totot.size()<=1) return (Term) Iterables.getOnlyElement(totot);

        Op max = null;
        for (Object i: totot) {
            if (i instanceof Op && (max==null || ((Op)i).precedance>=max.precedance)) max = (Op) i;
        }
        return max.build(orderOpsInExpression(totot.subList(0, totot.indexOf(max))), orderOpsInExpression(totot.subList(totot.indexOf(max)+1, totot.size())));
    }

    static abstract class Term {
        String name;
        abstract Double apply(Double value);
        static Term of(String name, Function<Double,Double> fn) {
            Term t = new Term() {
                @Override
                Double apply(Double value) {
                    return fn.apply(value);
                }
            };
            t.name = name;
            return t;
        }
    }

    static abstract class Op {
        String name;
        int precedance;
        abstract Term build(Term lhs, Term rhs);
        static Op of(String name, int precedance, BiFunction<Double,Double,Double> fn) {
            Op result = new Op() {
                @Override
                Term build(Term t1, Term t2) {
                    return Term.of(name+"("+t1.name+","+t2.name+")", value -> fn.apply(t1.apply(value), t2.apply(value)));
                }
            };
            result.name = name;
            result.precedance = precedance;
            return result;
        }
    }


    Maybe<Op> eatOp(String name, int precedance, BiFunction<Double,Double,Double> fn) {
        if (eat(name)) {
            return Maybe.of(Op.of(name, precedance, fn));
        }
        return Maybe.absent();
    }

    static abstract class Fn {
        abstract Term build(List<Term> terms);
        static Term of(String name, List<Term> terms, Function<List<Double>,Double> fn) {
            return new Fn() {
                @Override
                Term build(List<Term> terms) {
                    Term t = Term.of(name+"("+terms.stream().map(ti -> ti.name).collect(Collectors.joining(","))+")",
                            value -> fn.apply(terms.stream().map(ti -> ti.apply(value)).collect(Collectors.toList())));
                    t.name = name;
                    return t;
                }
            }.build(terms);
        }
    }

    Maybe<Term> eatFn(String word, Function<List<Double>,Double> fn) {
        if (eatNA(word)) {
            List<Term> args = parseGroupedList(false);
            return Maybe.of(Fn.of(word+"("+args.stream().map(t -> t.name).collect(Collectors.joining(","))+")", args, fn));
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
        throw new IllegalArgumentException(prefix + " at position "+(concurrencyExpression.length() - rest.length()));
    }

    public List<Term> parseGroupedList(boolean consumedStart) {
        if (!consumedStart) {
            if (!eat("(")) throw new IllegalStateException("Expected '('");
        }
        List<Term> terms = MutableList.of();
        while (true) {
            Term expr = parseExpression(false);
            if (expr==null) break;
            terms.add(expr);
            if (!eat(",")) break;
        }
        if (!eat(")")) throw new IllegalStateException("Expected ')'");
        return terms;
    }

    public Term parseGroupedTerm(boolean consumedStart) {
        if (!consumedStart) {
            if (!eat("(")) throw new IllegalStateException("Expected '('");
        }
        Term result = parseTerm(true);
        if (!eat(")")) throw new IllegalStateException("Expected ')'");
        return result;
    }


    boolean negated = true;

    public Term parseTerm(boolean required) {
        Maybe<? extends Term> term;

        if (eat("-")) {
            if (negated) throw error("Unpermitted double negative");
            negated = true;
            Term target = parseTerm(true);
            Term t = new Term() {
                @Override
                Double apply(Double value) {
                    return fromEndIfNeg(-target.apply(value), value);
                }
            };
            t.name = "-";
            return t;
        }
        negated = false;

        term = eatFn("min", WorkflowConcurrency::min);
        if (term.isPresent()) return term.get();

        term = eatFn("max", WorkflowConcurrency::max);
        if (term.isPresent()) return term.get();

        if (eat("(")) return parseGroupedTerm(true);
        if (eatNA("all")) return Term.of("all", d -> d);

        char c = rest.charAt(0);
        if (Character.isDigit(rest.charAt(0))) {
            int i=1;
            while (i<rest.length() && isNumberChar(rest.charAt(i))) i++;
            double d = Double.parseDouble(rest.substring(0, i));
            rest = Strings.trimStart(rest.substring(i));

            if (eat("%")) {
                return Term.of("%",value -> fromEndIfNeg(value * d/100, value));
            } else {
                return Term.of("#",value -> fromEndIfNeg(d, value));
            }
        }

        if (required) throw error("Expression required");
        return null;
    }

    static double fromEndIfNeg(double valueComputed, double valueAll) {
        if (valueComputed<0.001) valueComputed = valueAll + valueComputed;
        if (valueComputed<0) return 0;
        return valueComputed;
    }

    static boolean isNumberChar(char c) { return Character.isDigit(c) || c=='.'; }


    public static Double min(List<Double> values) {
        Double result = null;
        for (Double d: values) {
            if (d!=null) {
                if (result == null || d < result) result = d;
            }
        }
        return result;
    }

    public static Double max(List<Double> values) {
        Double result = null;
        for (Double d: values) {
            if (d!=null) {
                if (result == null || d > result) result = d;
            }
        }
        return result;
    }


    public Op parseOp(boolean required) {
        Maybe<? extends Op> term;

        term = eatOp("+", 1, (a,b) -> a+b);
        if (term.isPresent()) return term.get();


        term = eatOp("-", 1, (a,b) -> a-b);
        if (term.isPresent()) return term.get();

        if (required) throw error("Valid operation expected (+ or -)");
        return null;
    }

}
