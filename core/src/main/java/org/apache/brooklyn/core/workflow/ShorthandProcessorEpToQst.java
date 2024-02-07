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
package org.apache.brooklyn.core.workflow;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.brooklyn.core.workflow.utils.ExpressionParser;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNode;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNodeOrValue;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the latest version of the SP which uses the EP for input, and the QST for templates,
 * to keep backwards compatibility but allow better handling for internal double quotes and interpolated expressions with spaces.
 * It falls back to QST when there are errors to maximize compatibility.
 *
 * ===
 *
 * Accepts a shorthand template, and converts it to a map of values,
 * e.g. given template "[ ?${type_set} ${sensor.type} ] ${sensor.name} \"=\" ${value}"
 * and input "integer foo=3", this will return
 * { sensor: { type: integer, name: foo }, value: 3, type_set: true }.
 *
 * Expects space-separated TOKEN where TOKEN is either:
 *
 * ${VAR} - to set VAR, which should be of the regex [A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*, with dot separation used to set nested maps;
 *   will match a quoted string if supplied, else up to the next literal if the next token is a literal, else the next work.
 * ${VAR...} - as above, but will collect multiple args if needed (if the next token is a literal matched further on, or if at end of word)
 * "LITERAL" - to expect a literal expression. this must include the quotation marks and should include spaces if spaces are required.
 * [ TOKEN ] - to indicate TOKEN is optional, where TOKEN is one of the above sections. parsing is attempted first with it, then without it.
 * [ ?${VAR} TOKEN ] - as `[ TOKEN ]` but VAR is set true or false depending whether this optional section was matched.
 *
 * Would be nice to support A | B (exclusive or) for A or B but not both (where A might contain a literal for disambiguation),
 * and ( X ) for X required but grouped (for use with | (exclusive or) where one option is required).
 * Would also be nice to support any order, which could be ( A & B ) to allow A B or B A.
 *
 * But for now we've made do without it, with some compromises:
 * * keywords must follow the order indicated
 * * exclusive alternatives are disallowed by code subsequently or checked separately (eg Transform)
 */
public class ShorthandProcessorEpToQst {

    private static final Logger log = LoggerFactory.getLogger(ShorthandProcessorEpToQst.class);

    static final boolean TRY_HARDER_FOR_QST_COMPATIBILITY = true;

    private final String template;
    boolean finalMatchRaw = false;
    boolean failOnMismatch = true;

    public ShorthandProcessorEpToQst(String template) {
        this.template = template;
    }

    public Maybe<Map<String,Object>> process(String input) {
        return new ShorthandProcessorQstAttempt(this, input).call();
    }

    /** whether the last match should preserve quotes and spaces; default false */
    public ShorthandProcessorEpToQst withFinalMatchRaw(boolean finalMatchRaw) {
        this.finalMatchRaw = finalMatchRaw;
        return this;
    }

    /** whether to fail on mismatched quotes in the input, default true */
    public ShorthandProcessorEpToQst withFailOnMismatch(boolean failOnMismatch) {
        this.failOnMismatch = failOnMismatch;
        return this;
    }

    static class ShorthandProcessorQstAttempt {
        private final List<String> templateTokens;
        private final String inputOriginal;
        private final QuotedStringTokenizer qst0;
        private final String template;
        private final ShorthandProcessorEpToQst options;
        int optionalDepth = 0;
        int optionalSkippingInput = 0;
        private String inputRemaining;
        Map<String, Object> result;
        Consumer<String> valueUpdater;

        ShorthandProcessorQstAttempt(ShorthandProcessorEpToQst proc, String input) {
            this.template = proc.template;
            this.options = proc;
            this.qst0 = qst0(template);
            this.templateTokens = qst0.remainderAsList();  // QST works fine for the template
            this.inputOriginal = input;
        }

        private QuotedStringTokenizer qst0(String x) {
            return QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(false).expectQuotesDelimited(true).failOnOpenQuote(options.failOnMismatch).build(x);
        }

        public synchronized Maybe<Map<String,Object>> call() {
            if (result == null) {
                result = MutableMap.of();
                inputRemaining = inputOriginal;
            } else {
                throw new IllegalStateException("Only allowed to use once");
            }
            Maybe<Object> error = doCall();
            if (error.isAbsent()) return Maybe.Absent.castAbsent(error);
            inputRemaining = Strings.trimStart(inputRemaining);
            if (Strings.isNonBlank(inputRemaining)) {
                if (valueUpdater!=null) {
                    //QuotedStringTokenizer qstInput = qst(inputRemaining);
                    valueUpdater.accept(getRemainderPossiblyRaw(inputRemaining));
                } else {
                    // shouldn't come here
                    return Maybe.absent("Input has trailing characters after template is matched: '" + inputRemaining + "'");
                }
            }
            return Maybe.of(result);
        }

        protected Maybe<Object> doCall() {
            boolean isEndOfOptional = false;
            outer: while (true) {
                if (isEndOfOptional) {
                    if (optionalDepth <= 0) {
                        throw new IllegalStateException("Unexpected optional block closure");
                    }
                    optionalDepth--;
                    if (optionalSkippingInput>0) {
                        // we were in a block where we skipped something optional because it couldn't be matched; outer parser is now canonical,
                        // and should stop skipping
                        return Maybe.of(true);
                    }
                    isEndOfOptional = false;
                }

                if (templateTokens.isEmpty()) {
                    if (Strings.isNonBlank(inputRemaining) && valueUpdater==null) {
                        return Maybe.absent("Input has trailing characters after template is matched: '" + inputRemaining + "'");
                    }
                    if (optionalDepth>0)
                        return Maybe.absent("Mismatched optional marker in template");
                    return Maybe.of(true);
                }
                String t = templateTokens.remove(0);

                if (t.startsWith("[")) {
                    t = t.substring(1);
                    if (!t.isEmpty()) {
                        templateTokens.add(0, t);
                    }
                    String optionalPresentVar = null;
                    if (!templateTokens.isEmpty() && templateTokens.get(0).startsWith("?")) {
                        String v = templateTokens.remove(0);
                        if (v.startsWith("?${") && v.endsWith("}")) {
                            optionalPresentVar = v.substring(3, v.length() - 1);
                        } else {
                            throw new IllegalStateException("? after [ should indicate optional presence variable using syntax '?${var}', not '"+v+"'");
                        }
                    }
                    Maybe<Object> cr;
                    if (optionalSkippingInput<=0) {
                        // make a deep copy so that valueUpdater writes get replayed
                        Map<String, Object> backupResult = (Map) CollectionMerger.builder().deep(true).build().merge(MutableMap.of(), result);
                        Consumer<String> backupValueUpdater = valueUpdater;
                        String backupInputRemaining = inputRemaining;
                        List<String> backupTemplateTokens = MutableList.copyOf(templateTokens);
                        int oldDepth = optionalDepth;
                        int oldSkippingDepth = optionalSkippingInput;

                        optionalDepth++;
                        cr = doCall();
                        if (cr.isPresent()) {
                            // succeeded
                            if (optionalPresentVar!=null) result.put(optionalPresentVar, true);
                            continue;

                        } else {
                            // restore
                            result = backupResult;
                            valueUpdater = backupValueUpdater;
                            if (optionalPresentVar!=null) result.put(optionalPresentVar, false);
                            inputRemaining = backupInputRemaining;
                            templateTokens.clear();
                            templateTokens.addAll(backupTemplateTokens);
                            optionalDepth = oldDepth;
                            optionalSkippingInput = oldSkippingDepth;

                            optionalSkippingInput++;
                            optionalDepth++;
                            cr = doCall();
                            if (cr.isPresent()) {
                                optionalSkippingInput--;
                                continue;
                            }
                        }
                    } else {
                        if (optionalPresentVar!=null) {
                            result.put(optionalPresentVar, false);
                            valueUpdater = null;
                        }
                        optionalDepth++;
                        cr = doCall();
                        if (cr.isPresent()) {
                            continue;
                        }
                    }
                    return cr;
                }

                isEndOfOptional = t.endsWith("]");

                if (isEndOfOptional) {
                    t = t.substring(0, t.length() - 1);
                    if (t.isEmpty()) continue;
                    // next loop will process the end of the optionality
                }

                if (qst0.isQuoted(t)) {
                    if (optionalSkippingInput>0) continue;

                    String literal = qst0.unwrapIfQuoted(t);
                    do {
                        // ignore leading spaces (since the quoted string tokenizer will have done that anyway); but their _absence_ can be significant for intra-token searching when matching a var
                        inputRemaining = Strings.trimStart(inputRemaining);
                        if (inputRemaining.startsWith(Strings.trimStart(literal))) {
                            // literal found
                            inputRemaining = inputRemaining.substring(Strings.trimStart(literal).length());
                            continue outer;
                        }
                        if (inputRemaining.isEmpty()) return Maybe.absent("Literal '"+literal+"' expected, when end of input reached");
                        if (valueUpdater!=null) {
                            if (inputRemaining.isEmpty()) return Maybe.absent("Literal '"+literal+"' expected, when end of input tokens reached");
                            String value = getNextInputTokenUpToPossibleExpectedLiteral(literal);
                            valueUpdater.accept(value);
                            continue;
                        }
                        return Maybe.absent("Literal '"+literal+"' expected, when encountered '"+inputRemaining+"'");
                    } while (true);
                }

                if (t.startsWith("${") && t.endsWith("}")) {
                    if (optionalSkippingInput>0) continue;

                    t = t.substring(2, t.length()-1);
                    String value;

                    inputRemaining = inputRemaining.trim();
                    if (inputRemaining.isEmpty()) return Maybe.absent("End of input when looking for variable "+t);

                    if (!templateTokens.stream().filter(x -> !x.equals("]")).findFirst().isPresent()) {
                        // last word (whether optional or not) takes everything
                        value = getRemainderPossiblyRaw(inputRemaining);
                        inputRemaining = "";

                    } else {
                        value = getNextInputTokenUpToPossibleExpectedLiteral(null);
                    }
                    boolean multiMatch = t.endsWith("...");
                    if (multiMatch) t = Strings.removeFromEnd(t, "...");
                    String keys[] = t.split("\\.");
                    final String tt = t;
                    valueUpdater = v2 -> {
                        Map target = result;
                        for (int i=0; i<keys.length; i++) {
                            if (!Pattern.compile("[A-Za-z0-9_-]+").matcher(keys[i]).matches()) {
                                throw new IllegalArgumentException("Invalid variable '"+tt+"'");
                            }
                            if (i == keys.length - 1) {
                                target.compute(keys[i], (k, v) -> v == null ? v2 : v + " " + v2);
                            } else {
                                // need to make sure we have a map or null
                                target = (Map) target.compute(keys[i], (k, v) -> {
                                    if (v == null) return MutableMap.of();
                                    if (v instanceof Map) return v;
                                    return Maybe.absent("Cannot process shorthand for " + Arrays.asList(keys) + " because entry '" + k + "' is not a map (" + v + ")");
                                });
                            }
                        }
                    };
                    valueUpdater.accept(value);
                    if (!multiMatch) valueUpdater = null;
                    continue;
                }

                // unexpected token
                return Maybe.absent("Unexpected token in shorthand pattern '"+template+"' at position "+(template.lastIndexOf(t)+1));
            }
        }

        private String getRemainderPossiblyRawQst(QuotedStringTokenizer qstInput) {
            String value;
            value = Strings.join(qstInput.remainderRaw(), "");
            if (!options.finalMatchRaw) {
                return qstInput.unwrapIfQuoted(value);
            }
            return value;
        }

        private String getRemainderPossiblyRaw(String inputRemaining) {
            Maybe<String> mp = getRemainderPossiblyRawEp(inputRemaining);

            if (TRY_HARDER_FOR_QST_COMPATIBILITY || (mp.isAbsent() && !options.failOnMismatch)) {
                String qstResult = getRemainderPossiblyRawQst(qst0(inputRemaining));
                if (mp.isPresent() && !mp.get().equals(qstResult)) {
                    log.debug("Shorthand parsing semantics change for: "+inputOriginal+"\n" +
                            "  old qst: "+qstResult+"\n"+
                            "  new exp: "+mp.get());
                    // to debug
//                    getRemainderPossiblyRawEp(inputRemaining);
                }
                if (mp.isAbsent()) {
                    return qstResult;
                }
            }

            return mp.get();
        }
        private Maybe<String> getRemainderPossiblyRawEp(String inputRemaining) {
            if (options.finalMatchRaw) {
                return Maybe.of(inputRemaining);
            }
            Maybe<List<ParseNodeOrValue>> mp = ShorthandProcessorExprParser.tokenizer().parseEverything(inputRemaining);
            return mp.map(pnl -> {
                final boolean UNQUOTE_INDIVIDUAL_WORDS = false;  // legacy behaviour

                if (pnl.size()==1 || UNQUOTE_INDIVIDUAL_WORDS) {
                    return ExpressionParser.getAllUnquoted(pnl);
                } else {
                    return ExpressionParser.getUnescapedButNotUnquoted(pnl);
                }
            });
        }

        private String getNextInputTokenUpToPossibleExpectedLiteralQst(QuotedStringTokenizer qstInput, String nextLiteral) {
            String value;
            if (!qstInput.hasMoreTokens()) {
                return "";  // shouldn't happen
            }
            String v = qstInput.nextToken();
            if (qstInput.isQuoted(v)) {
                // input was quoted, eg "\"foo=b\" ..." -- ignore the = in "foo=b"
                value = qstInput.unwrapIfQuoted(v);
                inputRemaining = inputRemaining.substring(v.length());
            } else {
                // input not quoted, if next template token is literal, look for it
                boolean isLiteralExpected;
                if (nextLiteral==null) {
                    nextLiteral = templateTokens.get(0);
                    if (qstInput.isQuoted(nextLiteral)) {
                        nextLiteral = qstInput.unwrapIfQuoted(nextLiteral);
                        isLiteralExpected = true;
                    } else {
                        isLiteralExpected = false;
                    }
                } else {
                    isLiteralExpected = true;
                }
                if (isLiteralExpected) {
                    int nli = v.indexOf(nextLiteral);
                    if (nli>0) {
                        // literal found in unquoted string, eg "foo=bar" when literal is =
                        value = v.substring(0, nli);
                        inputRemaining = inputRemaining.substring(value.length());
                    } else {
                        // literal not found
                        value = v;
                        inputRemaining = inputRemaining.substring(value.length());
                    }
                } else {
                    // next is not a literal, so the whole token is the value
                    value = v;
                    inputRemaining = inputRemaining.substring(value.length());
                }
            }
            return value;
        }

        private String getNextInputTokenUpToPossibleExpectedLiteral(String nextLiteral) {
            String oi = inputRemaining;
            Maybe<String> v1 = getNextInputTokenUpToPossibleExpectedLiteralEp(nextLiteral);

            if (TRY_HARDER_FOR_QST_COMPATIBILITY || (v1.isAbsent() && !options.failOnMismatch)) {
                String ni = inputRemaining;

                inputRemaining = oi;
                String qstResult = getNextInputTokenUpToPossibleExpectedLiteralQst(qst0(inputRemaining), nextLiteral);
                if (v1.isPresent() && !v1.get().equals(qstResult)) {
                    log.debug("Shorthand parsing semantics change for literal " + nextLiteral + ": " + inputOriginal + "\n" +
                            "  old qst: " + qstResult + "\n" +
                            "  new exp: " + v1.get());

//                    // to debug differences
//                    inputRemaining = oi;
////                    getNextInputTokenUpToPossibleExpectedLiteralQst(qst0(inputRemaining), nextLiteral);
//                    getNextInputTokenUpToPossibleExpectedLiteralEp(nextLiteral);
                }
                if (v1.isAbsent()) {
                    return qstResult;
                }
                inputRemaining = ni;
            }

            return v1.get();
        }

        private Maybe<String> getNextInputTokenUpToPossibleExpectedLiteralEp(String nextLiteral) {
            String result = "";
            boolean canRepeat = true;

            Maybe<ParseNode> parseM = ShorthandProcessorExprParser.tokenizer().parse(inputRemaining);
            while (canRepeat) {
                String value;
                if (parseM.isAbsent()) return Maybe.castAbsent(parseM);

                List<ParseNodeOrValue> tokens = parseM.get().getContents();
                ParseNodeOrValue t = tokens.iterator().next();

                if (ExpressionParser.isQuotedExpressionNode(t)) {
                    // input was quoted, eg "\"foo=b\" ..." -- ignore the = in "foo=b"
                    value = ExpressionParser.getUnquoted(t);
                    inputRemaining = inputRemaining.substring(t.getSource().length());

                } else {
                    // input not quoted, if next template token is literal, look for it
                    boolean isLiteralExpected;
                    if (nextLiteral == null) {
                        String nl = templateTokens.get(0);
                        if (qst0.isQuoted(nl)) {
                            nextLiteral = qst0.unwrapIfQuoted(nl);
                            isLiteralExpected = true;
                        } else {
                            isLiteralExpected = false;
                        }
                    } else {
                        isLiteralExpected = true;
                    }
                    if (isLiteralExpected) {
                        int nli =
                                // previously took next QST token
                                qst0(inputRemaining).nextToken().indexOf(nextLiteral);

//                            // this is simple, slightly greedier;
//                            // slightly too greedy, in that nextLiteral inside quotes further away will match
//                            // and it breaks backwards compatibility
//                            inputRemaining.indexOf(nextLiteral);

//                            // this parse node is probably too short
//                            t.getSource().indexOf(nextLiteral);

                        final boolean ALLOW_NOTHING_BEFORE_LITERAL = false;
                        if ((nli == 0 && ALLOW_NOTHING_BEFORE_LITERAL) || nli > 0) {
                            // literal found in unquoted string, eg "foo=bar" when literal is =
                            if (!qst0(inputRemaining).nextToken().startsWith(inputRemaining.substring(0, nli))) {
                                String v = qst0(inputRemaining).nextToken();
                            }
                            value = inputRemaining.substring(0, nli);
                            inputRemaining = inputRemaining.substring(nli);
                            canRepeat = false;

                        } else {
                            // literal not found
                            value = t.getSource();  // since we know it isn't quoted
                            inputRemaining = inputRemaining.substring(value.length());
                        }
                    } else {
                        // next is not a literal, so the whole token is the value - not unquoted
                        value = t.getSource();
                        inputRemaining = inputRemaining.substring(value.length());
                    }
                }
                result += value;

                canRepeat &= !inputRemaining.isEmpty();
                if (canRepeat) {
                    parseM = ShorthandProcessorExprParser.tokenizer().parse(inputRemaining);
                    if (parseM.isAbsent()) canRepeat = false;
                    else {
                        if (ExpressionParser.startsWithWhitespace(parseM.get())) canRepeat = false;
                        // otherwise, to act like QST, we treat non-whitespace-separated things all as one token
                    }
                }
            }

            return Maybe.of(result);
        }

    }

}
