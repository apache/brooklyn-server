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
import java.util.stream.Collectors;

import org.apache.brooklyn.core.workflow.utils.ExpressionParser;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.CharactersCollectingParseMode;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNode;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNodeOrValue;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseValue;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.tuple.Pair;

/**
 * An implentation of {@link ShorthandProcessor} which tries to use the ExpressionParser for everything.
 * However the semantics of a parse tree vs a linear string are too different so this is deprecated.
 *
 * It would be better to write this anew, and accept a breakage of semantics -- or use SPEpToQst (which we do)
 */
@Deprecated
public class ShorthandProcessorExprParser {

    private final String template;
    boolean finalMatchRaw = false;

    public ShorthandProcessorExprParser(String template) {
        this.template = template;
    }

    public Maybe<Map<String,Object>> process(String input) {
        return new ShorthandProcessorAttempt(this, input).call();
    }

    /** whether the last match should preserve quotes and spaces; default false */
    public ShorthandProcessorExprParser withFinalMatchRaw(boolean finalMatchRaw) {
        this.finalMatchRaw = finalMatchRaw;
        return this;
    }

    public static ExpressionParser tokenizer() {
        return ExpressionParser
                .newDefaultAllowingUnquotedAndSplittingOnWhitespace()
                .includeGroupingBracketsAtUsualPlaces()

                // including = means we can handle let x=1 !
                .includeAllowedTopLevelTransition(new CharactersCollectingParseMode("equals", '='))

                // whitespace in square brackets might be interesting -- though i don't think it is
//                .includeAllowedSubmodeTransition(ExpressionParser.SQUARE_BRACKET, ExpressionParser.WHITESPACE)
                ;
    }
    private static Maybe<List<ParseNodeOrValue>> tokenized(String x) {
        return tokenizer().parseEverything(x);
    }

    static class ShorthandProcessorAttempt {
        private final List<ParseNodeOrValue> templateTokensOriginal;
        private final String inputOriginal2;
        private final String template;
        private final ShorthandProcessorExprParser options;
        int optionalDepth = 0;
        int optionalSkippingInput = 0;
        Map<String, Object> result;
        Consumer<String> valueUpdater;

        ShorthandProcessorAttempt(ShorthandProcessorExprParser proc, String input) {
            this.template = proc.template;
            this.options = proc;
            this.templateTokensOriginal = tokenized(template).get();
            this.inputOriginal2 = input;
        }

        public synchronized Maybe<Map<String,Object>> call() {
            if (result != null) {
                throw new IllegalStateException("Only allowed to use once");
            }
            result = MutableMap.of();
            List<ParseNodeOrValue> templateTokens = MutableList.copyOf(templateTokensOriginal);
            List<ParseNodeOrValue> inputTokens = tokenized(inputOriginal2).get();
            Maybe<Boolean> error = doCall(templateTokens, inputTokens, 0);
            if (error.isAbsent()) return Maybe.Absent.castAbsent(error);
            if (!error.get()) return Maybe.absent("Template could not be matched."); // shouldn't happen
            chompWhitespace(inputTokens);
            if (!inputTokens.isEmpty()) {
                if (valueUpdater!=null) {
                    valueUpdater.accept(getRemainderPossiblyRaw(inputTokens));
                } else {
                    // shouldn't come here
                    return Maybe.absent("Input has trailing characters after template is matched: '" + inputTokens.stream().map(x -> x.getSource()).collect(Collectors.joining()) + "'");
                }
            }
            return Maybe.of(result);
        }

        private static final Object EMPTY = "empty";

        protected Maybe<Boolean> doCall(List<ParseNodeOrValue> templateTokens, List<ParseNodeOrValue> inputTokens, int depth) {
//            boolean isEndOfOptional = false;
            outer: while (true) {
//                if (isEndOfOptional) {
//                    if (optionalDepth <= 0) {
//                        throw new IllegalStateException("Unexpected optional block closure");
//                    }
//                    optionalDepth--;
//                    if (optionalSkippingInput>0) {
//                        // we were in a block where we skipped something optional because it couldn't be matched; outer parser is now canonical,
//                        // and should stop skipping
//                        return Maybe.of(true);
//                    }
//                    isEndOfOptional = false;
//                }

                if (templateTokens.isEmpty()) {
////                    if (Strings.isNonBlank(inputRemaining) && valueUpdater==null) {
////                        return Maybe.absent("Input has trailing characters after template is matched: '" + inputRemaining + "'");
////                    }
//                    if (optionalDepth>0)
//                        return Maybe.absent("Mismatched optional marker in template");
                    return Maybe.of(true);
                }
                ParseNodeOrValue tnv = templateTokens.remove(0);

                if (tnv.isParseNodeMode(ExpressionParser.SQUARE_BRACKET)) {
                    List<ParseNodeOrValue> tt = ((ParseNode) tnv).getContents();
                    String optionalPresentVar = null;
                    chompWhitespace(tt);
                    if (!tt.isEmpty() && tt.get(0).getParseNodeMode().equals(ParseValue.MODE) &&
                            ((String)tt.get(0).getContents()).startsWith("?")) {
                        ParseNodeOrValue vt = tt.remove(0);
                        ParseNodeOrValue vt2 = tt.stream().findFirst().orElse(null);
                        if (vt2!=null && vt2.isParseNodeMode(ExpressionParser.INTERPOLATED)) {
                            ParseValue vt2c = ((ParseNode) vt2).getOnlyContent()
                                    .mapMaybe(x -> x instanceof ParseValue ? Maybe.of((ParseValue)x) : Maybe.absent())
                                    .orThrow(() -> new IllegalStateException("? after [ should be followed by optional presence variable using syntax '?${var}', not '" + vt2.getSource() + "'"));
                            optionalPresentVar = vt2c.getContents();
                            tt.remove(0);
                        } else {
                            throw new IllegalStateException("? after [ should indicate optional presence variable using syntax '?${var}', not '"+vt.getSource()+"'");
                        }
                    }
                    Maybe<Boolean> cr;
                    if (optionalSkippingInput<=0) {
                        // make a deep copy so that valueUpdater writes get replayed
                        Map<String, Object> backupResult = (Map) CollectionMerger.builder().deep(true).build().merge(MutableMap.of(), result);
                        Consumer<String> backupValueUpdater = valueUpdater;
                        List<ParseNodeOrValue> backupInputRemaining = MutableList.copyOf(inputTokens);
                        List<ParseNodeOrValue> backupTT = tt;
                        int oldDepth = optionalDepth;
                        int oldSkippingDepth = optionalSkippingInput;

                        optionalDepth++;
                        tt = MutableList.copyOf(tt);
                        tt.addAll(templateTokens);
                        // this nested call handles the bracketed nodes, and everything after, to make sure the inclusion of the optional works to the end
                        cr = doCall(tt, inputTokens, depth+1);
                        if (cr.isPresent()) {
                            if (cr.get()) {
                                // succeeded
                                templateTokens.clear();  // because the subcall handled them
                                if (optionalPresentVar != null) result.put(optionalPresentVar, true);
                                continue;
                            } else {
                                // go into next block, we'll re-run, but with optional skipping turned on
                            }
                        }
                        // restore
                        result = backupResult;
                        valueUpdater = backupValueUpdater;
                        if (optionalPresentVar!=null) result.put(optionalPresentVar, false);
                        inputTokens.clear();
                        inputTokens.addAll(backupInputRemaining);
                        tt = backupTT;
                        optionalDepth = oldDepth;
                        optionalSkippingInput = oldSkippingDepth;

                        optionalSkippingInput++;
                        optionalDepth++;
//                        tt.add(0, tnv); // put our bracket back on the head so we come back in to this block
                        cr = doCall(tt, inputTokens, depth+1);
                        if (cr.isPresent()) {
                            optionalSkippingInput--;
                            continue;
                        }
                    } else {
                        if (optionalPresentVar!=null) {
                            result.put(optionalPresentVar, false);
                            valueUpdater = null;
                        }
                        optionalDepth++;
                        cr = doCall(tt, inputTokens, depth+1);
                        if (cr.isPresent()) {
                            continue;
                        }
                    }
                    // failed
                    return cr;
                }

                if (ExpressionParser.isQuotedExpressionNode(tnv)) {
                    if (optionalSkippingInput>0) continue;

                    Maybe<String> literalM = getSingleStringContents(tnv, "inside shorthand template quote");
                    if (literalM.isAbsent()) return Maybe.castAbsent(literalM);
                    String literal = Strings.trimStart(literalM.get());
//                    boolean foundSomething = false;
                    valueUpdater: do {
                        chompWhitespace(inputTokens);
                        literal: while (!inputTokens.isEmpty()) {
                            if (!literal.isEmpty()) {
                                Maybe<String> nextInputM = getSingleStringContents(inputTokens.stream().findFirst().orElse(null), "matching literal '" + literal + "'");
                                String nextInput = Strings.trimStart(nextInputM.orNull());
                                if (nextInput == null) {
                                    break; // shouldn't happen, but if so, fall through to error below
                                }
                                if (literal.startsWith(nextInput)) {
                                    literal = Strings.removeFromStart(literal, nextInput.trim());
                                    inputTokens.remove(0);
                                    chompWhitespace(inputTokens);
                                    literal = Strings.trimStart(literal);
                                    continue literal;
                                }
                                if (nextInput.startsWith(literal)) {
                                    String putBackOnInput = nextInput.substring(literal.length());
                                    literal = "";
                                    inputTokens.remove(0);
                                    if (!putBackOnInput.isEmpty()) inputTokens.add(0, new ParseValue(putBackOnInput));
                                    else chompWhitespace(inputTokens);
                                    // go into next block
                                }
                            }
                            if (literal.isEmpty()) {
                                // literal found
                                continue outer;
                            }
                            break;
                        }
                        if (inputTokens.isEmpty()) {
                            return Maybe.absent("Literal '" + literalM.get() + "' expected, when end of input reached");
                        }
                        if (valueUpdater!=null) {
                            if (inputTokens.isEmpty()) return Maybe.absent("Literal '"+literal+"' expected, when end of input tokens reached");
                            Pair<String,Boolean> value = getNextInputTokenUpToPossibleExpectedLiteral(inputTokens, templateTokens, literal, false);
                            if (value.getRight()) valueUpdater.accept(value.getLeft());
                            // always continue, until we see the literal
//                            if (!value.getRight()) {
//                                return Maybe.of(foundSomething);
//                            }
//                            foundSomething = true;
                            continue valueUpdater;
                        }
                        return Maybe.absent("Literal '"+literal+"' expected, when encountered '"+inputTokens+"'");
                    } while (true);
                }

                if (tnv.isParseNodeMode(ExpressionParser.INTERPOLATED)) {
                    if (optionalSkippingInput>0) continue;

                    Maybe<String> varNameM = getSingleStringContents(tnv, "in template interpolated variable definition");
                    if (varNameM.isAbsent()) return Maybe.castAbsent(varNameM);
                    String varName = varNameM.get();

                    if (inputTokens.isEmpty()) return Maybe.absent("End of input when looking for variable "+varName);

                    chompWhitespace(inputTokens);
                    String value;
                    if (templateTokens.isEmpty()) {
                        // last word (whether optional or not) takes everything
                        value = getRemainderPossiblyRaw(inputTokens);
                        inputTokens.clear();

                    } else {
                        Pair<String,Boolean> valueM = getNextInputTokenUpToPossibleExpectedLiteral(inputTokens, templateTokens, null, false);
                        if (!valueM.getRight()) {
                            // if we didn't find an expression, bail out
                            return Maybe.absent("Did not find expression prior to expected literal");
                        }
                        value = valueM.getLeft();
                    }
                    boolean dotsMultipleWordMatch = varName.endsWith("...");
                    if (dotsMultipleWordMatch) varName = Strings.removeFromEnd(varName, "...");
                    String keys[] = varName.split("\\.");
                    final String tt = varName;
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
                    if (!dotsMultipleWordMatch && !templateTokens.isEmpty()) valueUpdater = null;
                    continue;
                }

                if (tnv.isParseNodeMode(ExpressionParser.WHITESPACE)) {
                    chompWhitespace(templateTokens);
                    continue;
                }

                // unexpected token
                return Maybe.absent("Unexpected token in shorthand pattern '"+template+"' at "+tnv+", followed by "+templateTokens);
            }
        }

        private static Maybe<String> getSingleStringContents(ParseNodeOrValue tnv, String where) {
            if (tnv==null)
                Maybe.absent(()-> new IllegalArgumentException("No remaining tokens "+where));
            if (tnv instanceof ParseValue) return Maybe.of(((ParseValue)tnv).getContents());
            Maybe<ParseNodeOrValue> c = ((ParseNode) tnv).getOnlyContent();
            if (c.isAbsent()) Maybe.absent(()-> new IllegalArgumentException("Expected single string "+where));
            return getSingleStringContents(c.get(), where);
        }

        private static String getEscapedForInsideQuotedString(ParseNodeOrValue tnv, String where) {
            if (tnv==null) throw new IllegalArgumentException("No remaining tokens "+where);
            if (tnv instanceof ParseValue) return ((ParseValue)tnv).getContents();
            if (ExpressionParser.isQuotedExpressionNode(tnv))
                return ((ParseNode) tnv).getContents().stream().map(x -> getEscapedForInsideQuotedString(x, where)).collect(Collectors.joining());
            else
                return ((ParseNode) tnv).getContents().stream().map(x -> x.getSource()).collect(Collectors.joining());
        }

        private void chompWhitespace(List<ParseNodeOrValue> tt) {
            while (!tt.isEmpty() && tt.get(0).isParseNodeMode(ExpressionParser.WHITESPACE)) tt.remove(0);
        }

        private String getRemainderPossiblyRaw(List<ParseNodeOrValue> remainder) {
            if (options.finalMatchRaw) {
                return remainder.stream().map(ParseNodeOrValue::getSource).collect(Collectors.joining());
            } else {
                return remainder.stream().map(x -> {
                    if (x instanceof ParseValue) return ((ParseValue)x).getContents();
                    return getRemainderPossiblyRaw(((ParseNode)x).getContents());
                }).collect(Collectors.joining());
            }
        }

        private Pair<String,Boolean> getNextInputTokenUpToPossibleExpectedLiteral(List<ParseNodeOrValue> inputTokens, List<ParseNodeOrValue> templateTokens, String nextLiteral, boolean removeLiteralFromInput) {
            String value;
            ParseNodeOrValue nextInput = inputTokens.iterator().next();
            Boolean foundLiteral;
            Boolean foundContentsPriorToLiteral;
            if (ExpressionParser.isQuotedExpressionNode(nextInput)) {
                // quoted expressions in input won't match template literals
                value = getEscapedForInsideQuotedString(nextInput, "reading quoted input");
                inputTokens.remove(0);
                foundLiteral = false;
                foundContentsPriorToLiteral = true;
            } else {
                boolean isLiteralExpected;
                if (nextLiteral==null) {
                    // input not quoted, no literal supplied; if next template token is literal, look for it
                    chompWhitespace(templateTokens);
                    ParseNodeOrValue nextTT = templateTokens.isEmpty() ? null : templateTokens.get(0);
                    if (ExpressionParser.isQuotedExpressionNode(nextTT)) {
                        Maybe<String> nextLiteralM = getSingleStringContents(nextTT, "identifying expected literal");
                        isLiteralExpected = nextLiteralM.isPresent();
                        nextLiteral = nextLiteralM.orNull();
                    } else {
                        isLiteralExpected = false;
                    }
                } else {
                    isLiteralExpected = true;
                }
                if (isLiteralExpected) {
                    value = "";
                    while (true) {
                        Maybe<String> vsm = getSingleStringContents(nextInput, "looking for literal '" + nextLiteral + "'");
                        if (vsm.isAbsent()) {
                            foundLiteral = false;
                            break;
                        }
                        String vs = vsm.get();
                        int nli = (" "+vs+" ").indexOf(nextLiteral); // wrap the token we got in spaces in case there is a quoted space in the next literal
                        if (nli >= 0) {
                            // literal found in unquoted string, eg "foo=bar" when literal is =
                            if (nli>0) nli--; // walk back the space
                            value += vs.substring(0, nli);
                            value = Strings.trimEnd(value);
                            if (removeLiteralFromInput) nli += nextLiteral.length();
                            String putBackOnInput = vs.substring(nli);
                            inputTokens.remove(0);
                            if (!putBackOnInput.isEmpty()) inputTokens.add(0, new ParseValue(putBackOnInput));
                            foundLiteral = true;
                            break;
                        } else {
                            nli = (" "+value + vs).indexOf(nextLiteral);
                            if (nli >= 0) {
                                // found in concatenation
                                if (nli>0) nli--; // walk back the space
                                String putBackOnInput = (value + vs).substring(nli);
                                value = (value + vs).substring(0, nli);
                                value = Strings.trimEnd(value);
                                inputTokens.remove(0);
                                if (removeLiteralFromInput) putBackOnInput = putBackOnInput.substring(nextLiteral.length());
                                if (!putBackOnInput.isEmpty()) inputTokens.add(0, new ParseValue(putBackOnInput));
                                foundLiteral = true;
                                break;
                            } else {
                                // literal not found
                                value += vs;
                                inputTokens.remove(0);
                                if (inputTokens.isEmpty()) {
                                    foundLiteral = false;
                                    break;
                                } else {
                                    nextInput = inputTokens.iterator().next();
                                    continue;
                                }
                            }
                        }
                    }
                    foundContentsPriorToLiteral = Strings.isNonEmpty(value);
                } else {
                    // next is not a literal, so take the next, and caller will probably recurse, taking the entire rest as the value
                    value = getSingleStringContents(nextInput, "taking remainder when no literal").or("");
                    foundLiteral = false;
                    foundContentsPriorToLiteral = true;
                    inputTokens.remove(0);
                }
            }
            return Pair.of(value, foundContentsPriorToLiteral);
        }
    }

}
