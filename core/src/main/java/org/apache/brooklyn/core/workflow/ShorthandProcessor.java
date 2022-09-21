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

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Accepts a shorthand template, and converts it to a map of values,
 * e.g. given template "[ ${sensor.type} ] ${sensor.name} \"=\" ${value}"
 * and input "integer foo=3", this will return
 * { sensor: { type: integer, name: foo }, value: 3 }.
 *
 * Expects space separated TOKEN where TOKEN is either:
 *
 * [ TOKEN ] - to indicate TOKEN is optional. parsing is attempted first with it, then without it.
 * ${VAR} - to set VAR, which should be of the regex [A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*, with dot separation used to set nested maps;
 *   will match a quoted string if supplied, else up to the next literal if the next token is a literal, else the next work.
 * "LITERAL" - to expect a literal expression. this should include spaces if spaces are required.
 */
public class ShorthandProcessor {

    private final String template;

    public ShorthandProcessor(String template) {
        this.template = template;
    }

    public Maybe<Map<String,Object>> process(String input) {
        return new ShorthandProcessorAttempt(template, input).call();
    }

    static class ShorthandProcessorAttempt {
        private final List<String> templateTokens;
        private final String inputOriginal;
        private final QuotedStringTokenizer qst;
        private final String template;
        int optionalDepth = 0;
        boolean optionalSkippingInput = false;
        private String inputRemaining;
        Map<String, Object> result;

        ShorthandProcessorAttempt(String template, String input) {
            this.template = template;
            this.qst = qst(template);
            this.templateTokens = qst.remainderAsList();
            this.inputOriginal = input;
        }

        private QuotedStringTokenizer qst(String x) {
            return QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(false).keepInternalQuotes(true).build(x);
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
            if (Strings.isNonBlank(inputRemaining)) return Maybe.absent("Input has trailing characters after template is matched: '"+inputRemaining+"'");
            return Maybe.of(result);
        }

        protected Maybe<Object> doCall() {
            boolean isEndOfOptional = false;
            outer: while (true) {
                if (isEndOfOptional) {
                    if (optionalDepth <= 0) throw new IllegalStateException("Unexpected optional block closure");
                    optionalDepth--;
                    if (optionalSkippingInput) {
                        // we were in a block where we skipped something optional because it couldn't be matched; outer parser is now canonical,
                        // and should stop skipping
                        return Maybe.of(true);
                    }
                    isEndOfOptional = false;
                }

                if (templateTokens.isEmpty()) {
                    return Maybe.of(true);
                }
                String t = templateTokens.remove(0);

                if (t.startsWith("[")) {
                    t = t.substring(1);
                    if (!t.isEmpty()) {
                        templateTokens.add(0, t);
                    }
                    Maybe<Object> cr;
                    if (!optionalSkippingInput) {
                        MutableMap<String, Object> backupResult = MutableMap.copyOf(result);
                        String backupInputRemaining = inputRemaining;
                        List<String> backupTemplateTokens = MutableList.copyOf(templateTokens);

                        optionalDepth++;
                        cr = doCall();
                        if (cr.isPresent()) {
                            // succeeded
                            continue;

                        } else {
                            // restore
                            result = backupResult;
                            inputRemaining = backupInputRemaining;
                            templateTokens.clear();
                            templateTokens.addAll(backupTemplateTokens);

                            optionalSkippingInput = true;
                            optionalDepth++;
                            cr = doCall();
                            if (cr.isPresent()) {
                                optionalSkippingInput = false;
                                continue;
                            }
                        }
                    } else {
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

                if (qst.isQuoted(t)) {
                    if (optionalSkippingInput) continue;

                    String literal = qst.unwrapIfQuoted(t);
                    do {
                        // ignore leading spaces (since the quoted string tokenizer will have done that anyway); but their _absence_ can be significant for intra-token searching when matching a var
                        if (Strings.trimStart(inputRemaining).startsWith(Strings.trimStart(literal))) {
                            inputRemaining = Strings.trimStart(inputRemaining).substring(Strings.trimStart(literal).length());
                            continue outer;
                        }
                        if (inputRemaining.isEmpty()) return Maybe.absent("Literal '"+literal+"' expected, when end of input reached");
                        return Maybe.absent("Literal '"+literal+"' expected, when encountered '"+inputRemaining+"'");
                    } while (true);
                }

                if (t.startsWith("${") && t.endsWith("}")) {
                    if (optionalSkippingInput) continue;

                    t = t.substring(2, t.length()-1);
                    String value;
                    if (templateTokens.isEmpty()) {
                        // last word takes everything, but trimmed parsed and joined with spaces
                        value = Strings.join(qst(inputRemaining).remainderAsList().stream().map(qst::unwrapIfQuoted).collect(Collectors.toList()), " ");
                        inputRemaining = "";

                    } else {
                        inputRemaining = inputRemaining.trim();
                        QuotedStringTokenizer qst = qst(inputRemaining);
                        if (!qst.hasMoreTokens()) return Maybe.absent("End of input when looking for variable "+t);
                        String v = qst.nextToken();
                        if (qst.isQuoted(v)) {
                            // input was quoted, eg "\"foo=b\" ..." -- ignore the = in "foo=b"
                            value = qst.unwrapIfQuoted(v);
                            inputRemaining = inputRemaining.substring(v.length());
                        } else {
                            // input not quoted, if next template token is literal, look for it
                            String nextLiteral = templateTokens.get(0);
                            if (qst.isQuoted(nextLiteral)) {
                                nextLiteral = qst.unwrapIfQuoted(nextLiteral);
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
                    }
                    String keys[] = t.split("\\.");
                    Map target = result;
                    for (int i=0; i<keys.length; i++) {
                        if (i==keys.length-1) target.put(keys[i], value);
                        else {
                            // need to make sure we have a map or null
                            target = (Map) target.compute(keys[i], (k,v) -> {
                                if (v==null) return MutableMap.of();
                                if (v instanceof Map) return v;
                                return Maybe.absent("Cannot process shorthand for "+ Arrays.asList(keys)+" because entry '"+k+"' is not a map ("+v+")");
                            });
                        }
                    }

                    continue;
                }

                // unexpected token
                return Maybe.absent("Unexpected token in shorthand pattern '"+template+"'");
            }
        }
    }


}
