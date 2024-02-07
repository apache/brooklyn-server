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
package org.apache.brooklyn.core.workflow.steps.variables;

import org.apache.brooklyn.core.workflow.ShorthandProcessor;
import org.apache.brooklyn.util.guava.Maybe;

import java.util.Map;
import java.util.regex.Pattern;

public class TransformReplace extends WorkflowTransformDefault {
    boolean regex;
    boolean glob;
    boolean literal;
    boolean all;
    String patternToMatch;
    String replacement;

    String SHORTHAND = "\"replace\" [ ?${all} \"all\" ] [ ?${regex} \"regex\" ] [ ?${glob} \"glob\" ] [ ?${literal} \"literal\" ] ${patternToMatch} ${replacement}";

    @Override
    protected void initCheckingDefinition() {
        Maybe<Map<String, Object>> maybeResult = new ShorthandProcessor(SHORTHAND)
                .withFinalMatchRaw(false)
                .process(transformDef);

        if (maybeResult.isPresent()) {
            // TODO: Ability to parse Brooklyn DSL
            Map<String, Object> result = maybeResult.get();
            all = Boolean.TRUE.equals(result.get("all"));
            regex = Boolean.TRUE.equals(result.get("regex"));
            glob = Boolean.TRUE.equals(result.get("glob"));
            literal = Boolean.TRUE.equals(result.get("literal"));
            patternToMatch = String.valueOf(result.get("patternToMatch"));
            replacement = String.valueOf(result.get("replacement"));

            int replaceTypeCount = (regex?1:0) + (glob?1:0) + (literal?1:0);

            if (replaceTypeCount > 1) {
                throw new IllegalArgumentException("Only one of regex, glob, and literal can be set");
            } else if (replaceTypeCount == 0) {
                // Set the default if none provided
                literal = true;
            }
        } else {
            throw new IllegalArgumentException("Expression must be of the form 'replace [all] [regex|glob|literal] patternToMatch replacement'");
        }
    }

    @Override
    public Object apply(Object o) {
        if (o == null)
            return null;
        if (!(o instanceof String)) {
            throw new IllegalArgumentException("Expression must be of the form replace [regex|glob|literal] pattern_to_match replacement");
        }
        String input = (String)o;

        if (regex) {
            return all ? input.replaceAll(patternToMatch, replacement)
                    : input.replaceFirst(patternToMatch, replacement);
        }
        if (glob) {
            String globToRegex = convertGlobToRegex(patternToMatch, !all);

            return all ? input.replaceAll(globToRegex, replacement)
                    : input.replaceFirst(globToRegex, replacement);
        }
        if (literal) {
            return all ? Pattern.compile(patternToMatch, Pattern.LITERAL).matcher(input).replaceAll(replacement)
                    : Pattern.compile(patternToMatch, Pattern.LITERAL).matcher(input).replaceFirst(replacement);
        }
        // Should never get here
        throw new IllegalArgumentException("Expression must be of the form replace [regex|glob|literal] pattern_to_match replacement");
    }

    /**
     * Converts a standard POSIX Shell globbing pattern into a regular expression
     *
     * Copied from Neil Traft's answer in https://stackoverflow.com/questions/1247772/is-there-an-equivalent-of-java-util-regex-for-glob-type-patterns/17369948#17369948
     * See public domain statement in comments on the above
     *
     * TODO: Want to use library utility, but cant' find a suitable one other than the now-retired Apache Jakarta ORO
     * https://jakarta.apache.org/oro/
     *
     */
    private String convertGlobToRegex(String pattern, boolean isGreedy) {
        StringBuilder sb = new StringBuilder(pattern.length());
        int inGroup = 0;
        int inClass = 0;
        int firstIndexInClass = -1;
        char[] arr = pattern.toCharArray();
        for (int i = 0; i < arr.length; i++) {
            char ch = arr[i];
            switch (ch) {
                case '\\':
                    if (++i >= arr.length) {
                        sb.append('\\');
                    } else {
                        char next = arr[i];
                        switch (next) {
                            case ',':
                                // escape not needed
                                break;
                            case 'Q':
                            case 'E':
                                // extra escape needed
                                sb.append('\\');
                            default:
                                sb.append('\\');
                        }
                        sb.append(next);
                    }
                    break;
                case '*':
                    if (inClass == 0)
                        sb.append(".*"+(isGreedy ? "" : "?"));
                    else
                        sb.append('*');
                    break;
                case '?':
                    if (inClass == 0)
                        sb.append('.');
                    else
                        sb.append('?');
                    break;
                case '[':
                    inClass++;
                    firstIndexInClass = i+1;
                    sb.append('[');
                    break;
                case ']':
                    inClass--;
                    sb.append(']');
                    break;
                case '.':
                case '(':
                case ')':
                case '+':
                case '|':
                case '^':
                case '$':
                case '@':
                case '%':
                    if (inClass == 0 || (firstIndexInClass == i && ch == '^'))
                        sb.append('\\');
                    sb.append(ch);
                    break;
                case '!':
                    if (firstIndexInClass == i)
                        sb.append('^');
                    else
                        sb.append('!');
                    break;
                case '{':
                    inGroup++;
                    sb.append('(');
                    break;
                case '}':
                    inGroup--;
                    sb.append(')');
                    break;
                case ',':
                    if (inGroup > 0)
                        sb.append('|');
                    else
                        sb.append(',');
                    break;
                default:
                    sb.append(ch);
            }
        }
        return sb.toString();
    }
}
