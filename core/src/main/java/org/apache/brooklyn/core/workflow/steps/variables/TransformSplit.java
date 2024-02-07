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

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.brooklyn.core.workflow.ShorthandProcessor;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.guava.Maybe;

public class TransformSplit extends WorkflowTransformDefault {

    String SHORTHAND = "\"split\" [ \"limit\" ${limit} ] [ ?${keep_delimiters} \"keep_delimiters\" ] [ ?${literal} \"literal\" ] [ ?${regex} \"regex\" ] ${delimiter}";

    Integer limit;
    String delimiter;
    boolean keep_delimiters, literal, regex;

    @Override
    protected void initCheckingDefinition() {
        Maybe<Map<String, Object>> maybeResult = new ShorthandProcessor(SHORTHAND)
                .withFinalMatchRaw(false)
                .process(transformDef);

        if (maybeResult.isPresent()) {
            Map<String, Object> result = maybeResult.get();
            keep_delimiters = Boolean.TRUE.equals(result.get("keep_delimiters"));
            literal = Boolean.TRUE.equals(result.get("literal"));
            regex = Boolean.TRUE.equals(result.get("regex"));
            limit = TransformSlice.resolveAs(result.get("limit"), context, "First argument 'limit'", false, Integer.class, "an integer");
            delimiter = TransformSlice.resolveAs(result.get("delimiter"), context, "Last argument 'delimiter'", true, String.class, "a string");

            // could disallow this, but it makes sense and works so we allow it;
            //if (Strings.isEmpty(delimiter)) throw new IllegalArgumentException("Delimiter to split must not be empty");

            if (regex && literal) throw new IllegalArgumentException("Only one of regex and literal can be set");
            if (!regex && !literal) literal = true;
        } else {
            throw new IllegalArgumentException("Expression must be of the form 'split [limit L] [keep_delimiters] [literal|regex] DELIMITER");
        }
    }

    @Override
    public Object apply(Object v) {
        if (v instanceof String) {
            List<String> split = MutableList.of();

            final String s = (String)v;
            Matcher m = regex ? Pattern.compile(delimiter).matcher((String) v) : null;

            int lastEnd = 0;
            while (true) {
                if (m==null) {
                    int index = s.indexOf(delimiter, lastEnd);

                    if (delimiter.isEmpty()) {
                        if (split.isEmpty()) {
                            split.add("");
                            if (s.isEmpty()) break;
                            if (keep_delimiters) split.add("");
                        }
                        index++;
                    }

                    if (index >= 0  && index<=s.length() && !s.isEmpty()) {
                        split.add(s.substring(lastEnd, index));
                        if (keep_delimiters) split.add(delimiter);
                        lastEnd = index + delimiter.length();
                    } else {
                        split.add(s.substring(lastEnd));
                        break;
                    }
                } else {
                    if (m.find() && !s.isEmpty()) {
                        if (m.start()<lastEnd) continue;
                        if (lastEnd==m.end() && !split.isEmpty()) {
                            // Matcher.find should increment, so this shouldn't happen, but double check;
                            // we do match at start and end, deliberately
                            throw new IllegalStateException("Regex match repeats splitting on empty string at same position");
                        }
                        split.add(s.substring(lastEnd, m.start()));
                        if (keep_delimiters) split.add(s.substring(m.start(), m.end()));
                        lastEnd = m.end();
                    } else {
                        split.add(s.substring(lastEnd));
                        break;
                    }
                }
                if (limit!=null && split.size() >= limit) {
                    split = split.subList(0, limit);
                    break;
                }
            }
            return split;

        } else {
            throw new IllegalStateException("Input must be a string to split");
        }
    }

}
