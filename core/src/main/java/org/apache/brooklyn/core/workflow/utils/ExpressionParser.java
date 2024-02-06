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

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.BackslashParseMode;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;

import static org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.CharactersCollectingParseMode;
import static org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.CommonParseMode;
import static org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseMode;
import static org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNode;
import static org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNodeOrValue;
import static org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseValue;
import static org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.TopLevelParseMode;

/** simplistic parser for workflow expressions and strings, recognizing single and double quotes, backslash escapes, and interpolated strings with ${...} */
public abstract class ExpressionParser {

    public abstract Maybe<ParseNode> parse(String input);
    public abstract Maybe<List<ParseNodeOrValue>> parseEverything(String inputRemaining);


    public static final ParseMode BACKSLASH_ESCAPE = new BackslashParseMode();
    public static final ParseMode WHITESPACE = new CharactersCollectingParseMode("whitespace", Character::isWhitespace);

    public static final ParseMode DOUBLE_QUOTE = CommonParseMode.transitionNested("double_quote", "\"", "\"");
    public static final ParseMode SINGLE_QUOTE = CommonParseMode.transitionNested("single_quote", "\'", "\'");
    public static final ParseMode INTERPOLATED = CommonParseMode.transitionNested("interpolated_expression", "${", "}");

    public static final ParseMode SQUARE_BRACKET = CommonParseMode.transitionNested("square_bracket", "[", "]");
    public static final ParseMode PARENTHESES = CommonParseMode.transitionNested("parenthesis", "(", ")");
    public static final ParseMode CURLY_BRACES = CommonParseMode.transitionNested("curly_brace", "{", "}");


    private static Multimap<ParseMode,ParseMode> getCommonInnerTransitions() {
        ListMultimap<ParseMode,ParseMode> m = Multimaps.newListMultimap(MutableMap.of(), MutableList::of);
        m.putAll(BACKSLASH_ESCAPE, MutableList.of());
        m.putAll(SINGLE_QUOTE, MutableList.of(BACKSLASH_ESCAPE, INTERPOLATED));
        m.putAll(DOUBLE_QUOTE, MutableList.of(BACKSLASH_ESCAPE, INTERPOLATED));
        m.putAll(INTERPOLATED, MutableList.of(BACKSLASH_ESCAPE, DOUBLE_QUOTE, SINGLE_QUOTE, INTERPOLATED));
        return Multimaps.unmodifiableMultimap(m);
    }
    public static final Multimap<ParseMode,ParseMode> COMMON_INNER_TRANSITIONS = getCommonInnerTransitions();
    public static final List<ParseMode> COMMON_TOP_LEVEL_TRANSITIONS = MutableList.of(BACKSLASH_ESCAPE, DOUBLE_QUOTE, SINGLE_QUOTE, INTERPOLATED).asUnmodifiable();


    public static ExpressionParserImpl newDefaultAllowingUnquotedAndSplittingOnWhitespace() {
        return new ExpressionParserImpl(new TopLevelParseMode(true),
                MutableList.copyOf(COMMON_TOP_LEVEL_TRANSITIONS).append(WHITESPACE),
                COMMON_INNER_TRANSITIONS);
    }
    public static ExpressionParserImpl newDefaultAllowingUnquotedLiteralValues() {
        return new ExpressionParserImpl(new TopLevelParseMode(true),
                MutableList.copyOf(COMMON_TOP_LEVEL_TRANSITIONS),
                COMMON_INNER_TRANSITIONS);
    }
    public static ExpressionParserImpl newDefaultRequiringQuotingOrExpressions() {
        return new ExpressionParserImpl(new TopLevelParseMode(false),
                MutableList.copyOf(COMMON_TOP_LEVEL_TRANSITIONS),
                COMMON_INNER_TRANSITIONS);
    }

    public static ExpressionParserImpl newEmptyDefault(boolean requiresQuoting) {
        return newEmpty(new TopLevelParseMode(!requiresQuoting));
    }
    public static ExpressionParserImpl newEmpty(TopLevelParseMode topLevel) {
        return new ExpressionParserImpl(topLevel, MutableList.of());
    }


    public static boolean isQuotedExpressionNode(ParseNodeOrValue next) {
        if (next==null) return false;
        return next.isParseNodeMode(SINGLE_QUOTE) || next.isParseNodeMode(DOUBLE_QUOTE);
    }

    public static String getAllUnquoted(List<ParseNodeOrValue> mp) {
        return join(mp, ExpressionParser::getUnquoted);
    }

    public static String getAllSource(List<ParseNodeOrValue> mp) {
        return join(mp, ParseNodeOrValue::getSource);
    }

    public static String getUnescapedButNotUnquoted(List<ParseNodeOrValue> mp) {
        return join(mp, ExpressionParser::getUnescapedButNotUnquoted);
    }

    public static String getUnescapedButNotUnquoted(ParseNodeOrValue mp) {
        if (mp instanceof ParseValue) return ((ParseValue)mp).getContents();

        // backslashes are unescaped
        if (mp.isParseNodeMode(BACKSLASH_ESCAPE)) return getAllSource(((ParseNode)mp).getContents());

        // in interpolations we don't unescape
        if (mp.isParseNodeMode(INTERPOLATED)) return mp.getSource();
        // in double quotes we don't unescape
        if (isQuotedExpressionNode(mp)) return mp.getSource();

        // everything else is recursed
        return getContentsWithStartAndEnd((ParseNode) mp, ExpressionParser::getUnescapedButNotUnquoted);
    }

    public static String getContentsWithStartAndEnd(ParseNode mp, Function<List<ParseNodeOrValue>,String> fn) {
        String v = fn.apply(mp.getContents());
        ParseMode m = mp.getParseNodeModeClass();
        if (m instanceof CommonParseMode) {
            return Strings.toStringWithValueForNull(((CommonParseMode)m).enterOnString, "")
                    + v
                    + Strings.toStringWithValueForNull(((CommonParseMode)m).exitOnString, "");
        }
        return v;
    }

    public static boolean startsWithWhitespace(ParseNodeOrValue pn) {
        if (pn.isParseNodeMode(WHITESPACE)) return true;
        if (pn instanceof ParseValue) return ((ParseValue)pn).getContents().startsWith(" ");
        return ((ParseNode)pn).getStartingContent().map(ExpressionParser::startsWithWhitespace).or(false);
    }


    public static String join(List<ParseNodeOrValue> mp, Function<ParseNodeOrValue,String> fn) {
        return mp.stream().map(fn).collect(Collectors.joining());
    }

    public static String getUnquoted(ParseNodeOrValue mp) {
        if (mp instanceof ParseValue) return ((ParseValue)mp).getContents();
        ParseNode mpn = (ParseNode) mp;

        // unquote, unescaping what's inside
        if (isQuotedExpressionNode(mp)) return getUnescapedButNotUnquoted(((ParseNode) mp).getContents());

        // source for anything else
        return mp.getSource();
    }

    /** removes whitespace, either WHITESPACE nodes, or values containing whitespace */
    public static boolean isBlank(ParseNodeOrValue n) {
        if (n.isParseNodeMode(WHITESPACE)) return true;
        if (n instanceof ParseValue) return Strings.isBlank(((ParseValue) n).getContents());
        return false;
    }

    /** removes whitespace, either WHITESPACE or empty/blank ParseValue nodes,
     * and changing ParseValues starting or ending with whitespace and the start or end (once other whitespace removed) */
    public static List<ParseNodeOrValue> trimWhitespace(List<ParseNodeOrValue> contents) {
        return trimWhitespace(contents, true, true);
    }
    public static List<ParseNodeOrValue> trimWhitespace(List<ParseNodeOrValue> contents, boolean removeFromStart, boolean removeFromEnd) {
        List<ParseNodeOrValue> result = MutableList.of();
        boolean changed = false;
        Iterator<ParseNodeOrValue> ni = contents.iterator();
        while (ni.hasNext()) {
            ParseNodeOrValue n = ni.next();
            if (isBlank(n)) {
                changed = true;
                continue;

            } else {
                if (n instanceof ParseValue) {
                    String c = ((ParseValue) n).getContents();
                    if (Character.isWhitespace(c.charAt(0))) {
                        changed = true;
                        while (!c.isEmpty() && Character.isWhitespace(c.charAt(0))) c = c.substring(1);
                        n = new ParseValue(c);
                    }
                }
                result.add(n);
                break;
            }
        }
        if (ni.hasNext()) {
            // did the start - now need to identify the last non-whitespace thing, and then will need to replay it
            ParseNodeOrValue lastNonWhite = null;
            for (ParseNodeOrValue pn: contents) {
                if (!isBlank(pn)) lastNonWhite = pn;
            }
            if (lastNonWhite==null) throw new IllegalStateException("Non-whitespace was found but then not found"); // shouldn't happen

            ParseNodeOrValue n;
            do {
                n = ni.next();
                if (n == lastNonWhite) break;
                result.add(n);
            } while (ni.hasNext());

            if (n instanceof ParseValue) {
                String c = ((ParseValue) n).getContents();
                if (Character.isWhitespace(c.charAt(c.length()-1))) {
                    changed = true;
                    while (!c.isEmpty() && Character.isWhitespace(c.charAt(c.length()-1))) c = c.substring(0, c.length()-1);
                    n = new ParseValue(c);
                }
            }
            result.add(n);

            // and ignore the rest
        }

        if (!changed) return contents;
        return result;
    }

}
