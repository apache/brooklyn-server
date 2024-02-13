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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;

/** simplistic parser for workflow expressions and strings, recognizing single and double quotes, backslash escapes, and interpolated strings with ${...} */
public class ExpressionParserImpl extends ExpressionParser {

    public interface ParseNodeOrValue {
        String getParseNodeMode();
        default boolean isParseNodeMode(String pm, String ...pmi) {
            if (Objects.equals(getParseNodeMode(), pm)) return true;
            for (String pmx: pmi) {
                if (Objects.equals(getParseNodeMode(), pmx)) return true;
            }
            return false;
        }
        default boolean isParseNodeMode(ParseMode pm, ParseMode ...pmi) {
            if (Objects.equals(getParseNodeMode(), pm.getModeName())) return true;
            for (ParseMode pmx: pmi) {
                if (Objects.equals(getParseNodeMode(), pmx.getModeName())) return true;
            }
            return false;
        }
        default boolean isParseNodeMode(Collection<ParseMode> pm) {
            return pm.stream().anyMatch(pmx -> Objects.equals(getParseNodeMode(), pmx.getModeName()));
        }
        String getSource();
        Object getContents();
    }
    public static class ParseValue implements ParseNodeOrValue {
        public final static String MODE = "value";
        final String value;

        public ParseValue(String value) { this.value = value; }

        @Override
        public String getSource() {
            return value;
        }

        @Override public String getContents() { return value; }

        @Override
        public String toString() {
            return "["+value+"]";
        }

        @Override
        public String getParseNodeMode() {
            return MODE;
        }
    }
    public static class ParseNode implements ParseNodeOrValue {
        String source;
        ParseMode mode;
        public ParseNode(ParseMode mode, String source) {
            this.mode = mode;
            this.source = source;
        }
        public static ParseNode ofValue(ParseMode mode, String source, String value) {
            ParseNode pr = new ParseNode(mode, source);
            pr.contents = MutableList.of(new ParseValue(value));
            return pr;
        }
        List<ParseNodeOrValue> contents = null;

        public String getSource() {
            return source;
        }
        @Override public String getParseNodeMode() {
            return mode.name;
        }
        public ParseMode getParseNodeModeClass() {
            return mode;
        }

        public List<ParseNodeOrValue> getContents() {
            return contents;
        }
        public Maybe<ParseNodeOrValue> getOnlyContent() {
            if (contents==null) return Maybe.absent("no contents set");
            if (contents.size()==1) return Maybe.of(contents.iterator().next());
            return Maybe.absent(contents.isEmpty() ? "no items" : contents.size()+" items");
        }
        public Maybe<ParseNodeOrValue> getFinalContent() {
            if (contents==null) return Maybe.absent("no contents set");
            if (contents.isEmpty()) return Maybe.absent("contents empty");
            return Maybe.of(contents.get(contents.size() - 1));
        }
        public Maybe<ParseNodeOrValue> getStartingContent() {
            if (contents==null) return Maybe.absent("no contents set");
            if (contents.isEmpty()) return Maybe.absent("contents empty");
            return Maybe.of(contents.get(0));
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(mode.name);
            if (contents==null) sb.append("?");

            if (contents!=null && contents.size()==1 && getOnlyContent().get() instanceof ParseValue) sb.append(contents.iterator().next().toString());
            else {
                sb.append("[");
                if (contents == null) sb.append(source);
                else sb.append(contents.stream().map(c -> c.toString()).collect(Collectors.joining()));
                sb.append("]");
            }
            return sb.toString();
        }
    }

    public static class InternalParseResult {
        public final boolean skip;
        public boolean earlyTerminationRequested;
        @Nullable /** null means no error and no result */
        public final Maybe<ParseNode> resultOrError;
        protected InternalParseResult(boolean skip, Maybe<ParseNode> resultOrError) {
            this.skip = skip;
            this.resultOrError = resultOrError;
        }
        public static final InternalParseResult SKIP = new InternalParseResult(true, null);

        public static InternalParseResult of(ParseNode parseModeResult) {
            return new InternalParseResult(false, Maybe.of(parseModeResult));
        }
        public static InternalParseResult ofValue(ParseMode mode, String source, String value) {
            return of(ParseNode.ofValue(mode, source, value));
        }
        public static InternalParseResult ofError(String message) {
            return new InternalParseResult(false, Maybe.absent(message));
        }
        public static InternalParseResult ofError(Throwable t) {
            return new InternalParseResult(false, Maybe.absent(t));
        }
    }

    public abstract static class ParseMode {
        final String name;

        public ParseMode(String name) {
            this.name = name;
        }

        public String getModeName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }

        public abstract InternalParseResult parse(String input, int offset, Multimap<ParseMode, ParseMode> allowedTransitions, @Nullable Collection<ParseMode> transitionsAllowedHereOverride);
    }

    public static class CommonParseMode extends ParseMode {
        public final String enterOnString;
        public final String exitOnString;

        /** parses anything starting with 'enterOnString'; if exitOnString is not null, it does so until 'exitOnString' is encountered, then reverts to previous mode */
        protected CommonParseMode(String name, String enterOnString, @Nullable String exitOnString) {
            super(name);
            Preconditions.checkNotNull(enterOnString);
            this.enterOnString = enterOnString;
            this.exitOnString = exitOnString;
        }

        /** parses anything starting with 'enterOnString', until another mode is entered */
        public static CommonParseMode transitionSimple(String name, String enterOnString) {
            Preconditions.checkNotNull(enterOnString);
            Preconditions.checkArgument(!enterOnString.isEmpty());
            return new CommonParseMode(name, enterOnString, null);
        }

        /** parses anything starting with 'enterOnString', until 'exitOnString' is reached when it reverts to previous mode */
        public static CommonParseMode transitionNested(String name, String enterOnString, String exitOnString) {
            Preconditions.checkNotNull(exitOnString);
            Preconditions.checkArgument(!exitOnString.isEmpty());
            return new CommonParseMode(name, enterOnString, exitOnString);
        }

        public boolean allowedToTakeUnmatchedCharsAsLiteralValue() { return true; /** currently all non-top-level custom parse modes do this; but could be overridden */ }
        public boolean allowedToExitBeforeExitStringEncountered() { return false; /** currently all non-top-level custom parse modes do not this; but could be overridden */ }

        public InternalParseResult parse(String input, int offset, Multimap<ParseMode, ParseMode> allowedTransitions, Collection<ParseMode> transitionsAllowedHereOverride) {
            if (!input.substring(offset).startsWith(enterOnString)) return InternalParseResult.SKIP;

            ParseMode currentLevelMode = this;
            ParseNode result = new ParseNode(currentLevelMode, input);
            result.contents = MutableList.of();
            int i=offset;
            if (!input.substring(i).startsWith(enterOnString))
                return InternalParseResult.ofError("wrong start sequence for " + currentLevelMode + " at position " + i);
            i+= enterOnString.length();

            int last = i;
            boolean exitStringEncountered = false;
            boolean earlyTerminationRequested = false;
            if (transitionsAllowedHereOverride==null) transitionsAllowedHereOverride = allowedTransitions.get(currentLevelMode);
            if (transitionsAllowedHereOverride==null || transitionsAllowedHereOverride.isEmpty()) throw new IllegalStateException("Mode '"+currentLevelMode+"' is not configured with any transitions");
            input: while (i<input.length()) {
                if (exitOnString !=null && input.substring(i).startsWith(exitOnString)) {
                    if (i>last) result.contents.add(new ParseValue(input.substring(last, i)));
                    i+= exitOnString.length();
                    last = i;
                    exitStringEncountered = true;
                    break;
                }
                Maybe<ParseNode> error = null;
                for (ParseMode candidate: transitionsAllowedHereOverride) {
                    InternalParseResult cpr = candidate.parse(input, i, allowedTransitions, null);
                    if (cpr.skip) continue;
                    if (cpr.resultOrError.isPresent()) {
                        if (i>last) result.contents.add(new ParseValue(input.substring(last, i)));
                        result.contents.add(cpr.resultOrError.get());
                        i += cpr.resultOrError.get().source.length();
                        last = i;
                        if (cpr.earlyTerminationRequested) {
                            earlyTerminationRequested = true;
                            break input;
                        }
                        continue input;
                    }
                    if (error == null) {
                        error = cpr.resultOrError;
                    } else {
                        // multiple possible modes, not used currently;
                        // only take error from first mode
                    }
                }
                if (!allowedToTakeUnmatchedCharsAsLiteralValue() && error==null) {
                    if (allowedToExitBeforeExitStringEncountered()) break;
                    error = Maybe.absent("Characters starting at " + i + " not permitted for " + currentLevelMode);
                }
                if (error!=null) return InternalParseResult.ofError(Maybe.Absent.getException(error));
                i++;
            }

            if (!exitStringEncountered && !allowedToExitBeforeExitStringEncountered()) return InternalParseResult.ofError("Non-terminated "+currentLevelMode.name);
            if (i>last) result.contents.add(new ParseValue(input.substring(last, i)));
            last = i;
            result.source = input.substring(offset, last);
            InternalParseResult resultIPR = InternalParseResult.of(result);
            Maybe<ParseNodeOrValue> lastPMR = result.getFinalContent();
            if (earlyTerminationRequested) {
                resultIPR.earlyTerminationRequested = true;
            }
            return resultIPR;
        }
    }

    public static class BackslashParseMode extends ParseMode {
        public static final Map<Character,String> COMMON_ESAPES = MutableMap.of(
                'n', "\n",
                'r', "\r",
                't', "\t",
                '0', "\0"
                // these are supported because by default we support the same character
//                '\'', "\'",
//                '\"', "\""
        ).asUnmodifiable();

        public BackslashParseMode() { super(MODE); }
        final static String MODE = "backslash_escape";
        final static String START = "\\";
        @Override
        public InternalParseResult parse(String input, int offset, Multimap<ParseMode, ParseMode> _allowedTransitions, Collection<ParseMode> _transitionsAllowedHereOverride) {
            if (!input.substring(offset).startsWith(START)) return InternalParseResult.SKIP;
            if (input.substring(offset).length()>=2) {
                char c = input.charAt(offset+1);
                return InternalParseResult.ofValue(this, input.substring(offset, offset+2), Maybe.ofDisallowingNull(COMMON_ESAPES.get(c)).or(() -> ""+c));
            } else {
                return InternalParseResult.ofError("Backslash escape character not permitted at end of string");
            }
        }
    }

    public static class CharactersCollectingParseMode extends ParseMode {
        final Predicate<Character> charactersAcceptable;
        public CharactersCollectingParseMode(String name, Predicate<Character> charactersAcceptable) {
            super(name);
            this.charactersAcceptable = charactersAcceptable;
        }
        public CharactersCollectingParseMode(String name, char c) {
            this(name, cx -> Objects.equals(cx, c));
        }
        @Override
        public InternalParseResult parse(String input, int offset, Multimap<ParseMode, ParseMode> _allowedTransitions, Collection<ParseMode> _transitionsAllowedHereOverride) {
            int i=offset;
            while (i<input.length() && charactersAcceptable.test(input.charAt(i))) {
                i++;
            }
            if (i>offset) {
                String v = input.substring(offset, i);
                return InternalParseResult.ofValue(this, v, v);
            } else {
                return InternalParseResult.SKIP;
            }
        }
    }

    public static class TopLevelParseMode extends CommonParseMode {
        private final boolean allowsValues;
        private final List<ParseMode> allowedTransitions = MutableList.of();
        public List<String> mustEndWithOneOfTheseModes;

        protected TopLevelParseMode(boolean allowsUnquotedLiteralValues) {
            super("top-level", "", null);
            this.allowsValues = allowsUnquotedLiteralValues;
        }

        @Override
        public boolean allowedToTakeUnmatchedCharsAsLiteralValue() {
            return allowsValues;
        }

        @Override
        public boolean allowedToExitBeforeExitStringEncountered() {
            return true;
        }

        @Override
        public InternalParseResult parse(String input, int offset, Multimap<ParseMode, ParseMode> allowedTransitions, Collection<ParseMode> transitionsAllowedHereOverride) {
            InternalParseResult result = super.parse(input, offset, allowedTransitions, transitionsAllowedHereOverride);
            if (mustEndWithOneOfTheseModes!=null && result.resultOrError.isPresent()) {
                String error = result.resultOrError.get().getFinalContent().map(pn ->
                        mustEndWithOneOfTheseModes.contains(pn.getParseNodeMode())
                            ? null
                            : "Expression ends with " + pn.getParseNodeMode() + " but should have ended with required token: " + mustEndWithOneOfTheseModes)
                        .or("Expression is empty but was expected to end with required token: " + mustEndWithOneOfTheseModes);
                if (error!=null) return InternalParseResult.ofError(error);
            }
            return result;
        }
    }

    private final Multimap<ParseMode,ParseMode> allowedTransitions = Multimaps.newListMultimap(MutableMap.of(), MutableList::of);
    private final TopLevelParseMode topLevel;

    protected ExpressionParserImpl(TopLevelParseMode topLevel, List<ParseMode> allowedAtTopLevel, Multimap<ParseMode,ParseMode> allowedTransitions) {
        this(topLevel, allowedAtTopLevel);
        this.allowedTransitions.putAll(allowedTransitions);
    }
    protected ExpressionParserImpl(TopLevelParseMode topLevel, List<ParseMode> allowedAtTopLevel) {
        this.topLevel = topLevel;
        this.topLevel.allowedTransitions.addAll(allowedAtTopLevel);
    }

    public ExpressionParserImpl includeAllowedSubmodeTransition(ParseMode parent, ParseMode allowedSubmode, ParseMode ...allowedOtherSubmodes) {
        allowedTransitions.put(parent, allowedSubmode);
        for (ParseMode m: allowedOtherSubmodes) allowedTransitions.put(parent, m);
        return this;
    }
    public ExpressionParserImpl includeGroupingBracketsAtUsualPlaces(ParseMode ...optionalExplicitBrackets) {
        List<ParseMode> brackets = optionalExplicitBrackets==null ? MutableList.of() :
                Arrays.asList(optionalExplicitBrackets).stream().filter(b -> b!=null).collect(Collectors.toList());
        if (brackets.isEmpty()) brackets.addAll(COMMON_BRACKETS);

        brackets.forEach(b -> includeAllowedTopLevelTransition(b));
        allowedTransitions.putAll(INTERPOLATED, brackets);
        for (ParseMode b : brackets) {
            allowedTransitions.putAll(b, brackets);
            includeAllowedSubmodeTransition(b, INTERPOLATED);
            includeAllowedSubmodeTransition(b, SINGLE_QUOTE);
            includeAllowedSubmodeTransition(b, DOUBLE_QUOTE);
            brackets.forEach(bb -> includeAllowedSubmodeTransition(b, bb));
        }
        return this;
    }
    public ExpressionParserImpl includeAllowedTopLevelTransition(ParseMode allowedAtTopLevel) {
        topLevel.allowedTransitions.add(allowedAtTopLevel);
        return this;
    }
    public ExpressionParserImpl removeAllowedSubmodeTransition(ParseMode parent, ParseMode disallowedSubmode) {
        allowedTransitions.remove(parent, disallowedSubmode);
        return this;
    }
    public ExpressionParserImpl removeAllowedTopLevelTransition(ParseMode disallowedSubmode) {
        topLevel.allowedTransitions.remove(disallowedSubmode);
        return this;
    }

    public Maybe<ParseNode> parse(String input) {
        return topLevel.parse(input, 0, allowedTransitions, topLevel.allowedTransitions).resultOrError;
    }
    public Maybe<List<ParseNodeOrValue>> parseEverything(String input) {
        return parse(input).mapMaybe(pr -> {
            if (!Objects.equals(pr.source, input)) return Maybe.absent("Could not parse everything");
            return Maybe.of(pr.contents);
        });
    }

    public ExpressionParserImpl stoppingAt(String id, Predicate<String> earlyTermination, boolean requireTopLevelToEndWithThisOrAnotherRequiredMode) {
        if (requireTopLevelToEndWithThisOrAnotherRequiredMode) {
            if (topLevel.mustEndWithOneOfTheseModes == null) topLevel.mustEndWithOneOfTheseModes = MutableList.of();
            topLevel.mustEndWithOneOfTheseModes.add(id);
        }
        return includeAllowedTopLevelTransition(new ParseMode(id) {
            @Override
            public InternalParseResult parse(String input, int offset, Multimap<ParseMode, ParseMode> allowedTransitions, @Nullable Collection<ParseMode> transitionsAllowedHereOverride) {
                if (earlyTermination.test(input.substring(offset))) {
                    InternalParseResult ipr = InternalParseResult.ofValue(this, "", "");
                    ipr.earlyTerminationRequested = true;
                    return ipr;
                }
                return InternalParseResult.SKIP;
            }
        });
    }

}
