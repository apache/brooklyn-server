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
package org.apache.brooklyn.camp.brooklyn.spi.dsl.parse;

import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.fasterxml.jackson.databind.annotation.JsonAppend.Prop;
import org.apache.brooklyn.util.collections.MutableList;

public class DslParser {
    private final String expression;
    int index = -1;

    public DslParser(String expression) {
        this.expression = expression;
    }

    public synchronized Object parse() {
        if (index>=0)
            throw new IllegalStateException("Parser can only be used once");

        index++;
        Object result = next();

        if (index < expression.length())
            throw new IllegalStateException("Unexpected character at position "+index+" in "+expression);

        return result;
    }

    @SuppressWarnings("unchecked")
    public Object next() {
        int start = index;

        boolean isAlreadyInBracket = (index > 0 && (expression.charAt(index -1) == '[' )) || // for [x] syntax - indexes in lists
            (index > 1 && (expression.charAt(index -1) == '"' && expression.charAt(index -2) == '[')) ;  // for ["x"] syntax - string properties and map keys
        if (!isAlreadyInBracket) {
            // feel like this block shouldn't be used; not sure it is
            skipWhitespace();
            if (index >= expression.length())
                throw new IllegalStateException("Unexpected end of expression to parse, looking for content since position " + start);

            if (expression.charAt(index) == '"') {
                // assume a string, that is why for property syntax using [x] or ["x"], we skip this part
                return nextAsQuotedString();
            }
        }
        // not a string, must be a function (or chain thereof)
        List<Object> result = new MutableList<Object>();

        skipWhitespace();
        int fnStart = index;
        boolean isBracketed = expression.charAt(fnStart) == '[';
        if (isBracketed) {
            if (isAlreadyInBracket)
                throw new IllegalStateException("Nested brackets not permitted, at position "+start);
            // proceed to below

        } else {
            // non-bracketed property access, skip separators
            do {
                if (index >= expression.length())
                    break;
                char c = expression.charAt(index);
                if (Character.isJavaIdentifierPart(c)) ;
                    // these chars also permitted
                else if (c==':') ;
                else if (c=='.' && expression.substring(0, index).endsWith("function")) ;  // function.xxx used for some static DslAccessible functions

                    // other things e.g. whitespace, parentheses, etc, beak on
                else break;
                index++;
            } while (true);
        }

        String fn = isBracketed ? "" : expression.substring(fnStart, index);
        if (fn.length()==0 && !isBracketed)
            throw new IllegalStateException("Expected a function name or double-quoted string at position "+start);

        if (index < expression.length() && ( isBracketed || expression.charAt(index)=='(')) {
            // collect arguments
            int parenStart = index;
            List<Object> args = new MutableList<>();
            if (expression.charAt(index)=='[' && !isBracketed) {
                if (!fn.isEmpty()) {
                    return new PropertyAccess(fn);
                }
                // not sure comes here
                if (isBracketed)
                    throw new IllegalStateException("Nested brackets not permitted, at position "+start);
                isBracketed = true;
            }
            index++;
            boolean justAdded = false;
            do {
                skipWhitespace();
                if (index >= expression.length())
                    throw new IllegalStateException("Unexpected end of arguments to function '"+fn+"', no close parenthesis matching character at position "+parenStart);
                char c = expression.charAt(index);

                if (c=='"') {
                    if (justAdded)
                        throw new IllegalStateException("Expected , before quoted string at position "+index);

                    QuotedString next = nextAsQuotedString();
                    if (isBracketed) args.add(next.unwrapped());
                    else args.add(next);
                    justAdded = true;
                    continue;
                }

                if (c == ']') {
                    if (!isBracketed)
                        throw new IllegalStateException("Mismatched close bracket at position "+index);
                    justAdded = false;
                    break;
                }
                if (c==')') {
                    justAdded = false;
                    break;
                }
                if (c==',') {
                    if (!justAdded)
                        throw new IllegalStateException("Invalid character at position "+index);
                    justAdded = false;
                    index++;
                    continue;
                }

                if (justAdded)  // did have this but don't think need it?: && !isProperty)
                    throw new IllegalStateException("Expected , before position "+index);

                args.add(next()); // call with first letter of the property
                justAdded = true;
            } while (true);

            if (justAdded) {
                if (isBracketed) {
                    throw new IllegalStateException("Expected ] at position " + index);
                }
            }

            if (fn.isEmpty()) {
                Object arg = args.get(0);
                if (arg instanceof PropertyAccess) {
                    result.add(arg);
                } else if (arg instanceof FunctionWithArgs) {
                    FunctionWithArgs holder = (FunctionWithArgs) arg;
                    if(holder.getArgs() == null || holder.getArgs().isEmpty()) {
                        result.add(new PropertyAccess(holder.getFunction()));
                    } else {
                        throw new IllegalStateException("Expected empty FunctionWithArgs - arguments present!");
                    }
                } else {
                    result.add(new PropertyAccess(arg));
                }
            } else {
                result.add(new FunctionWithArgs(fn, args));
            }

            index++;
            do {
                skipWhitespace();
                if (index >= expression.length())
                    return result;
                char c = expression.charAt(index);
                if (c == '.') {
                    // chained expression
                    int chainStart = index;
                    index++;
                    Object next = next();
                    if (next instanceof List) {
                        result.addAll((Collection<?>) next);
                    } else if (next instanceof PropertyAccess) {
                        result.add(next);
                    } else {
                        throw new IllegalStateException("Expected functions following position " + chainStart);
                    }
                } else if (c == '[') {
                    int selectorsStart = index;
                    Object next = next();
                    if (next instanceof List) {
                        result.addAll((Collection<? extends PropertyAccess>) next);
                        skipWhitespace();
                        if (index >= expression.length())
                            return result;
                    } else {
                        throw new IllegalStateException("Expected property selectors following position " + selectorsStart);
                    }
                } else {
                    // following word not something handled at this level; assume parent will handle (or throw) - e.g. a , or extra )
                    return result;
                }
            } while (true);
        } else {
            // previously we returned a null-arg function; now we treat as explicit property access,
            // and places that need it as a function with arguments from a map key convert it on to new FunctionWithArgs(selector, null)

            // ideally we'd have richer types here; sometimes it is property access, but sometimes just a wrapped non-quoted constant
            return new PropertyAccess(fn);
        }
    }

    private QuotedString nextAsQuotedString() {
        int stringStart = index;
        index++;
        do {
            if (index >= expression.length())
                throw new IllegalStateException("Unexpected end of expression to parse, looking for close quote since position " + stringStart);
            char c = expression.charAt(index);
            if (c == '"') break;
            if (c == '\\') index++;
            index++;
        } while (true);
        index++;
        return new QuotedString(expression.substring(stringStart, index));
    }

    private void skipWhitespace() {
        while (index<expression.length() && Character.isWhitespace(expression.charAt(index)))
            index++;
    }

}